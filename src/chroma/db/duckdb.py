from dis import dis
from xml.etree import ElementPath
from db.abstract import Database
import duckdb
import json

class DuckDB(Database):
    _conn = None

    def __init__(self):
        # TODO: the schema should live outside this class and be passed to this class. 
        # the schema may end up even being dynamic per partition (though we would to think through that cost of complexity)
        self._conn = duckdb.connect()
        self._conn.execute('''
            CREATE TABLE embeddings (
                id integer PRIMARY KEY,
                embedding_data REAL[], 
                metadata JSON, 
                input_uri STRING, 
                infer JSON,
                app STRING,
                model_version STRING,
                layer STRING,
                distance REAL,
                category_name STRING
            )
        ''')
        # we create ids to manage internal bookkeeping and *nothing else*, users should not have to care about these ids
        self._conn.execute('''
            CREATE SEQUENCE seq_id START 1;
        ''')

        self._conn.execute('''
        -- change the default null sorting order to either NULLS FIRST and NULLS LAST
        PRAGMA default_null_order='NULLS LAST';
        -- change the default sorting order to either DESC or ASC
        PRAGMA default_order='DESC';
        ''')
        return

    def add_batch(self, embedding_data, metadata, input_uri, inference_data, app, model_version, layer, distance=None, category_name=None):
        '''
        Add embeddings to the database
        This accepts both a single input and a list of inputs
        '''

        # create list of the types of all inputs
        types = [type(x).__name__ for x in [embedding_data, input_uri, inference_data, app, model_version, layer]]

        # if all of the types are 'list' - do batch mode
        if all(x == 'list' for x in types):
            lengths = [len(x) for x in [embedding_data, input_uri, inference_data, app, model_version, layer]]

            if distance is None:
                distance = [None] * lengths[0]
            if category_name is None:
                category_name = [None] * lengths[0]
            # if all of the lengths are the same, then we can do batch mode
            # data_to_insert = [embedding_data, metadata, input_uri, inference_data,  app, model_version, layer, distance, category_name] for [embedding_data, metadata, input_uri, inference_data,  app, model_version, layer, distance, category_name] in zip(*data_to_insert)

            # an array that is the first element in embedding_data and the first element in inpt_uri and so on
            data_to_insert = []
            for i in range(lengths[0]):
                data_to_insert.append([embedding_data[i], metadata[i], input_uri[i], inference_data[i],  app[i], model_version[i], layer[i], distance[i], category_name[i]])

            if all(x == lengths[0] for x in lengths):
                self._conn.executemany('''
                    INSERT INTO embeddings VALUES (nextval('seq_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                    data_to_insert
                )
                return
        
        # if any of the types are 'list' - throw an error
        # remove the first type from types because embedding_data is always a list
        if any(x == list for x in types.pop(0)):
            raise Exception("Invalid input types. One input is a list where others are not: " + str(types))

        # if we get here, then we are doing single insert mode
        self._conn.execute('''
            INSERT INTO embeddings VALUES (nextval('seq_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
            [embedding_data, metadata, input_uri, inference_data,  app, model_version, layer, distance, category_name]
        )

        # self._conn.execute(f'''
        #     UPDATE embeddings SET distance=5 WHERE id=1'''
        # )

    def update(self, data): # call this update_distance! that is all it does
        '''
        I was not able to figure out how to do a bulk update in duckdb
        This is going to be slow and bad
        If this is a limitation of duckdb, we should consider switching to something transactional / OLTP
        '''
        print("starting update")
        for element in data:    
            if element['distance'] is None:
                continue
            self._conn.execute(f'''
                UPDATE embeddings SET distance={element['distance']} WHERE id={element['id']}'''
            )

        print("completed update")

    def fetch(self, where_filter={}, sort=None, limit=None):
        # check to see if query is a dict and if it is a flat list of key value pairs
        if where_filter is not None:
            if not isinstance(where_filter, dict):
                raise Exception("Invalid where_filter: " + str(where_filter))
            # ensure where_filter is a flat dict
            for key in where_filter:
                if isinstance(where_filter[key], dict):
                    raise Exception("Invalid where_filter: " + str(where_filter))
        
        # dict to string, with = between keys and values, and & between key-value pairs
        # TODO: i am wrapping value in single quotes, which is correct for strings.. but it would be better to have a check for type
        where_filter = " AND ".join([f"{key} = '{value}'" for key, value in where_filter.items()])

        # if where_filter is empty, then we don't want to add the WHERE clause
        if where_filter:
            where_filter = f"WHERE {where_filter}"

        # if sort is not none add to the end of where_filter, force to be string?
        if sort is not None:
            where_filter += f" ORDER BY {sort}"

        if limit is not None and isinstance(limit, int):
            where_filter += f" LIMIT {limit}"

        return self._conn.execute(f'''
            SELECT 
                id,
                embedding_data, 
                infer, 
                metadata, 
                input_uri,
                app,
                model_version,
                layer,
                distance,
                category_name
            FROM 
                embeddings
        {where_filter}
        ''').fetchdf()

    def get_all_embeddings(self):
        return self._conn.execute('''
                SELECT 
                    embedding_data 
                FROM 
                    embeddings;
            ''').fetchdf()

    def delete_batch(self, batch):
        # DELETE FROM tbl WHERE i=2;
        # TODO: implement
        # the use case here is you accidentally loaded in some data
        # but how to identify that data? because we allow dupes on all fields (a row can share all fields with another row)
        return

    def persist(self):
        '''
        Persist the database to disk
        TODO: we can think about partitioning here... we could partition out by app/model_version/layer for example
        '''
        if self._conn is None:
            return

        self._conn.execute('''
            COPY 
                (SELECT * FROM embeddings) 
            TO '.chroma/chroma.parquet' 
                (FORMAT PARQUET);
        ''')

    def load(self, path=".chroma/chroma.parquet"):
        '''
        Load the database from disk
        TODO: we could load only the partitions we need here, once we persist via partitioning
        '''
        self._conn.execute(f"INSERT INTO embeddings SELECT * FROM read_parquet('{path}');")