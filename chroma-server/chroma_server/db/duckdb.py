from os import EX_CANTCREAT
from chroma_server.db.abstract import Database
import duckdb
import numpy as np
import pandas as pd

class DuckDB(Database):
    _conn = None

    def __init__(self):
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
                dataset STRING,
                distance REAL,
                category_name STRING
            )
        ''')

        # ids to manage internal bookkeeping and *nothing else*, users should not have to care about these ids
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

    def add_batch(self, embedding_data, metadata, input_uri, inference_data, app, model_version, layer, dataset=None, distance=None, category_name=None):
        '''
        Add embeddings to the database
        This accepts both a single input and a list of inputs
        '''

        # create list of the types of all inputs
        types = [type(x).__name__ for x in [embedding_data, input_uri, inference_data, app, model_version, layer]]

        # if all of the types are 'list' - do batch mode
        if all(x == 'list' for x in types):
            lengths = [len(x) for x in [embedding_data, input_uri, inference_data, app, model_version, layer]]

            # accepts some inputs as str or none, and this multiples them out to the correct length
            if distance is None or isinstance(distance, str):
                distance = [distance] * lengths[0]
            if category_name is None or isinstance(category_name, str):
                category_name = [category_name] * lengths[0]
            if dataset is None or isinstance(dataset, str):
                dataset = [dataset] * lengths[0]

            # we have to move from column to row format for duckdb
            data_to_insert = []
            for i in range(lengths[0]):
                data_to_insert.append([embedding_data[i], metadata[i], input_uri[i], inference_data[i],  app[i], model_version[i], layer[i], dataset[i], distance[i], category_name[i]])

            if all(x == lengths[0] for x in lengths):
                self._conn.executemany('''
                    INSERT INTO embeddings VALUES (nextval('seq_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
                    data_to_insert
                )
                return
        
        # if any of the types are 'list' - throw an error
        if any(x == list for x in [input_uri, inference_data, app, model_version, layer, dataset, distance, category_name]):
            raise Exception("Invalid input types. One input is a list where others are not: " + str(types))

        # single insert mode
        self._conn.execute('''
            INSERT INTO embeddings VALUES (nextval('seq_id'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', 
            [embedding_data, metadata, input_uri, inference_data,  app, model_version, layer, dataset, distance, category_name]
        )
        
    def count(self):
        return self._conn.execute('''
            SELECT COUNT(*) FROM embeddings;
        ''').fetchone()[0]

    def update(self, data): # call this update_distance! that is all it does
        '''
        I was not able to figure out (yet) how to do a bulk update in duckdb
        This is going to be fairly slow
        '''
        for element in data:    
            if element['distance'] is None:
                continue
            self._conn.execute(f'''
                UPDATE embeddings SET distance={element['distance']} WHERE id={element['id']}'''
            )

    def fetch(self, where_filter={}, sort=None, limit=None):
        # check to see if query is a dict and if it is a flat list of key value pairs
        if where_filter is not None:
            if not isinstance(where_filter, dict):
                raise Exception("Invalid where_filter: " + str(where_filter))
            
            # ensure where_filter is a flat dict
            for key in where_filter:
                if isinstance(where_filter[key], dict):
                    raise Exception("Invalid where_filter: " + str(where_filter))
        
        where_filter = " AND ".join([f"{key} = '{value}'" for key, value in where_filter.items()])

        if where_filter:
            where_filter = f"WHERE {where_filter}"

        if sort is not None:
            where_filter += f" ORDER BY {sort}"

        if limit is not None or isinstance(limit, int):
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
                dataset,
                layer,
                distance,
                category_name
            FROM 
                embeddings
        {where_filter}
        ''').fetchdf().replace({np.nan: None}) # replace nan with None for json serialization

    def delete_batch(self, batch):
        raise NotImplementedError

    def persist(self):
        '''
        Persist the database to disk
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
        '''
        self._conn.execute(f"INSERT INTO embeddings SELECT * FROM read_parquet('{path}');")

    def get_by_ids(self, ids=list):
        # select from duckdb table where ids are in the list
        if not isinstance(ids, list):
            raise Exception("ids must be a list")
        
        if not ids:
            # create an empty pandas dataframe
            return pd.DataFrame()

        return self._conn.execute(f'''
            SELECT 
                id,
                embedding_data, 
                infer, 
                metadata, 
                input_uri,
                app,
                model_version,
                dataset,
                layer,
                distance,
                category_name
            FROM 
                embeddings
            WHERE
                id IN ({','.join([str(x) for x in ids])})
        ''').fetchdf().replace({np.nan: None}) # replace nan with None for json serialization