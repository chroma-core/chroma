from algorithms.mhb_class_distances import class_distances
from algorithms.umap_and_project import umap_and_project
from logger import logger
from db.duckdb import DuckDB
from index.hnswlib import Hnswlib
import os
import json

# This core chroma logic can be used either:
# - as a client only, that ferrires data to a remote server
# - as a server only, that is receiving data, storing, and processing it
# - in-memory, which doing everything locally <-- implemented here
_modes = ["in-memory"] # "client", "server"
DEFAULT_DB = DuckDB
DEFAULT_INDEXING = Hnswlib
DEFAULT_MODE= "in-memory"
ALLOWED_DICT = {
    'in-memory': {
        "_databaseOptions": [DuckDB],
        "_indexingOptions": [Hnswlib]
    }
}

# TODOS
# TODO: if mode is client, then we need to send data to a server, save a url for this
# TODO: are we storing label data?
# TODO: timestamps for things
# TODO: feels like i could be handling json smarter.. avoiding the json.dumps and json.loads, using the type in duckdb
# TODO: use temp table? https://duckdb.org/docs/sql/statements/create_table
# TODO: enable passing in a database and indexing files that exist in the chroma folder
    # TODO: should we scan that folder on init and see if it exists.... create it... scan contents... get latest ... 

# create a new python class definition
class Chroma:
    _mode = None
    _db = None
    _ann_index = None
    _base_metadata = None

    # define the constructor
    def __init__(
        self, 
        mode=DEFAULT_MODE, 
        db=DEFAULT_DB, 
        indexing=DEFAULT_INDEXING, 
        base_metadata=None,
        ):
        # check if the mode is valid
        if mode not in _modes:
            raise Exception("Invalid mode: " + str(mode))
        self._mode = mode

        # check if the database is valid
        if db not in ALLOWED_DICT[self._mode]["_databaseOptions"]:
            raise Exception("Invalid database: " + str(db))
        db = db

        # check if the indexing is valid
        if indexing not in ALLOWED_DICT[self._mode]["_indexingOptions"]:
            raise Exception("Invalid indexing: " + str(indexing))
        ann_index = indexing

        # check if the base metadata is valid
        if base_metadata is not None:
            if not isinstance(base_metadata, dict):
                raise Exception("Invalid base metadata: " + str(base_metadata))
            self._base_metadata = base_metadata
        else:
            self._base_metadata = {}
        
        self._db = db()
        self._ann_index = ann_index()

        # create the .chroma directory if it doesn't exist
        if not os.path.exists(".chroma"):
            os.mkdir(".chroma")

        # load an existing database if it exists
        if os.path.exists(".chroma/chroma.parquet"):
            logger.info("Loading existing chroma database")
            self._db.load()

        # load an existing index if it exists
        if os.path.exists(".chroma/index.bin"):
            logger.info("Loading existing chroma index")
            self._ann_index.load()

        # logger.info("self._db: " + str(self._db))
        # logger.info("self._ann_index: " + str(self._ann_index))
        return

    def __del__(self):
        self._db.persist() 
        self._ann_index.persist() 
        return

    # todo: should this support both single and batch mode?
    # batch mode could accept lists for all these inputs and do the lining up of them...
    def log(self, input_uri, inference_data, embedding_data, metadata=None):
        # ensure that input uri is a string
        if not isinstance(input_uri, str):
            raise Exception("Invalid input uri: " + str(input_uri))

        # ensure that inference data is an object
        if not isinstance(inference_data, object):
            raise Exception("Invalid inference data: " + str(inference_data))
        
        # ensure that embedding data is a list of numbers
        # TODO: verify that the length matches what is already in the DB?
        if not isinstance(embedding_data, list):
            raise Exception("Invalid embedding data: " + str(embedding_data))

        # ensure metadata is a dict
        if metadata is not None:
            if not isinstance(metadata, dict):
                raise Exception("Invalid metadata: " + str(metadata))
            # ensure metadata is a flat dict
            for key in metadata:
                if isinstance(metadata[key], dict):
                    raise Exception("Invalid metadata: " + str(metadata))

        # pull out app, model_version, and layer from the metadata
        # force coerce to string 
        app = str(metadata["app"])
        model_version = str(metadata["model_version"])
        layer = str(metadata["layer"])
        # make sure app, model_version, and layer are strings
        if not isinstance(app, str):
            raise Exception("Invalid app: " + str(app))
        if not isinstance(model_version, str):
            raise Exception("Invalid model_version: " + str(model_version))
        if not isinstance(layer, str):
            raise Exception("Invalid layer: " + str(layer))

        # get category_name from inference_data
        category_name = json.loads(inference_data)["annotations"][0]["category_name"]
        
        self._db.add_batch(embedding_data, metadata, input_uri, inference_data, app, model_version, layer, None, category_name)

        # logger.info("Log running")
        return

    def log_training(self, input_uri, inference_data, embedding_data):
        metadata = self._base_metadata.copy()
        metadata["dataset"] = "training"
        self.log(input_uri, inference_data, embedding_data, metadata=metadata)
        # logger.info("Log training running")
        return
    
    def log_production(self, input_uri, inference_data, embedding_data):
        metadata = self._base_metadata.copy()
        metadata["dataset"] = "production"
        metadata["reference_dataset"] = "training"
        self.log(input_uri, inference_data, embedding_data, metadata=metadata)
        # logger.info("Log production running")
        return

    def log_triage(self, input_uri, inference_data, embedding_data):
        metadata = self._base_metadata.copy()
        metadata["dataset"] = "triage"
        self.log(input_uri, inference_data, embedding_data, metadata=metadata)
        # logger.info("Log triage running")
        return

    def process(self, metadata=None):
        # if metadata is none, set it to the base metadata, else check its type
        if metadata is None:
            metadata = self._base_metadata.copy()
        else:
            if not isinstance(metadata, dict):
                raise Exception("Invalid metadata: " + str(metadata))

        # get the embdding data from the database
        self._ann_index.run(self._db.get_all_embeddings()) #TODOTODO - change this now

        # print('self._db.update()', self._db.update())
        self._db.update(class_distances(self._db.fetch()))
        data = self._db.fetch()
        # umap_and_project(data["embedding_data"], data['distance'])

        # logger.info("Process running")
        return

    def fetch(self, metadata={}, sort=None, limit=None):
        # if metadata is none, set it to the base metadata, else check its type
        if metadata is None:
            metadata = self._base_metadata.copy()
            metadata["dataset"] = "production"
        else:
            if not isinstance(metadata, dict):
                raise Exception("Invalid metadata: " + str(metadata))

        # ensure n_results is an int
        if limit != None and not isinstance(limit, int):
            raise Exception("Invalid limit: " + str(limit))

        # we will by default filter on the base metadata, unless overridden?
        # still unclear how to filter by stringified json in the db
        # so for right now, this only works for app, model_version, 
        # and layer where are split out, into their own columns
        where_filter = {**self._base_metadata, **metadata}

        # logger.info("Fetch running")
        return self._db.fetch(where_filter, sort, limit)

    def fetch_highest_signal(self, metadata={}, n_results=10):
        # TODO: id like to be able to fetch by category_id
        '''
        Fetches the highest distance items from the database for a given metadata
        What we really want is the distance and input_uri I think.... 
        but right now this will return everything in the row
        TODO: dont do a magic string "distance" here... have a global dict of column names somewhere
        '''
        return self.fetch(metadata=metadata, sort="distance", limit=n_results)
        # do the same thing as def fetch... but additionally we want to SORT by distance, and then LIMIT n_results
        return

    def query_distance(self):
        # logger.info("Query distance running")
        return

    def delete(self, metadata=None):
        # if metadata is none, set it to the base metadata, else check its type
        if metadata is None:
            metadata = self._base_metadata.copy()
        else:
            if not isinstance(metadata, dict):
                raise Exception("Invalid metadata: " + str(metadata))

        # logger.info("Delete running")
        return