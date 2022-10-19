from logger import logger
from db.duckdb import DuckDB
from index.hnswlib import Hnswlib

# This core chroma logic can be used either:
# - as a client only, that ferrires data to a remote server
# - as a server only, that is receiving data, storing, and processing it
# - in-memory, which doing everything locally
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

        logger.info("self._db: " + str(self._db))
        logger.info("self._ann_index: " + str(self._ann_index))
        return

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._db.close() # TODO: not implemented yet

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
        
        self._db.add_batch(embedding_data, metadata, input_uri, inference_data)

        logger.info("Log running")
        print(metadata)
        return

    def log_training(self, input_uri, inference_data, embedding_data):
        metadata = self._base_metadata.copy()
        metadata["dataset"] = "training"
        self.log(input_uri, inference_data, embedding_data, metadata=metadata)
        logger.info("Log training running")
        return
    
    def log_production(self, input_uri, inference_data, embedding_data):
        metadata = self._base_metadata.copy()
        metadata["dataset"] = "production"
        metadata["reference_dataset"] = "training"
        self.log(input_uri, inference_data, embedding_data, metadata=metadata)
        logger.info("Log production running")
        return

    def log_triage(self, input_uri, inference_data, embedding_data):
        metadata = self._base_metadata.copy()
        metadata["dataset"] = "triage"
        self.log(input_uri, inference_data, embedding_data, metadata=metadata)
        logger.info("Log triage running")
        return

    def process(self, metadata=None):
        # if metadata is none, set it to the base metadata, else check its type
        if metadata is None:
            metadata = self._base_metadata.copy()
        else:
            if not isinstance(metadata, dict):
                raise Exception("Invalid metadata: " + str(metadata))

        logger.info("Process running")
        print(metadata)
        return

    def fetch(self, metadata=None, n_results=10):
        # if metadata is none, set it to the base metadata, else check its type
        if metadata is None:
            metadata = self._base_metadata.copy()
            metadata["dataset"] = "production"
        else:
            if not isinstance(metadata, dict):
                raise Exception("Invalid metadata: " + str(metadata))

        # ensure n_results is an int
        if not isinstance(n_results, int):
            raise Exception("Invalid n_results: " + str(n_results))

        logger.info("Fetch running")
        print(self._db.fetch())
        return

    def query_distance(self):
        logger.info("Query distance running")
        return

    def delete(self, metadata=None):
        # if metadata is none, set it to the base metadata, else check its type
        if metadata is None:
            metadata = self._base_metadata.copy()
        else:
            if not isinstance(metadata, dict):
                raise Exception("Invalid metadata: " + str(metadata))

        logger.info("Delete running")
        print(metadata)
        return