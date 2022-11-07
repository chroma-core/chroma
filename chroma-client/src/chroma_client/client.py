import requests
import json
from typing import Union

class Chroma:

    _api_url = "http://localhost:8000/api/v1"
    _space_key = None

    def __init__(self, url=None, app=None, model_version=None, layer=None):
        """Initialize Chroma client"""

        if isinstance(url, str) and url.startswith("http"):
            self._api_url = url

        if app and model_version and layer:
            self._space_key = app + "_" + model_version + "_" + layer

    def set_context(self, app, model_version, layer):
        '''
        Sets the context of the client
        '''
        self._space_key = app + "_" + model_version + "_" + layer

    def set_space_key(self, space_key):
        '''
        Sets the space key for the client, enables overriding the string concat
        '''
        self._space_key = space_key

    def get_context(self):
        '''
        Returns the space key
        '''
        return self._space_key

    def count(self, space_key=None):
        '''
        Returns the number of embeddings in the database
        '''
        payload = json.dumps({"space_key": space_key or self._space_key})
        x = requests.get(self._api_url + "/count", data=payload)
        return x.json()

    def fetch(self, where_filter={}, sort=None, limit=None):
        '''
        Fetches embeddings from the database
        '''
        where_filter["space_key"] = self._space_key

        x = requests.get(self._api_url + "/fetch", data=json.dumps({
            "where_filter":where_filter, 
            "sort":sort, 
            "limit":limit
        }))
        return x.json()

    def process(self, space_key=None):
        '''
        Processes embeddings in the database
        - currently this only runs hnswlib, doesnt return anything
        '''
        requests.get(self._api_url + "/process", data = json.dumps({"space_key": space_key or self._space_key}))
        return True

    def reset(self):
        '''
        Resets the database
        '''
        return requests.get(self._api_url + "/reset")

    def heartbeat(self):
        '''
        Returns the current server time in milliseconds to check if the server is alive
        '''
        x = requests.get(self._api_url)
        return x.json()

    def log(self, 
        embedding_data: list, 
        input_uri: list, 
        dataset: list = None,
        category_name: list = None,
        space_keys: list = None):
        '''
        Logs a batch of embeddings to the database
        - pass in column oriented data lists
        '''

        if not space_keys:
            if isinstance(dataset, list):
                space_keys = [self._space_key] * len(dataset)
            else:
                space_keys = self._space_key

        x = requests.post(self._api_url + "/add", data = json.dumps({
            "space_key": space_keys,
            "embedding_data": embedding_data, 
            "input_uri": input_uri, 
            "dataset": dataset, 
            "category_name": category_name 
        }) )

        if x.status_code == 201:
            return True
        else:
            return False
    
    def log_training(self, embedding_data: list, input_uri: list, category_name: list, space_keys: list = None):
        '''
        Small wrapper around log() to log a batch of training embedding
        - sets dataset to "training"
        '''
        return self.log(
            embedding_data=embedding_data, 
            input_uri=input_uri, 
            dataset="training",
            category_name=category_name,
            space_keys=space_keys
        )
        
    def log_production(self, embedding_data: list, input_uri: list, category_name: list, space_keys: list = None):
        '''
        Small wrapper around log() to log a batch of production embedding
        - sets dataset to "production"
        '''
        return self.log(
            embedding_data=embedding_data, 
            input_uri=input_uri, 
            dataset="production",
            category_name=category_name,
            space_keys=space_keys
        )
        
    def log_triage(self, embedding_data: list, input_uri: list, category_name: list, space_keys: list = None):
        '''
        Small wrapper around log() to log a batch of triage embedding
        - sets dataset to "triage"
        '''
        return self.log(
            embedding_data=embedding_data, 
            input_uri=input_uri, 
            dataset="triage",
            category_name=category_name,
            space_keys=space_keys
        )
        
    def get_nearest_neighbors(self, embedding, n_results=10, category_name=None, dataset="training", space_key = None):
        '''
        Gets the nearest neighbors of a single embedding
        '''
        if not space_key:
            space_key = self._space_key

        x = requests.post(self._api_url + "/get_nearest_neighbors", data = json.dumps({
            "space_key": space_key,
            "embedding": embedding, 
            "n_results": n_results,
            "category_name": category_name,
            "dataset": dataset
        }) )

        if x.status_code == 200:
            return x.json()
        else:
            return False

    def raw_sql(self, sql):
        '''
        Runs a raw SQL query against the database
        '''
        x = requests.get(self._api_url + "/raw_sql", data = json.dumps({"raw_sql": sql}))
        return x.json()