import requests
import json
from typing import Union

class Chroma:

    _api_url = "http://localhost:8000/api/v1"

    def __init__(self, url=None):
        """Initialize Chroma client"""

        if isinstance(url, str) and url.startswith("http"):
            self._api_url = url

        self.url = url

    def count(self):
        '''
        Returns the number of embeddings in the database
        '''
        x = requests.get(self._api_url + "/count")
        return x.json()

    def fetch(self, where_filter={}, sort=None, limit=None):
        '''
        Fetches embeddings from the database
        '''
        x = requests.get(self._api_url + "/fetch", data=json.dumps({
            "where_filter":json.dumps(where_filter), 
            "sort":sort, 
            "limit":limit
        }))
        return x.json()

    def process(self):
        '''
        Processes embeddings in the database
        - currently this only runs hnswlib, doesnt return anything
        '''
        requests.get(self._api_url + "/process")
        return True

    def reset(self):
        '''
        Resets the database
        '''
        return requests.get(self._api_url + "/reset")

    def persist(self):
        '''
        Persists the database to disk in the .chroma folder inside chroma-server
        '''
        return requests.get(self._api_url + "/persist")

    def rand(self):
        '''
        Stubbed out sampling endpoint, returns a random bisection of the database
        '''
        x = requests.get(self._api_url + "/rand")
        return x.json()

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
        category_name: list = None):
        '''
        Logs a batch of embeddings to the database
        - pass in column oriented data lists
        '''

        x = requests.post(self._api_url + "/add", data = json.dumps({
            "embedding_data": embedding_data, 
            "input_uri": input_uri, 
            "dataset": dataset, 
            "category_name": category_name 
        }) )

        if x.status_code == 201:
            return True
        else:
            return False
    
    def log_training(self, embedding_data: list, input_uri: list, category_name: list):
        '''
        Small wrapper around log() to log a batch of training embedding
        - sets dataset to "training"
        '''
        return self.log(
            embedding_data=embedding_data, 
            input_uri=input_uri, 
            dataset="training",
            category_name=category_name
        )
        
    def log_production(self, embedding_data: list, input_uri: list, category_name: list):
        '''
        Small wrapper around log() to log a batch of production embedding
        - sets dataset to "production"
        '''
        return self.log(
            embedding_data=embedding_data, 
            input_uri=input_uri, 
            dataset="production",
            category_name=category_name
        )
        
    def log_triage(self, embedding_data: list, input_uri: list, category_name: list):
        '''
        Small wrapper around log() to log a batch of triage embedding
        - sets dataset to "triage"
        '''
        return self.log(
            embedding_data=embedding_data, 
            input_uri=input_uri, 
            dataset="triage",
            category_name=category_name
        )
        
    def get_nearest_neighbors(self, embedding, n_results=10, category_name=None, dataset="training"):
        '''
        Gets the nearest neighbors of a single embedding
        '''
        x = requests.post(self._api_url + "/get_nearest_neighbors", data = json.dumps({
            "embedding": embedding, 
            "n_results": n_results,
            "category_name": category_name,
            "dataset": dataset
        }) )

        if x.status_code == 200:
            return x.json()
        else:
            return False