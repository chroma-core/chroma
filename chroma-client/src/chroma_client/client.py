import requests
import json
from typing import Union

class Chroma:

    _api_url = "http://localhost:8000/api/v1"
    _model_space = "default_scope"

    def __init__(self, url=None, app=None, model_version=None, layer=None):
        """Initialize Chroma client"""

        if isinstance(url, str) and url.startswith("http"):
            self._api_url = url

        if app and model_version and layer:
            self._model_space = app + "_" + model_version + "_" + layer

    def set_model_space(self, app, model_version, layer):
        '''Sets the model_space of the client'''
        self._model_space = app + "_" + model_version + "_" + layer

    def set_model_space(self, model_space):
        '''Sets the space key for the client, enables overriding the string concat'''
        self._model_space = model_space

    def get_model_space(self):
        '''Returns the model_space key'''
        return self._model_space

    def heartbeat(self):
        '''Returns the current server time in nanoseconds to check if the server is alive'''
        return requests.get(self._api_url).json()

    def count(self, model_space=None):
        '''Returns the number of embeddings in the database'''
        params = {"model_space": model_space or self._model_space}
        x = requests.get(self._api_url + "/count", params=params)
        return x.json()

    def fetch(self, where_filter={}, sort=None, limit=None, offset=None, page=None, page_size=None):
        '''Fetches embeddings from the database'''
        if self._model_space:
            where_filter["model_space"] = self._model_space

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        return requests.post(self._api_url + "/fetch", data=json.dumps({
            "where_filter":where_filter, 
            "sort":sort, 
            "limit":limit,
            "offset":offset
        })).json()

    def delete(self, where_filter={}):
        '''Deletes embeddings from the database'''
        if self._model_space:
            where_filter["model_space"] = self._model_space

        return requests.post(self._api_url + "/delete", data=json.dumps({
            "where_filter":where_filter, 
        })).json()
   
    def add(self, 
        embedding: list, 
        input_uri: list, 
        dataset: list = None,
        category_name: list = None,
        model_spaces: list = None):
        '''
        Addss a batch of embeddings to the database
        - pass in column oriented data lists
        '''

        if not model_spaces:
            model_spaces = self._model_space

        x = requests.post(self._api_url + "/add", data = json.dumps({
            "model_space": model_spaces,
            "embedding": embedding, 
            "input_uri": input_uri, 
            "dataset": dataset, 
            "category_name": category_name 
        }) )

        if x.status_code == 201:
            return True
        else:
            return False
    
    def add_training(self, embedding: list, input_uri: list, category_name: list, model_spaces: list = None):
        '''
        Small wrapper around add() to add a batch of training embedding - sets dataset to "training"
        '''
        datasets = ["training"] * len(input_uri)
        return self.add(
            embedding=embedding, 
            input_uri=input_uri, 
            dataset=datasets,
            category_name=category_name,
            model_spaces=model_spaces
        )
        
    def add_production(self, embedding: list, input_uri: list, category_name: list, model_spaces: list = None):
        '''
        Small wrapper around add() to add a batch of production embedding - sets dataset to "production"
        '''
        datasets = ["production"] * len(input_uri)
        return self.add(
            embedding=embedding, 
            input_uri=input_uri, 
            dataset=datasets,
            category_name=category_name,
            model_spaces=model_spaces
        )
        
    def add_triage(self, embedding: list, input_uri: list, category_name: list, model_spaces: list = None):
        '''
        Small wrapper around add() to add a batch of triage embedding - sets dataset to "triage"
        '''
        datasets = ["triage"] * len(input_uri)
        return self.add(
            embedding=embedding, 
            input_uri=input_uri, 
            dataset=datasets,
            category_name=category_name,
            model_spaces=model_spaces
        )
        
    def get_nearest_neighbors(self, embedding, n_results=10, category_name=None, dataset="training", model_space = None):
        '''Gets the nearest neighbors of a single embedding'''
        if not model_space:
            model_space = self._model_space

        x = requests.post(self._api_url + "/get_nearest_neighbors", data = json.dumps({
            "model_space": model_space,
            "embedding": embedding, 
            "n_results": n_results,
            "category_name": category_name,
            "dataset": dataset
        }) )

        if x.status_code == 200:
            return x.json()
        else:
            return False

    def process(self, model_space=None):
        '''
        Processes embeddings in the database
        - currently this only runs hnswlib, doesnt return anything
        '''
        requests.post(self._api_url + "/process", data = json.dumps({"model_space": model_space or self._model_space}))
        return True

    def reset(self):
        '''Resets the database'''
        return requests.post(self._api_url + "/reset")

    def raw_sql(self, sql):
        '''Runs a raw SQL query against the database'''
        return requests.post(self._api_url + "/raw_sql", data = json.dumps({"raw_sql": sql})).json()

    def calculate_results(self, model_space=None):
        '''Calculates the results for the given space key'''
        return requests.post(self._api_url + "/calculate_results", data = json.dumps({"model_space": model_space or self._model_space})).json()

    def get_results(self, model_space=None, n_results = 100):
        '''Gets the results for the given space key'''
        return requests.post(self._api_url + "/get_results", data = json.dumps({"model_space": model_space or self._model_space, "n_results": n_results})).json()
    
    def get_task_status(self, task_id):
        '''Gets the status of a task'''
        return requests.post(self._api_url + f"/tasks/{task_id}").json()
