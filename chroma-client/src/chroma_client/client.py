import requests
import json
from typing import Union

class Chroma:

    _api_url = "http://localhost:8000/api/v1"
    _base_metadata = {}

    def __init__(self, url=None, app=None, model_version=None, layer=None):
        """Initialize Chroma client"""

        if isinstance(url, str) and url.startswith("http"):
            self._api_url = url

        if isinstance(app, str) and app != "":
            self._base_metadata["app"] = app

        if isinstance(model_version, str) and model_version != "":
            self._base_metadata["model_version"] = model_version

        if isinstance(layer, str) and layer != "":
            self._base_metadata["layer"] = layer

        self.url = url

    def count(self):
        '''
        Returns the number of embeddings in the database
        '''
        x = requests.get(self._api_url + "/count")
        return x.json()

    def fetch(self, metadata={}, sort=None, limit=None):
        '''
        Fetches embeddings from the database
        '''
        x = requests.get(self._api_url + "/fetch", data=json.dumps({
            "metadata":json.dumps(metadata), 
            "sort":sort, 
            "limit":limit
        }))
        return x.json()

    def process(self, metadata={}):
        '''
        Processes embeddings in the database
        - currently this only runs hnswlib, doesnt return anything
        '''
        requests.get(self._api_url + "/process", data=json.dumps({
            "metadata":json.dumps(metadata), 
        }))
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
        metadata: dict, 
        input_uri: str, 
        inference_data: dict, 
        app: str, 
        model_version: str, 
        layer: str, 
        dataset: str = None,
        category_name: str = None):
        '''
        Logs a single embedding to the database
        '''

        x = requests.post(self._api_url + "/add", data = json.dumps({
            "embedding_data": embedding_data,
            "metadata": metadata, 
            "input_uri": input_uri, 
            "inference_data": inference_data, 
            "app": app, 
            "model_version": model_version, 
            "layer": layer, 
            "dataset": dataset, 
            "category_name": category_name 
        })  )

        if x.status_code == 201:
            return True
        else:
            return False
    
    def log_training(self, embedding_data: list, input_uri: str, inference_data: dict):
        '''
        Small wrapper around log() to log a single training embedding
        - sets dataset to "training"
        '''
        return self.log(
            embedding_data=embedding_data, 
            metadata=self._base_metadata, 
            input_uri=input_uri, 
            inference_data=inference_data, 
            app=self._base_metadata["app"], 
            model_version=self._base_metadata["model_version"], 
            layer=self._base_metadata["layer"],
            dataset="training"
        )

    def log_production(self, embedding_data: list, input_uri: str, inference_data: dict):
        '''
        Small wrapper around log() to log a single production embedding
        - sets dataset to "production"
        '''
        return self.log(
            embedding_data=embedding_data, 
            metadata=self._base_metadata, 
            input_uri=input_uri, 
            inference_data=inference_data, 
            app=self._base_metadata["app"], 
            model_version=self._base_metadata["model_version"], 
            layer=self._base_metadata["layer"],
            dataset="production"
        )
    
    def log_triage(self, embedding_data: list, input_uri: str, inference_data: dict):
        '''
        Small wrapper around log() to log a single triage embedding
        - sets dataset to "triage"
        '''
        return self.log(
            embedding_data=embedding_data, 
            metadata=self._base_metadata, 
            input_uri=input_uri, 
            inference_data=inference_data, 
            app=self._base_metadata["app"], 
            model_version=self._base_metadata["model_version"], 
            layer=self._base_metadata["layer"],
            dataset="triage"
        )

    def log_batch(self, 
        embedding_data: list, 
        metadata: list, 
        input_uri: list, 
        inference_data: list, 
        app: Union[list, str], 
        model_version:  Union[list, str], 
        layer:  Union[list, str], 
        dataset: list = None,
        category_name: list = None):
        '''
        Logs a batch of embeddings to the database
        - pass in column oriented data lists
        '''

        if isinstance(app, str):
            app = [app] * len(embedding_data)
        
        if isinstance(model_version, str):
            model_version = [model_version] * len(embedding_data)

        if isinstance(layer, str):
            layer = [layer] * len(embedding_data)

        x = requests.post(self._api_url + "/add", data = json.dumps({
            "embedding_data": embedding_data, 
            "metadata": metadata,
            "input_uri": input_uri, 
            "inference_data": inference_data,
            "app": app, 
            "model_version": model_version, 
            "layer": layer, 
            "dataset": dataset, 
            "category_name": category_name 
        }) )

        if x.status_code == 201:
            return True
        else:
            return False
    
    def log_training_batch(self, embedding_data: list, input_uri: list, inference_data: list):
        '''
        Small wrapper around log_batch() to log a batch of training embedding
        - sets dataset to "training"
        '''
        return self.log(
            embedding_data=embedding_data, 
            metadata=self._base_metadata, 
            input_uri=input_uri, 
            inference_data=inference_data, 
            app=self._base_metadata["app"], 
            model_version=self._base_metadata["model_version"], 
            layer=self._base_metadata["layer"],
            dataset="training"
        )
        
    def log_production_batch(self, embedding_data: list, input_uri: list, inference_data: list):
        '''
        Small wrapper around log_batch() to log a batch of production embedding
        - sets dataset to "production"
        '''
        return self.log(
            embedding_data=embedding_data, 
            metadata=self._base_metadata, 
            input_uri=input_uri, 
            inference_data=inference_data, 
            app=self._base_metadata["app"], 
            model_version=self._base_metadata["model_version"], 
            layer=self._base_metadata["layer"],
            dataset="production"
        )
        
    def log_triage_batch(self, embedding_data: list, input_uri: list, inference_data: list):
        '''
        Small wrapper around log_batch() to log a batch of triage embedding
        - sets dataset to "triage"
        '''
        return self.log(
            embedding_data=embedding_data, 
            metadata=self._base_metadata, 
            input_uri=input_uri, 
            inference_data=inference_data, 
            app=self._base_metadata["app"], 
            model_version=self._base_metadata["model_version"], 
            layer=self._base_metadata["layer"],
            dataset="triage"
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