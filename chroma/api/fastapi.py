from chroma.api import API
from chroma.errors import NoDatapointsException
import pandas as pd
import requests
import json

class FastAPI(API):

    def __init__(self, settings):
        self._api_url = f'http://{settings.chroma_server_host}:{settings.chroma_server_http_port}/api/v1'

    def heartbeat(self):
        '''Returns the current server time in nanoseconds to check if the server is alive'''
        resp = requests.get(self._api_url)
        resp.raise_for_status()
        return int(resp.json()['nanosecond heartbeat'])

    def count(self, model_space=None):
        '''Returns the number of embeddings in the database'''
        params = {"model_space": model_space or self._model_space}
        resp = requests.get(self._api_url + "/count", params=params)
        resp.raise_for_status()
        return resp.json()

    def fetch(self, where={}, sort=None, limit=None, offset=None, page=None, page_size=None):
        '''Fetches embeddings from the database'''

        where = self.where_with_model_space(where)

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        resp = requests.post(self._api_url + "/fetch", data=json.dumps({
            "where":where,
            "sort":sort,
            "limit":limit,
            "offset":offset
        }))

        resp.raise_for_status()
        return pd.DataFrame.from_dict(resp.json())

    def delete(self, where={}):
        '''Deletes embeddings from the database'''

        where = self.where_with_model_space(where)

        resp = requests.post(self._api_url + "/delete", data=json.dumps({"where":where}))

        resp.raise_for_status()
        return resp.json()

    def add(self,
            model_space,
            embedding,
            input_uri=None,
            dataset=None,
            metadata=None,
            ):
        '''
        Addss a batch of embeddings to the database
        - pass in column oriented data lists
        '''

        if not model_space:
            model_space = self._model_space

        resp = requests.post(self._api_url + "/add", data = json.dumps({
            "model_space": model_space,
            "embedding": embedding,
            "input_uri": input_uri,
            "dataset": dataset,
            "metadata": metadata,
        }) )

        resp.raise_for_status
        return True


    def get_nearest_neighbors(self, embedding, n_results=10, where={}):
        '''Gets the nearest neighbors of a single embedding'''

        where = self.where_with_model_space(where)

        resp = requests.post(self._api_url + "/get_nearest_neighbors", data = json.dumps({
            "embedding": embedding,
            "n_results": n_results,
            "where": where
        }) )

        resp.raise_for_status()

        val = resp.json()
        if 'error' in val:
            if val['error'] == "no data points":
                raise NoDatapointsException("No datapoints found for the supplied filter")
            else:
                raise Exception(val["error"])

        val['embeddings'] = pd.DataFrame.from_dict(val['embeddings'])

        return val

    # def process(self, model_space=None, training_dataset_name="training", unlabeled_dataset_name="unlabeled"):
    #     '''
    #     Processes embeddings in the database
    #     '''
    #     payload = {"model_space": model_space or self._model_space,
    #                "training_dataset_name": training_dataset_name,
    #                "unlabeled_dataset_name": unlabeled_dataset_name}
    #     resp = requests.post(self._api_url + "/process", data = json.dumps(payload))
    #     resp.raise_for_status()
    #     return resp.json()

    def reset(self):
        '''Resets the database'''
        resp = requests.post(self._api_url + "/reset")
        resp.raise_for_status()
        return resp.json

    def raw_sql(self, sql):
        '''Runs a raw SQL query against the database'''
        resp = requests.post(self._api_url + "/raw_sql", data = json.dumps({"raw_sql": sql}))
        resp.raise_for_status()
        return pd.DataFrame.from_dict(resp.json())

    # def get_results(self, model_space=None, n_results = 100, dataset_name="unlabeled"):
    #     '''Gets the results for the given space key'''
    #     resp = requests.post(self._api_url + "/get_results",
    #                          data = json.dumps({"model_space": model_space or self._model_space, "n_results": n_results, "dataset_name": dataset_name}))
    #     resp.raise_for_status()
    #     return pd.DataFrame.from_dict(resp.json())

    # def get_task_status(self, task_id):
    #     '''Gets the status of a task'''
    #     resp = requests.post(self._api_url + f"/tasks/{task_id}")
    #     resp.raise_for_status()
    #     return resp.json

    def create_index(self, model_space=None):
        '''Creates an index for the given space key'''
        resp = requests.post(self._api_url + "/create_index",
                             data = json.dumps({"model_space": model_space or self._model_space}))
        resp.raise_for_status()
        return resp.json()
