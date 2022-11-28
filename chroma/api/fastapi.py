from chroma.api import API
import requests

class FastAPI(API):

    def __init__(self, settings):
        self._api_url = f'http://{settings.chroma_server_host}:{settings.chroma_server_http_port}/api/v1'

    def heartbeat(self):
        '''Returns the current server time in nanoseconds to check if the server is alive'''
        return int(requests.get(self._api_url).json()['nanosecond heartbeat'])

    def count(self, model_space=None):
        '''Returns the number of embeddings in the database'''
        params = {"model_space": model_space or self._model_space}
        x = requests.get(self._api_url + "/count", params=params)
        return x.json()['count']

    def fetch(self, where={}, sort=None, limit=None, offset=None, page=None, page_size=None):
        '''Fetches embeddings from the database'''

        where = self.where_with_model_space(where)

        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        return requests.post(self._api_url + "/fetch", data=json.dumps({
            "where":where,
            "sort":sort,
            "limit":limit,
            "offset":offset
        })).json()

    def delete(self, where={}):
        '''Deletes embeddings from the database'''

        where = self.where_with_model_space(where)

        return requests.post(self._api_url + "/delete", data=json.dumps({
            "where":where,
        })).json()

    def add(self,
            embedding: list,
            input_uri: list,
            dataset: list = None,
            inference_class: list = None,
            label_class: list = None,
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
            "inference_class": inference_class,
            "label_class": label_class
        }) )

        if x.status_code == 201:
            return True
        else:
            return False

    def get_nearest_neighbors(self, embedding, n_results=10, where={}):
        '''Gets the nearest neighbors of a single embedding'''

        where = self.where_with_model_space(where)

        x = requests.post(self._api_url + "/get_nearest_neighbors", data = json.dumps({
            "embedding": embedding,
            "n_results": n_results,
            "where": where
        }) )

        if x.status_code == 200:
            return x.json()
        else:
            return False

    def process(self, model_space=None, training_dataset_name="training", inference_dataset_name="inference"):
        '''
        Processes embeddings in the database
        - currently this only runs hnswlib, doesnt return anything
        '''
        payload = {"model_space": model_space or self._model_space,
                   "training_dataset_name": training_dataset_name,
                   "inerence_dataset_name": inference_dataset_name}
        x = requests.post(self._api_url + "/process", data = json.dumps(payload))
        return x.json()

    def reset(self):
        '''Resets the database'''
        return requests.post(self._api_url + "/reset")

    def raw_sql(self, sql):
        '''Runs a raw SQL query against the database'''
        return requests.post(self._api_url + "/raw_sql", data = json.dumps({"raw_sql": sql})).json()

    def get_results(self, model_space=None, n_results = 100):
        '''Gets the results for the given space key'''
        return requests.post(self._api_url + "/get_results",
                             data = json.dumps({"model_space": model_space or self._model_space, "n_results": n_results})).json()

    def get_task_status(self, task_id):
        '''Gets the status of a task'''
        return requests.post(self._api_url + f"/tasks/{task_id}").json()

    def create_index(self, model_space=None):
        '''Creates an index for the given space key'''
        return requests.post(self._api_url + "/create_index",
                             data = json.dumps({"model_space": model_space or self._model_space})).json()
