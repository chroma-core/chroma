from abc import ABC, abstractmethod
from typing import Union

class API(ABC):

    _model_space = 'default_scope'

    @abstractmethod
    def __init__(self):
        pass


    @abstractmethod
    def heartbeat(self):
        '''Returns the current server time in nanoseconds to check if the server is alive'''
        pass


    @abstractmethod
    def add(self,
            embedding: list,
            input_uri: list,
            dataset: list = None,
            inference_class: list = None,
            label_class: list = None,
            model_spaces: list = None):
        """Add embeddings to the data store"""
        pass


    @abstractmethod
    def count(self, model_space=None):
        '''Returns the number of embeddings in the database'''
        pass


    @abstractmethod
    def fetch(self, where={}, sort=None, limit=None, offset=None, page=None, page_size=None):
        '''Fetches embeddings from the database'''
        pass


    @abstractmethod
    def delete(self, where={}):
        '''Deletes embeddings from the database'''
        pass


    @abstractmethod
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
        pass


    @abstractmethod
    def get_nearest_neighbors(self, embedding, n_results=10, where={}):
        '''Gets the nearest neighbors of a single embedding'''
        pass


    @abstractmethod
    def process(self, model_space=None):
        '''
        Processes embeddings in the database
        - currently this only runs hnswlib, doesnt return anything
        '''
        pass


    @abstractmethod
    def reset(self):
        '''Resets the database'''
        pass


    @abstractmethod
    def raw_sql(self, sql):
        '''Runs a raw SQL query against the database'''
        pass


    @abstractmethod
    def get_results(self, model_space=None, n_results = 100):
        '''Gets the results for the given space key'''
        pass


    @abstractmethod
    def get_task_status(self, task_id):
        '''Gets the status of a task'''
        pass


    @abstractmethod
    def create_index(self, model_space=None):
        '''Creates an index for the given space key'''
        pass


    def set_model_space(self, model_space):
        '''Sets the space key for the client, enables overriding the string concat'''
        self._model_space = model_space


    def get_model_space(self):
        '''Returns the model_space key'''
        return self._model_space


    def where_with_model_space(self, where_clause):
        '''Returns a where clause that specifies the default model space iff it wasn't already specified'''

        if self._model_space and "model_space" not in where_clause:
            where_clause["model_space"] = self._model_space

        return where_clause


    def add_training(self, embedding: list, input_uri: list, inference_class: list, label_class: list = None, model_spaces: list = None):
        '''
        Small wrapper around add() to add a batch of training embedding - sets dataset to "training"
        '''
        datasets = ["training"] * len(input_uri)
        return self.add(
            embedding=embedding,
            input_uri=input_uri,
            dataset=datasets,
            inference_class=inference_class,
            model_spaces=model_spaces,
            label_class=label_class
        )


    def add_production(self, embedding: list, input_uri: list, inference_class: list, label_class: list = None, model_spaces: list = None):
        '''
        Small wrapper around add() to add a batch of production embedding - sets dataset to "production"
        '''
        datasets = ["production"] * len(input_uri)
        return self.add(
            embedding=embedding,
            input_uri=input_uri,
            dataset=datasets,
            inference_class=inference_class,
            model_spaces=model_spaces,
            label_class=label_class
        )


    def add_triage(self, embedding: list, input_uri: list, inference_class: list, label_class: list = None, model_spaces: list = None):
        '''
        Small wrapper around add() to add a batch of triage embedding - sets dataset to "triage"
        '''
        datasets = ["triage"] * len(input_uri)
        return self.add(
            embedding=embedding,
            input_uri=input_uri,
            dataset=datasets,
            inference_class=inference_class,
            model_spaces=model_spaces,
            label_class=label_class
        )
