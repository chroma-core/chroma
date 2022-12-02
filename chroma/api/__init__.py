from abc import ABC, abstractmethod
from typing import Union, Sequence, Optional, TypedDict
from uuid import UUID
import pandas as pd

class API(ABC):

    _model_space = 'default_scope'

    @abstractmethod
    def __init__(self):
        pass


    @abstractmethod
    def heartbeat(self) -> int:
        '''Returns the current server time in nanoseconds to check if the server is alive'''
        pass


    @abstractmethod
    def add(self,
            model_space: Union[str, Sequence[str]],
            embedding: Sequence[Sequence[float]],
            input_uri: Optional[Sequence[str]] = None,
            dataset: Optional[Union[str, Sequence[str]]] = None,
            inference_class: Optional[Sequence[str]] = None,
            label_class: Optional[Sequence[str]] = None) -> bool:
        """Add embeddings to the data store"""
        pass


    @abstractmethod
    def count(self, model_space: Optional[str]=None) -> int:
        '''Returns the number of embeddings in the database'''
        pass


    @abstractmethod
    def fetch(self,
              where: Optional[dict[str, str]]={},
              sort: Optional[str]=None,
              limit: Optional[int]=None,
              offset: Optional[int]=None,
              page: Optional[int]=None,
              page_size: Optional[int]=None) -> pd.DataFrame:
        '''Fetches embeddings from the database'''
        pass


    @abstractmethod
    def delete(self, where: Optional[dict[str, str]]={}) -> Sequence[UUID]:
        '''Deletes embeddings from the database'''
        pass


    class NearestNeighborsResult(TypedDict):
        ids: Sequence[UUID]
        embeddings: pd.DataFrame
        distances: Sequence[float]

    @abstractmethod
    def get_nearest_neighbors(self,
                              embedding: Sequence[float],
                              n_results: int=10,
                              where: dict[str, str]={}) -> NearestNeighborsResult:
        '''Gets the nearest neighbors of a single embedding'''
        pass


    @abstractmethod
    def process(self,
                model_space: Optional[str]=None,
                training_dataset_name: str="training",
                inference_dataset_name: str="inference") -> bool:
        '''
        Processes embeddings in the database
        - currently this only runs hnswlib, doesnt return anything
        '''
        pass


    @abstractmethod
    def reset(self) -> bool:
        '''Resets the database'''
        pass


    @abstractmethod
    def raw_sql(self, sql: str) -> pd.DataFrame:
        '''Runs a raw SQL query against the database'''
        pass


    @abstractmethod
    def get_results(self,
                    model_space:Optional[str] = None,
                    dataset_name: str = "inference",
                    n_results:int = 100) -> pd.DataFrame:
        '''Gets the results for the given space key'''
        pass


    @abstractmethod
    def get_task_status(self, task_id):
        '''Gets the status of a task'''
        pass


    @abstractmethod
    def create_index(self,
                     model_space:Optional[str] = None) -> bool:
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


    def add_training(self, embedding: list, input_uri: list, inference_class: list, label_class: list = None, model_space: list = None):
        '''
        Small wrapper around add() to add a batch of training embedding - sets dataset to "training"
        '''
        datasets = ["training"] * len(input_uri)
        return self.add(
            embedding=embedding,
            input_uri=input_uri,
            dataset=datasets,
            inference_class=inference_class,
            model_space=model_space,
            label_class=label_class
        )


    def add_production(self, embedding: list, input_uri: list, inference_class: list, label_class: list = None, model_space: list = None):
        '''
        Small wrapper around add() to add a batch of production embedding - sets dataset to "production"
        '''
        datasets = ["production"] * len(input_uri)
        return self.add(
            embedding=embedding,
            input_uri=input_uri,
            dataset=datasets,
            inference_class=inference_class,
            model_space=model_space,
            label_class=label_class
        )


    def add_triage(self, embedding: list, input_uri: list, inference_class: list, label_class: list = None, model_space: list = None):
        '''
        Small wrapper around add() to add a batch of triage embedding - sets dataset to "triage"
        '''
        datasets = ["triage"] * len(input_uri)
        return self.add(
            embedding=embedding,
            input_uri=input_uri,
            dataset=datasets,
            inference_class=inference_class,
            model_space=model_space,
            label_class=label_class
        )
