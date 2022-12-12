from abc import ABC, abstractmethod
from typing import Union, Sequence, Optional, TypedDict, List, Dict
from uuid import UUID
import pandas as pd


class API(ABC):

    _model_space = "default_space"

    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def heartbeat(self) -> int:
        """Returns the current server time in nanoseconds to check if the server is alive

        Args:
            None

        Returns:
            int: The current server time in nanoseconds

        """
        pass

    @abstractmethod
    def add(
        self,
        embedding: Sequence[Sequence[float]],
        model_space: Union[str, Sequence[str]],
        input_uri: Optional[Sequence[str]] = None,
        dataset: Optional[Union[str, Sequence[str]]] = None,
        inference_class: Optional[Sequence[str]] = None,
        label_class: Optional[Sequence[str]] = None,
    ) -> bool:
        """Add embeddings to the data store. This is the most general way to add embeddings to the database.
        ⚠️ It is recommended to use the more specific methods below when possible.

        Args:
            model_space (Union[str, Sequence[str]]): The model space(s) to add the embeddings to
            embedding (Sequence[Sequence[float]]): The sequence of embeddings to add
            input_uri (Optional[Sequence[str]], optional): The input uris corresponding to each embedding. Defaults to None.
            dataset (Optional[Union[str, Sequence[str]]], optional): The dataset(s) the embeddings belong to. Defaults to None.
            inference_class (Optional[Sequence[str]], optional): The inferred classes of the model outputs. Defaults to None.
            label_class (Optional[Sequence[str]], optional): The labeled classes of the outputs (e.g. for training data). Defaults to None.
        Returns:
            bool: True if the embeddings were added successfully
        """
        pass

    @abstractmethod
    def count(self, model_space: Optional[str] = None) -> int:
        """Returns the number of embeddings in the database

        Args:
            model_space (Optional[str], optional): The model space to count the embeddings in. If None (default), returns the total count of all embeddings.

        Returns:
            int: The number of embeddings in the database

        """
        pass

    @abstractmethod
    def fetch(
        self,
        where: Optional[Dict[str, str]] = {},
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> pd.DataFrame:
        """Fetches embeddings from the database. Supports filtering, sorting, and pagination.
        ⚠️ This method should not be used directly.

        Args:
            where (Optional[Dict[str, str]], optional): A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.
            sort (Optional[str], optional): The column to sort the embeddings by. Defaults to None.
            limit (Optional[int], optional): The maximum number of embeddings to return. Defaults to None.
            offset (Optional[int], optional): The number of embeddings to skip before returning. Defaults to None.
            page (Optional[int], optional): The page number to return. Defaults to None.
            page_size (Optional[int], optional): The number of embeddings to return per page. Defaults to None.

        Returns:
            pd.DataFrame: A pandas dataframe containing the embeddings and metadata

        """
        pass

    @abstractmethod
    def delete(self, where: Optional[Dict[str, str]] = {}) -> Sequence[UUID]:
        """Deletes embeddings from the database
        ⚠️ This method should not be used directly.

        Args:
            where (Optional[Dict[str, str]], optional): A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.

        Returns:
            Sequence[UUID]: A list of the UUIDs of the embeddings that were deleted
        """
        pass

    class NearestNeighborsResult(TypedDict):
        ids: Sequence[UUID]
        embeddings: pd.DataFrame
        distances: Sequence[float]

    @abstractmethod
    def get_nearest_neighbors(
        self, embedding: Sequence[float], n_results: int = 10, where: Dict[str, str] = {}
    ) -> NearestNeighborsResult:
        """Gets the nearest neighbors of a single embedding
        ⚠️ This method should not be used directly.

        Args:
            embedding (Sequence[float]): The embedding to find the nearest neighbors of
            n_results (int, optional): The number of nearest neighbors to return. Defaults to 10.
            where (Dict[str, str], optional): A dictionary of key-value pairs to filter the embeddings by. Defaults to {}.
        """
        pass

    @abstractmethod
    def process(
        self,
        model_space: Optional[str] = None,
        training_dataset_name: str = "training",
        unlabeled_dataset_name: str = "unlabeled",
    ) -> bool:
        """
        Processes analysis for the given model space, using the Chroma algorithms, to determine the best data to label. This is then available with `get_results`.
        ⚠️ This method may take a long time to run.

        Args:
            model_space (Optional[str], optional): The model space to process. Defaults to None.
            training_dataset_name (str, optional): The name of the training dataset. Defaults to "training".
            unlabeled_dataset_name (str, optional): The name of the unlabeled dataset. Defaults to "unlabeled".

        Returns:
            bool: True if the processing was successful
        """
        pass

    @abstractmethod
    def reset(self) -> bool:
        """Resets the database
        ⚠️ This is destructive and will delete all data in the database.
        Args:
            None

        Returns:
            bool: True if the reset was successful
        """
        pass

    @abstractmethod
    def raw_sql(self, sql: str) -> pd.DataFrame:
        """Runs a raw SQL query against the database
        ⚠️ This method should not be used directly.

        Args:
            sql (str): The SQL query to run

        Returns:
            pd.DataFrame: A pandas dataframe containing the results of the query
        """
        pass

    @abstractmethod
    def get_results(
        self,
        model_space: Optional[str] = None,
        dataset_name: str = "unlabeled",
        n_results: int = 100,
    ) -> pd.DataFrame:
        """Get the set of unlabeled data to label, for a given unlabaled dataset and model space.

        Args:
            model_space (Optional[str], optional): The model space to get the results for. Uses the client's set space if None.
            dataset_name (str, optional): The name of the dataset to get the results for. Defaults to "unlabeled".
            n_results (int, optional): The number of results to return. Defaults to 100.

        Returns:
            pd.DataFrame: A pandas dataframe containing the URIs of the inputs to label.

        """
        pass

    @abstractmethod
    def get_task_status(self, task_id):
        """Gets the status of a task
        ⚠️ Not implemented. Will error.
        """

        pass

    @abstractmethod
    def create_index(self, model_space: Optional[str] = None) -> bool:
        """Creates an index for the given model space
        ⚠️ This method should not be used directly.

        Args:
            model_space (Optional[str], optional): The model space to create the index for. Uses the client's model space if None. Defaults to None.

        Returns:
            bool: True if the index was created successfully

        """
        pass

    def set_model_space(self, model_space: str) -> None:
        """Sets the model space name for the client, allowing it to be omitted elsewhere

        Args:
            model_space (str): The model space name

        Returns:
            None

        """
        self._model_space = model_space

    def get_model_space(self) -> str:
        """Returns the model space name the client has

        Args:
            None

        Returns:
            str: The model space name

        """
        return self._model_space

    def where_with_model_space(self, where_clause: Dict[str, str]) -> Dict[str, str]:
        """Returns a where clause that specifies the default model space iff it wasn't already specified
        ⚠️ This method should not be used directly.

        Args:
            where_clause (dict): The where clause to add the model space name to

        Returns:
            dict: The where clause with the model space name added
        """

        if self._model_space and "model_space" not in where_clause:
            where_clause["model_space"] = self._model_space

        return where_clause

    def add_training(
        self,
        embedding: Sequence[Sequence[float]],
        input_uri: Sequence[str],
        inference_class: Sequence[str],
        label_class: Sequence[str],
        model_space: Optional[Union[str, Sequence[str]]] = None,
    ) -> bool:
        """Adds a batch of training data to the database. Requires the embedding, input URI, inference class, and label class for each input.

        Args:
            embedding (Sequence[Sequence[float]]): The embeddings to add
            input_uri ([Sequence[str]): The input URIs to add.
            inference_class (Sequence[str]): The inference classes to add.
            label_class (Sequence[str]): The label classes to add.
            model_space (Optional[Union[str, Sequence[str]]], optional): The model space name(s) to add the data to. Defaults to the client's model space.

        Returns:
            bool: True if the data was added successfully
        """

        dataset = ["training"] * len(input_uri)
        if not model_space:
            model_space = self._model_space
        return self.add(
            embedding=embedding,
            input_uri=input_uri,
            dataset=dataset,
            inference_class=inference_class,
            label_class=label_class,
            model_space=model_space,
        )

    def add_unlabeled(
        self,
        embedding: Sequence[Sequence[float]],
        input_uri: Sequence[str],
        inference_class: Sequence[str],
        model_space: Optional[Union[str, Sequence[str]]] = None,
    ) -> bool:
        """Adds a batch of unlabeled data to analyze. By default, chroma selects data from this set for you to label. Requires the embedding, input URI, and inference class for each input.

        Args:
            embedding (Sequence[Sequence[float]]): The embeddings to add
            input_uri (Sequence[str]): The input URIs to add.
            inference_class (Sequence[str]): The inference classes to add.
            model_space (Optional[Union[str, Sequence[str]]]): The model space name(s) to add the data to. Defaults to the client's model space.

        Returns:
            bool: True if the data was added successfully

        """
        dataset = ["unlabeled"] * len(input_uri)
        if not model_space:
            model_space = self._model_space
        return self.add(
            embedding=embedding,
            model_space=model_space,
            input_uri=input_uri,
            dataset=dataset,
            inference_class=inference_class,
        )

    def add_triage(
        self,
        embedding: Sequence[Sequence[float]],
        input_uri: Sequence[str],
        inference_class: Sequence[str],
        label_class: Sequence[str],
        model_space: Optional[Union[str, Sequence[str]]] = None,
    ) -> bool:
        """Adds a batch of triage data to the database. These are examples which humans have detected as being problematic. Chroma will find similar examples automatically, and prioritize them for labeling.
        ⚠️ This functionality is not yet implemented.

        Args:
            embedding (Sequence[Sequence[float]]): The embeddings to add
            input_uri ([Sequence[str]): The input URIs to add.
            inference_class (Sequence[str]): The inference classes to add.
            label_class (Sequence[str]): The label classes to add.
            model_space (Optional[Union[str, Sequence[str]]], optional): The model space name(s) to add the data to. Defaults to the client's model space.

        Returns:
            bool: True if the data was added successfully


        """
        datasets = ["triage"] * len(input_uri)
        if not model_space:
            model_space = self._model_space
        return self.add(
            embedding=embedding,
            model_space=model_space,
            input_uri=input_uri,
            dataset=datasets,
            inference_class=inference_class,
            label_class=label_class,
        )
