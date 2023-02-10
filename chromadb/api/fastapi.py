from typing import Dict, Optional
from chromadb.api import API
from chromadb.errors import NoDatapointsException
import pandas as pd
import requests
import json
from chromadb.api.models.Collection import Collection


class FastAPI(API):
    def __init__(self, settings):
        self._api_url = (
            f"http://{settings.chroma_server_host}:{settings.chroma_server_http_port}/api/v1"
        )

    def heartbeat(self):
        """Returns the current server time in nanoseconds to check if the server is alive"""
        resp = requests.get(self._api_url)
        resp.raise_for_status()
        return int(resp.json()["nanosecond heartbeat"])

    def list_collections(self) -> int:
        """Returns a list of all collections"""
        resp = requests.get(self._api_url + "/collections")
        resp.raise_for_status()
        return resp.json()

    def create_collection(self, name: str, metadata: Optional[Dict] = None) -> Collection:
        """Creates a collection"""
        resp = requests.post(
            self._api_url + "/collections", data=json.dumps({"name": name, "metadata": metadata})
        )
        resp.raise_for_status()
        return Collection(self, name)

    def get_collection(self, name: str) -> Collection:
        """Returns a collection"""
        resp = requests.get(self._api_url + "/collections/" + name)
        resp.raise_for_status()
        return Collection(self, name)


    def modify(self, current_name, new_name: str, new_metadata: Optional[Dict] = None) -> int:
        '''Updates a collection'''
        resp = requests.put(self._api_url + "/collections/" + current_name, data=json.dumps({"metadata":new_metadata, "name":new_name}))
        resp.raise_for_status()
        return resp.json()

    def delete_collection(self, name: str) -> int:
        """Deletes a collection"""
        resp = requests.delete(self._api_url + "/collections/" + name)
        resp.raise_for_status()
        return resp.json()

    def count(self, collection_name=None):
        """Returns the number of embeddings in the database"""
        resp = requests.get(self._api_url + "/collections/" + collection_name + "/count")
        resp.raise_for_status()
        return resp.json()

    def peek(self, collection_name, limit=10):
        return self.get(collection_name, limit=limit)

    def get(
        self,
        collection_name,
        ids=None,
        where={},
        sort=None,
        limit=None,
        offset=None,
        page=None,
        page_size=None,
    ):
        """Gets embeddings from the database"""
        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        resp = requests.post(
            self._api_url + "/collections/" + collection_name + "/get",
            data=json.dumps(
                {"ids": ids, "where": where, "sort": sort, "limit": limit, "offset": offset}
            ),
        )

        resp.raise_for_status()
        return pd.DataFrame.from_dict(resp.json())

    def delete(self, collection_name, ids=None, where={}):
        """Deletes embeddings from the database"""

        resp = requests.post(
            self._api_url + "/collections/" + collection_name + "/delete",
            data=json.dumps({"where": where, "ids": ids}),
        )

        resp.raise_for_status()
        return resp.json()

    def add(
        self,
        collection_name,
        embeddings,
        metadatas=None,
        documents=None,
        ids=None,
        increment_index=True,
    ):
        """
        Adds a batch of embeddings to the database
        - pass in column oriented data lists
        - by default, the index is progressively built up as you add more data. If for ingestion performance reasons you want to disable this, set increment_index to False
        -     and then manually create the index yourself with collection.create_index()
        """

        resp = requests.post(
            self._api_url + "/collections/" + collection_name + "/add",
            data=json.dumps(
                {
                    "embeddings": embeddings,
                    "metadatas": metadatas,
                    "documents": documents,
                    "ids": ids,
                    "increment_index": increment_index,
                }
            ),
        )

        resp.raise_for_status
        return True

    def update(
        self,
        collection_name,
        embedding,
        metadata=None,
    ):
        """
        Updates a batch of embeddings in the database
        - pass in column oriented data lists
        """

        resp = requests.post(
            self._api_url + "/collections/" + collection_name + "/update",
            data=json.dumps(
                {
                    "embedding": embedding,
                    "metadata": metadata,
                }
            ),
        )

        resp.raise_for_status
        return True

    def query(self, collection_name, query_embeddings, n_results=10, where={}):
        """Gets the nearest neighbors of a single embedding"""

        resp = requests.post(
            self._api_url + "/collections/" + collection_name + "/query",
            data=json.dumps(
                {"query_embeddings": query_embeddings, "n_results": n_results, "where": where}
            ),
        )

        resp.raise_for_status()

        val = resp.json()
        if "error" in val:
            if val["error"] == "no data points":
                raise NoDatapointsException("No datapoints found for the supplied filter")
            else:
                raise Exception(val["error"])

        val["embeddings"] = pd.DataFrame.from_dict(val["embeddings"])

        return val

    def reset(self):
        """Resets the database"""
        resp = requests.post(self._api_url + "/reset")
        resp.raise_for_status()
        return resp.json

    def raw_sql(self, sql):
        """Runs a raw SQL query against the database"""
        resp = requests.post(self._api_url + "/raw_sql", data=json.dumps({"raw_sql": sql}))
        resp.raise_for_status()
        return pd.DataFrame.from_dict(resp.json())

    def create_index(self, collection_name=None):
        """Creates an index for the given space key"""
        resp = requests.post(self._api_url + "/collections/" + collection_name + "/create_index")
        resp.raise_for_status()
        return resp.json()
