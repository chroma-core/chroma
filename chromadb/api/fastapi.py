import json
import logging
from typing import Optional, cast, Tuple
from typing import Sequence
from uuid import UUID

import requests
from overrides import override

import chromadb.errors as errors
import chromadb.utils.embedding_functions as ef
from chromadb.api import API
from chromadb.api.models.Collection import Collection
from chromadb.api.types import (
    Documents,
    Embeddings,
    EmbeddingFunction,
    IDs,
    Include,
    Metadatas,
    Where,
    WhereDocument,
    GetResult,
    QueryResult,
    CollectionMetadata,
    validate_batch,
)
from chromadb.auth import (
    ClientAuthProvider,
)
from chromadb.auth.providers import RequestsClientAuthProtocolAdapter
from chromadb.auth.registry import resolve_provider
from chromadb.config import Settings, System
from chromadb.telemetry import Telemetry
from urllib.parse import urlparse, urlunparse, quote

logger = logging.getLogger(__name__)


class FastAPI(API):
    _settings: Settings
    _max_batch_size: int = -1

    @staticmethod
    def _validate_host(host: str) -> None:
        parsed = urlparse(host)
        if "/" in host and parsed.scheme not in {"http", "https"}:
            raise ValueError(
                "Invalid URL. " f"Unrecognized protocol - {parsed.scheme}."
            )
        if "/" in host and (not host.startswith("http")):
            raise ValueError(
                "Invalid URL. "
                "Seems that you are trying to pass URL as a host but without specifying the protocol. "
                "Please add http:// or https:// to the host."
            )

    @staticmethod
    def resolve_url(
        chroma_server_host: str,
        chroma_server_ssl_enabled: Optional[bool] = False,
        default_api_path: Optional[str] = "",
        chroma_server_http_port: Optional[int] = 8000,
    ) -> str:
        _skip_port = False
        _chroma_server_host = chroma_server_host
        FastAPI._validate_host(_chroma_server_host)
        if _chroma_server_host.startswith("http"):
            logger.debug("Skipping port as the user is passing a full URL")
            _skip_port = True
        parsed = urlparse(_chroma_server_host)

        scheme = "https" if chroma_server_ssl_enabled else parsed.scheme or "http"
        net_loc = parsed.netloc or parsed.hostname or chroma_server_host
        port = (
            ":" + str(parsed.port or chroma_server_http_port) if not _skip_port else ""
        )
        path = parsed.path or default_api_path

        if not path or path == net_loc:
            path = default_api_path if default_api_path else ""
        if not path.endswith(default_api_path or ""):
            path = path + default_api_path if default_api_path else ""
        full_url = urlunparse(
            (scheme, f"{net_loc}{port}", quote(path.replace("//", "/")), "", "", "")
        )

        return full_url

    def __init__(self, system: System):
        super().__init__(system)
        system.settings.require("chroma_server_host")
        system.settings.require("chroma_server_http_port")

        self._telemetry_client = self.require(Telemetry)
        self._settings = system.settings

        self._api_url = FastAPI.resolve_url(
            chroma_server_host=str(system.settings.chroma_server_host),
            chroma_server_http_port=int(str(system.settings.chroma_server_http_port)),
            chroma_server_ssl_enabled=system.settings.chroma_server_ssl_enabled,
            default_api_path=system.settings.chroma_server_api_default_path,
        )

        self._header = system.settings.chroma_server_headers
        if (
            system.settings.chroma_client_auth_provider
            and system.settings.chroma_client_auth_protocol_adapter
        ):
            self._auth_provider = self.require(
                resolve_provider(
                    system.settings.chroma_client_auth_provider, ClientAuthProvider
                )
            )
            self._adapter = cast(
                RequestsClientAuthProtocolAdapter,
                system.require(
                    resolve_provider(
                        system.settings.chroma_client_auth_protocol_adapter,
                        RequestsClientAuthProtocolAdapter,
                    )
                ),
            )
            self._session = self._adapter.session
        else:
            self._session = requests.Session()
        if self._header is not None:
            self._session.headers.update(self._header)

    @override
    def heartbeat(self) -> int:
        """Returns the current server time in nanoseconds to check if the server is alive"""
        resp = self._session.get(self._api_url)
        raise_chroma_error(resp)
        return int(resp.json()["nanosecond heartbeat"])

    @override
    def list_collections(self) -> Sequence[Collection]:
        """Returns a list of all collections"""
        resp = self._session.get(self._api_url + "/collections")
        raise_chroma_error(resp)
        json_collections = resp.json()
        collections = []
        for json_collection in json_collections:
            collections.append(Collection(self, **json_collection))

        return collections

    @override
    def create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
        get_or_create: bool = False,
    ) -> Collection:
        """Creates a collection"""
        resp = self._session.post(
            self._api_url + "/collections",
            data=json.dumps(
                {"name": name, "metadata": metadata, "get_or_create": get_or_create}
            ),
        )
        raise_chroma_error(resp)
        resp_json = resp.json()
        return Collection(
            client=self,
            id=resp_json["id"],
            name=resp_json["name"],
            embedding_function=embedding_function,
            metadata=resp_json["metadata"],
        )

    @override
    def get_collection(
        self,
        name: str,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
    ) -> Collection:
        """Returns a collection"""
        resp = self._session.get(self._api_url + "/collections/" + name)
        raise_chroma_error(resp)
        resp_json = resp.json()
        return Collection(
            client=self,
            name=resp_json["name"],
            id=resp_json["id"],
            embedding_function=embedding_function,
            metadata=resp_json["metadata"],
        )

    @override
    def get_or_create_collection(
        self,
        name: str,
        metadata: Optional[CollectionMetadata] = None,
        embedding_function: Optional[EmbeddingFunction] = ef.DefaultEmbeddingFunction(),
    ) -> Collection:
        return self.create_collection(
            name, metadata, embedding_function, get_or_create=True
        )

    @override
    def _modify(
        self,
        id: UUID,
        new_name: Optional[str] = None,
        new_metadata: Optional[CollectionMetadata] = None,
    ) -> None:
        """Updates a collection"""
        resp = self._session.put(
            self._api_url + "/collections/" + str(id),
            data=json.dumps({"new_metadata": new_metadata, "new_name": new_name}),
        )
        raise_chroma_error(resp)

    @override
    def delete_collection(self, name: str) -> None:
        """Deletes a collection"""
        resp = self._session.delete(self._api_url + "/collections/" + name)
        raise_chroma_error(resp)

    @override
    def _count(self, collection_id: UUID) -> int:
        """Returns the number of embeddings in the database"""
        resp = self._session.get(
            self._api_url + "/collections/" + str(collection_id) + "/count"
        )
        raise_chroma_error(resp)
        return cast(int, resp.json())

    @override
    def _peek(self, collection_id: UUID, n: int = 10) -> GetResult:
        return self._get(
            collection_id,
            limit=n,
            include=["embeddings", "documents", "metadatas"],
        )

    @override
    def _get(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        sort: Optional[str] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        where_document: Optional[WhereDocument] = {},
        include: Include = ["metadatas", "documents"],
    ) -> GetResult:
        if page and page_size:
            offset = (page - 1) * page_size
            limit = page_size

        resp = self._session.post(
            self._api_url + "/collections/" + str(collection_id) + "/get",
            data=json.dumps(
                {
                    "ids": ids,
                    "where": where,
                    "sort": sort,
                    "limit": limit,
                    "offset": offset,
                    "where_document": where_document,
                    "include": include,
                }
            ),
        )

        raise_chroma_error(resp)
        body = resp.json()
        return GetResult(
            ids=body["ids"],
            embeddings=body.get("embeddings", None),
            metadatas=body.get("metadatas", None),
            documents=body.get("documents", None),
        )

    @override
    def _delete(
        self,
        collection_id: UUID,
        ids: Optional[IDs] = None,
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
    ) -> IDs:
        """Deletes embeddings from the database"""
        resp = self._session.post(
            self._api_url + "/collections/" + str(collection_id) + "/delete",
            data=json.dumps(
                {"where": where, "ids": ids, "where_document": where_document}
            ),
        )

        raise_chroma_error(resp)
        return cast(IDs, resp.json())

    def _submit_batch(
        self,
        batch: Tuple[
            IDs, Optional[Embeddings], Optional[Metadatas], Optional[Documents]
        ],
        url: str,
    ) -> requests.Response:
        """
        Submits a batch of embeddings to the database
        """
        resp = self._session.post(
            self._api_url + url,
            data=json.dumps(
                {
                    "ids": batch[0],
                    "embeddings": batch[1],
                    "metadatas": batch[2],
                    "documents": batch[3],
                }
            ),
        )
        return resp

    @override
    def _add(
        self,
        ids: IDs,
        collection_id: UUID,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ) -> bool:
        """
        Adds a batch of embeddings to the database
        - pass in column oriented data lists
        """
        batch = (ids, embeddings, metadatas, documents)
        validate_batch(batch, {"max_batch_size": self.max_batch_size})
        resp = self._submit_batch(batch, "/collections/" + str(collection_id) + "/add")
        raise_chroma_error(resp)
        return True

    @override
    def _update(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Optional[Embeddings] = None,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ) -> bool:
        """
        Updates a batch of embeddings in the database
        - pass in column oriented data lists
        """
        batch = (ids, embeddings, metadatas, documents)
        validate_batch(batch, {"max_batch_size": self.max_batch_size})
        resp = self._submit_batch(
            batch, "/collections/" + str(collection_id) + "/update"
        )
        resp.raise_for_status()
        return True

    @override
    def _upsert(
        self,
        collection_id: UUID,
        ids: IDs,
        embeddings: Embeddings,
        metadatas: Optional[Metadatas] = None,
        documents: Optional[Documents] = None,
    ) -> bool:
        """
        Upserts a batch of embeddings in the database
        - pass in column oriented data lists
        """
        batch = (ids, embeddings, metadatas, documents)
        validate_batch(batch, {"max_batch_size": self.max_batch_size})
        resp = self._submit_batch(
            batch, "/collections/" + str(collection_id) + "/upsert"
        )
        resp.raise_for_status()
        return True

    @override
    def _query(
        self,
        collection_id: UUID,
        query_embeddings: Embeddings,
        n_results: int = 10,
        where: Optional[Where] = {},
        where_document: Optional[WhereDocument] = {},
        include: Include = ["metadatas", "documents", "distances"],
    ) -> QueryResult:
        """Gets the nearest neighbors of a single embedding"""
        resp = self._session.post(
            self._api_url + "/collections/" + str(collection_id) + "/query",
            data=json.dumps(
                {
                    "query_embeddings": query_embeddings,
                    "n_results": n_results,
                    "where": where,
                    "where_document": where_document,
                    "include": include,
                }
            ),
        )

        raise_chroma_error(resp)
        body = resp.json()

        return QueryResult(
            ids=body["ids"],
            distances=body.get("distances", None),
            embeddings=body.get("embeddings", None),
            metadatas=body.get("metadatas", None),
            documents=body.get("documents", None),
        )

    @override
    def reset(self) -> bool:
        """Resets the database"""
        resp = self._session.post(self._api_url + "/reset")
        raise_chroma_error(resp)
        return cast(bool, resp.json())

    @override
    def get_version(self) -> str:
        """Returns the version of the server"""
        resp = self._session.get(self._api_url + "/version")
        raise_chroma_error(resp)
        return cast(str, resp.json())

    @override
    def get_settings(self) -> Settings:
        """Returns the settings of the client"""
        return self._settings

    @property
    @override
    def max_batch_size(self) -> int:
        if self._max_batch_size == -1:
            resp = self._session.get(self._api_url + "/pre-flight-checks")
            raise_chroma_error(resp)
            self._max_batch_size = cast(int, resp.json()["max_batch_size"])
        return self._max_batch_size


def raise_chroma_error(resp: requests.Response) -> None:
    """Raises an error if the response is not ok, using a ChromaError if possible"""
    if resp.ok:
        return

    chroma_error = None
    try:
        body = resp.json()
        if "error" in body:
            if body["error"] in errors.error_types:
                chroma_error = errors.error_types[body["error"]](body["message"])

    except BaseException:
        pass

    if chroma_error:
        raise chroma_error

    try:
        resp.raise_for_status()
    except requests.HTTPError:
        raise (Exception(resp.text))
