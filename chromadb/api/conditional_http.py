import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple
from uuid import UUID

import chromadb_rust_bindings

from chromadb.api.types import IDs, convert_np_embeddings_to_list


ConditionalHttpGetPayload = Dict[str, Any]
ConditionalHttpJsonPayload = Dict[str, Any]
ConditionalHttpPayload = chromadb_rust_bindings.ConditionalCommitPayload


@dataclass(frozen=True)
class ConditionalHttpScope:
    collection_id: str
    tenant: str
    database: str


class ConditionalHttpTransaction:
    def __init__(self) -> None:
        self._transaction = chromadb_rust_bindings.ConditionalTransaction()

        self._scope: Optional[ConditionalHttpScope] = None

    def prepare_get(
        self,
        collection_id: UUID,
        tenant: str,
        database: str,
        payload: ConditionalHttpGetPayload,
    ) -> ConditionalHttpGetPayload:
        self._ensure_open()
        scope = self._record_scope(collection_id, tenant, database)

        request_payload = dict(payload)
        request_payload["read_token"] = self._transaction.prepare_get(
            scope.collection_id,
            request_payload.get("ids"),
            _json_or_none(request_payload.get("where")),
            request_payload.get("limit"),
            request_payload.get("offset"),
            _json_or_none(request_payload.get("where_document")),
            request_payload.get("include", []),
            scope.tenant,
            scope.database,
        )
        return request_payload

    def record_get(
        self,
        request_payload: ConditionalHttpGetPayload,
        returned_ids: IDs,
        read_token: int,
    ) -> None:
        self._ensure_open()
        scope = self._require_scope()
        self._transaction.record_get_response(
            scope.collection_id,
            request_payload.get("ids"),
            _json_or_none(request_payload.get("where")),
            request_payload.get("limit"),
            request_payload.get("offset"),
            _json_or_none(request_payload.get("where_document")),
            request_payload.get("include", []),
            scope.tenant,
            scope.database,
            returned_ids,
            read_token,
        )

    def buffer_add(
        self,
        collection_id: UUID,
        tenant: str,
        database: str,
        ids: IDs,
        embeddings: Any,
        metadatas: Any = None,
        documents: Any = None,
        uris: Any = None,
    ) -> None:
        self._ensure_open()
        self._record_scope(collection_id, tenant, database)
        self._transaction.buffer_add(
            str(collection_id),
            ids,
            convert_np_embeddings_to_list(embeddings),
            metadatas,
            documents,
            uris,
            tenant,
            database,
        )

    def buffer_update(
        self,
        collection_id: UUID,
        tenant: str,
        database: str,
        ids: IDs,
        embeddings: Any = None,
        metadatas: Any = None,
        documents: Any = None,
        uris: Any = None,
    ) -> None:
        self._ensure_open()
        self._record_scope(collection_id, tenant, database)
        self._transaction.buffer_update(
            str(collection_id),
            ids,
            convert_np_embeddings_to_list(embeddings) if embeddings is not None else None,
            metadatas,
            documents,
            uris,
            tenant,
            database,
        )

    def buffer_upsert(
        self,
        collection_id: UUID,
        tenant: str,
        database: str,
        ids: IDs,
        embeddings: Any,
        metadatas: Any = None,
        documents: Any = None,
        uris: Any = None,
    ) -> None:
        self._ensure_open()
        self._record_scope(collection_id, tenant, database)
        self._transaction.buffer_upsert(
            str(collection_id),
            ids,
            convert_np_embeddings_to_list(embeddings),
            metadatas,
            documents,
            uris,
            tenant,
            database,
        )

    def buffer_delete(
        self,
        collection_id: UUID,
        tenant: str,
        database: str,
        ids: IDs,
    ) -> None:
        self._ensure_open()
        self._record_scope(collection_id, tenant, database)
        self._transaction.buffer_delete(str(collection_id), ids, tenant, database)

    def prepare_commit(self) -> Optional[ConditionalHttpPayload]:
        self._ensure_open()
        prepared_commit = self._transaction.prepare_commit()
        if prepared_commit is None:
            return None
        return prepared_commit

    def prepare_commit_payload(
        self,
    ) -> Optional[Tuple[ConditionalHttpScope, ConditionalHttpJsonPayload]]:
        prepared_commit = self.prepare_commit()
        if prepared_commit is None:
            return None
        scope = self._require_scope()
        return (scope, prepared_commit.to_json())

    def close(self, first_inserted_record_offset: Optional[int] = None) -> None:
        self._transaction.finish_commit(first_inserted_record_offset)

    def _ensure_open(self) -> None:
        if self._transaction.is_closed():
            raise ValueError("conditional transaction is closed")

    def _record_scope(
        self, collection_id: UUID, tenant: str, database: str
    ) -> ConditionalHttpScope:
        scope = ConditionalHttpScope(str(collection_id), tenant, database)
        if self._scope is None:
            self._scope = scope
        elif self._scope != scope:
            raise ValueError("conditional transaction cannot span collections")
        return scope

    def _require_scope(self) -> ConditionalHttpScope:
        if self._scope is None:
            raise ValueError("conditional transaction has no collection scope")
        return self._scope


def require_conditional_http_transaction(
    transaction: object,
) -> ConditionalHttpTransaction:
    if not isinstance(transaction, ConditionalHttpTransaction):
        raise ValueError("invalid conditional transaction for HTTP client")
    return transaction


def _json_or_none(value: Any) -> Optional[str]:
    return json.dumps(value) if value else None
