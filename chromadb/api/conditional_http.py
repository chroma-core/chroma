from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple
from uuid import UUID

from chromadb.api.types import IDs, convert_np_embeddings_to_list
from chromadb.errors import InternalError, InvalidArgumentError


ConditionalHttpGetPayload = Dict[str, Any]
ConditionalHttpJsonPayload = Dict[str, Any]
ConditionalHttpPayload = Dict[str, Any]
_MAX_I64 = (1 << 63) - 1


@dataclass(frozen=True)
class ConditionalHttpScope:
    collection_id: str
    tenant: str
    database: str


class ConditionalHttpTransaction:
    def __init__(self) -> None:
        self._scope: Optional[ConditionalHttpScope] = None
        self._read_ids: Set[str] = set()
        self._read_token: Optional[int] = None
        self._known_present: Set[str] = set()
        self._known_absent: Set[str] = set()
        self._buffered_write_ids: Set[str] = set()
        self._operations: List[ConditionalHttpJsonPayload] = []
        self._closed = False

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
        self._validate_get_request(request_payload)
        request_payload["read_token"] = self._read_token
        return request_payload

    def record_get(
        self,
        request_payload: ConditionalHttpGetPayload,
        returned_ids: IDs,
        read_token: int,
    ) -> None:
        self._ensure_open()
        self._require_scope()
        self._validate_get_request(request_payload)
        self._validate_read_token(request_payload.get("read_token"), read_token)

        returned_id_set = set(returned_ids)
        for id in returned_ids:
            if id in self._buffered_write_ids:
                raise _invalid_read_after_write(id)

        next_read_ids = set(self._read_ids)
        next_known_present = set(self._known_present)
        next_known_absent = set(self._known_absent)
        ids = request_payload.get("ids")

        if ids is not None:
            next_read_ids.update(ids)
            for id in returned_ids:
                next_read_ids.add(id)
                next_known_present.add(id)
                next_known_absent.discard(id)
            if not request_payload.get("where") and not request_payload.get(
                "where_document"
            ):
                for id in ids:
                    if id not in returned_id_set:
                        next_known_absent.add(id)
                        next_known_present.discard(id)
        else:
            for id in returned_ids:
                next_read_ids.add(id)
                next_known_present.add(id)
                next_known_absent.discard(id)

        self._read_ids = next_read_ids
        self._known_present = next_known_present
        self._known_absent = next_known_absent
        if self._read_token is None:
            self._read_token = read_token

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
        self._buffer_write(
            "add",
            ids,
            {
                "ids": ids,
                "embeddings": convert_np_embeddings_to_list(embeddings),
                "documents": documents,
                "uris": uris,
                "metadatas": metadatas,
            },
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
        self._buffer_write(
            "update",
            ids,
            {
                "ids": ids,
                "embeddings": (
                    convert_np_embeddings_to_list(embeddings)
                    if embeddings is not None
                    else None
                ),
                "documents": documents,
                "uris": uris,
                "metadatas": metadatas,
            },
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
        self._buffer_write(
            "upsert",
            ids,
            {
                "ids": ids,
                "embeddings": convert_np_embeddings_to_list(embeddings),
                "documents": documents,
                "uris": uris,
                "metadatas": metadatas,
            },
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
        self._buffer_write(
            "delete",
            ids,
            {
                "ids": ids,
                "where": None,
                "where_document": None,
                "limit": None,
            },
        )

    def prepare_commit(self) -> Optional[ConditionalHttpPayload]:
        self._ensure_open()
        if not self._operations:
            self._closed = True
            return None
        return {
            "read_token": self._read_token,
            "read_ids": sorted(self._read_ids),
            "operations": self._operations.copy(),
        }

    def prepare_commit_payload(
        self,
    ) -> Optional[Tuple[ConditionalHttpScope, ConditionalHttpJsonPayload]]:
        prepared_commit = self.prepare_commit()
        if prepared_commit is None:
            return None
        scope = self._require_scope()
        return (scope, prepared_commit)

    def close(self, first_inserted_record_offset: Optional[int] = None) -> None:
        self._ensure_open()
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
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

    def _validate_get_request(self, request_payload: ConditionalHttpGetPayload) -> None:
        ids = request_payload.get("ids")
        if ids is not None:
            for id in ids:
                if id in self._buffered_write_ids:
                    raise _invalid_read_after_write(id)
            return

        limit = request_payload.get("limit")
        if not isinstance(limit, int) or limit <= 0:
            raise InvalidArgumentError(
                "transactional filter reads require a positive limit"
            )

    def _validate_read_token(
        self, expected_read_token: Optional[int], actual_read_token: Optional[int]
    ) -> None:
        if actual_read_token is None:
            raise InternalError(
                "transactional get response did not include an OCC read token"
            )
        if actual_read_token > _MAX_I64:
            raise InternalError(
                f"transactional read token offset {actual_read_token} exceeds i64 range"
            )
        if expected_read_token is not None and expected_read_token != actual_read_token:
            raise InternalError(
                "transactional read token changed from log upper bound offset "
                f"{expected_read_token} to {actual_read_token}"
            )
        if self._read_token is not None and self._read_token != actual_read_token:
            raise InternalError(
                "transactional read token changed from log upper bound offset "
                f"{self._read_token} to {actual_read_token}"
            )

    def _buffer_write(
        self,
        operation: str,
        ids: IDs,
        payload: ConditionalHttpJsonPayload,
    ) -> None:
        self._validate_buffered_write(operation, ids)
        for id in ids:
            self._buffered_write_ids.add(id)
        self._operations.append({"operation": operation, "payload": payload})

    def _validate_buffered_write(self, operation: str, ids: IDs) -> None:
        call_ids: Set[str] = set()
        for id in ids:
            if id in call_ids:
                raise InvalidArgumentError(
                    f'transactional write request contains duplicate id "{id}"'
                )
            call_ids.add(id)
            if id in self._buffered_write_ids:
                raise InvalidArgumentError(
                    f'transaction already has a buffered write for id "{id}"'
                )
            self._validate_write_precondition(operation, id)

    def _validate_write_precondition(self, operation: str, id: str) -> None:
        if operation == "add" and id not in self._known_absent:
            raise InvalidArgumentError(
                f'transactional add for id "{id}" requires a prior read '
                "proving the id is absent"
            )
        if operation == "update" and id not in self._known_present:
            raise InvalidArgumentError(
                f'transactional update for id "{id}" requires a prior read '
                "proving the id is present"
            )
        if operation == "delete" and id not in self._known_present:
            raise InvalidArgumentError(
                f'transactional delete for id "{id}" requires a prior read '
                "proving the id is present"
            )


def require_conditional_http_transaction(
    transaction: object,
) -> ConditionalHttpTransaction:
    if not isinstance(transaction, ConditionalHttpTransaction):
        raise ValueError("invalid conditional transaction for HTTP client")
    return transaction


def _invalid_read_after_write(id: str) -> InvalidArgumentError:
    return InvalidArgumentError(
        f'cannot transactionally read id "{id}" after buffering a write for it'
    )
