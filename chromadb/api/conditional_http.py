from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from uuid import UUID

from chromadb.api.types import IDs


ConditionalHttpPayload = Dict[str, Any]


@dataclass(frozen=True)
class ConditionalHttpScope:
    collection_id: str
    tenant: str
    database: str


class ConditionalHttpTransaction:
    def __init__(self) -> None:
        self.read_token: Optional[int] = None
        self.operations: List[ConditionalHttpPayload] = []

        self._scope: Optional[ConditionalHttpScope] = None
        self._has_buffered_writes = False
        self._closed = False

    def prepare_get(
        self,
        collection_id: UUID,
        tenant: str,
        database: str,
        payload: ConditionalHttpPayload,
    ) -> ConditionalHttpPayload:
        self._ensure_open()
        self._record_scope(collection_id, tenant, database)

        request_payload = dict(payload)
        request_payload["read_token"] = self.read_token
        return request_payload

    def record_get(
        self,
        request_payload: ConditionalHttpPayload,
        returned_ids: IDs,
        read_token: int,
    ) -> None:
        self._ensure_open()
        self.read_token = read_token
        self.operations.append(
            {
                "operation": "get",
                "payload": {
                    "ids": request_payload.get("ids"),
                    "where": request_payload.get("where"),
                    "where_document": request_payload.get("where_document"),
                    "limit": request_payload.get("limit"),
                    "offset": request_payload.get("offset"),
                    "include": request_payload.get("include"),
                    "expected_ids": list(returned_ids),
                },
            }
        )

    def buffer_write(
        self,
        collection_id: UUID,
        tenant: str,
        database: str,
        operation: str,
        payload: ConditionalHttpPayload,
    ) -> None:
        self._ensure_open()
        self._record_scope(collection_id, tenant, database)
        self._has_buffered_writes = True
        self.operations.append({"operation": operation, "payload": payload})

    def prepare_commit_payload(
        self,
    ) -> Optional[Tuple[ConditionalHttpScope, ConditionalHttpPayload]]:
        self._ensure_open()
        if not self._has_buffered_writes:
            self._closed = True
            return None

        if self._scope is None:
            raise ValueError("conditional transaction has no collection scope")

        return (
            self._scope,
            {
                "read_token": self.read_token,
                "operations": self.operations,
            },
        )

    def close(self) -> None:
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


def require_conditional_http_transaction(
    transaction: object,
) -> ConditionalHttpTransaction:
    if not isinstance(transaction, ConditionalHttpTransaction):
        raise ValueError("invalid conditional transaction for HTTP client")
    return transaction
