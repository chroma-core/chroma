from typing import TYPE_CHECKING, Callable, Optional, TypeVar, Union

from chromadb.errors import (
    BackoffError,
    ConditionalWriteConflictError,
    StaleReadError,
)
from chromadb.api.types import (
    ConditionalCommitResult,
    Document,
    Embedding,
    GetResult,
    ID,
    Image,
    Include,
    Metadata,
    OneOrMany,
    PyEmbedding,
    URI,
    Where,
    WhereDocument,
)

if TYPE_CHECKING:
    from chromadb.api.models.Collection import Collection


T = TypeVar("T")
_RUN_RETRYABLE_ERRORS = (
    ConditionalWriteConflictError,
    StaleReadError,
    BackoffError,
)


def _validate_max_retries(max_retries: int) -> None:
    if not isinstance(max_retries, int) or max_retries < 0:
        raise ValueError("max_retries must be a non-negative integer")


class ConditionalCollectionTransaction:
    def __init__(self, collection: "Collection") -> None:
        self._collection = collection
        self._transaction = collection._client._begin_conditional_transaction()
        # True iff within a `run` block and therefore explicit commit shall be disallowed.
        self._commit_blocked_by_run = False
        self._retryable_operation_exception: Optional[Exception] = None

    def _run_transaction_operation(self, operation: Callable[[], T]) -> T:
        try:
            return operation()
        except _RUN_RETRYABLE_ERRORS as exc:
            self._retryable_operation_exception = exc
            raise

    def _new_attempt(self, attempt: int) -> "ConditionalCollectionTransaction":
        if attempt == 0:
            return self
        return ConditionalCollectionTransaction(self._collection)

    def _commit_after_run(self) -> ConditionalCommitResult:
        return self._collection._client._conditional_commit(
            transaction=self._transaction
        )

    def run(
        self,
        callback: Callable[["ConditionalCollectionTransaction"], T],
        max_retries: int = 3,
    ) -> T:
        _validate_max_retries(max_retries)

        attempt = 0
        while True:
            txn = self._new_attempt(attempt)
            txn._commit_blocked_by_run = True
            try:
                result = callback(txn)
            except Exception as exc:
                if txn._retryable_operation_exception is exc and attempt < max_retries:
                    attempt += 1
                    continue
                raise
            finally:
                txn._commit_blocked_by_run = False

            try:
                txn._commit_after_run()
            except _RUN_RETRYABLE_ERRORS:
                if attempt < max_retries:
                    attempt += 1
                    continue
                raise

            return result

    def get(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        where: Optional[Where] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents"],
    ) -> GetResult:
        get_request = self._collection._validate_and_prepare_get_request(
            ids=ids,
            where=where,
            where_document=where_document,
            include=include,
        )

        get_results = self._run_transaction_operation(
            lambda: self._collection._client._conditional_get(
                transaction=self._transaction,
                collection_id=self._collection.id,
                ids=get_request["ids"],
                where=get_request["where"],
                where_document=get_request["where_document"],
                include=get_request["include"],
                limit=limit,
                offset=offset,
                tenant=self._collection.tenant,
                database=self._collection.database,
            )
        )
        return self._collection._transform_get_response(
            response=get_results, include=get_request["include"]
        )

    def add(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> None:
        add_request = self._collection._validate_and_prepare_add_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._run_transaction_operation(
            lambda: self._collection._client._conditional_add(
                transaction=self._transaction,
                collection_id=self._collection.id,
                ids=add_request["ids"],
                embeddings=add_request["embeddings"],
                metadatas=add_request["metadatas"],
                documents=add_request["documents"],
                uris=add_request["uris"],
                tenant=self._collection.tenant,
                database=self._collection.database,
            )
        )

    def update(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> None:
        update_request = self._collection._validate_and_prepare_update_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._run_transaction_operation(
            lambda: self._collection._client._conditional_update(
                transaction=self._transaction,
                collection_id=self._collection.id,
                ids=update_request["ids"],
                embeddings=update_request["embeddings"],
                metadatas=update_request["metadatas"],
                documents=update_request["documents"],
                uris=update_request["uris"],
                tenant=self._collection.tenant,
                database=self._collection.database,
            )
        )

    def upsert(
        self,
        ids: OneOrMany[ID],
        embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        metadatas: Optional[OneOrMany[Metadata]] = None,
        documents: Optional[OneOrMany[Document]] = None,
        images: Optional[OneOrMany[Image]] = None,
        uris: Optional[OneOrMany[URI]] = None,
    ) -> None:
        upsert_request = self._collection._validate_and_prepare_upsert_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._run_transaction_operation(
            lambda: self._collection._client._conditional_upsert(
                transaction=self._transaction,
                collection_id=self._collection.id,
                ids=upsert_request["ids"],
                embeddings=upsert_request["embeddings"],
                metadatas=upsert_request["metadatas"],
                documents=upsert_request["documents"],
                uris=upsert_request["uris"],
                tenant=self._collection.tenant,
                database=self._collection.database,
            )
        )

    def delete(self, ids: OneOrMany[ID]) -> None:
        delete_request = self._collection._validate_and_prepare_delete_request(
            ids, None, None
        )
        if delete_request["ids"] is None:
            raise ValueError("ids must be provided for transactional delete")

        self._run_transaction_operation(
            lambda: self._collection._client._conditional_delete(
                transaction=self._transaction,
                collection_id=self._collection.id,
                ids=delete_request["ids"],
                tenant=self._collection.tenant,
                database=self._collection.database,
            )
        )

    def commit(self) -> ConditionalCommitResult:
        if self._commit_blocked_by_run:
            raise ValueError("txn.commit() cannot be called inside run()")
        return self._commit_after_run()
