import inspect
from typing import TYPE_CHECKING, Awaitable, Callable, Optional, TypeVar, Union, cast

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
from chromadb.api.models.ConditionalCollectionTransaction import (
    _RUN_RETRYABLE_ERRORS,
    _validate_max_retries,
)

if TYPE_CHECKING:
    from chromadb.api.models.AsyncCollection import AsyncCollection


T = TypeVar("T")


class AsyncConditionalCollectionTransaction:
    def __init__(self, collection: "AsyncCollection", transaction: object) -> None:
        self._collection = collection
        self._transaction = transaction
        self._commit_blocked_by_run = False
        self._retryable_operation_exception: Optional[Exception] = None

    async def _run_transaction_operation(self, operation: Awaitable[T]) -> T:
        try:
            return await operation
        except _RUN_RETRYABLE_ERRORS as exc:
            self._retryable_operation_exception = exc
            raise

    async def _new_attempt(
        self, attempt: int
    ) -> "AsyncConditionalCollectionTransaction":
        if attempt == 0:
            return self
        return await self._collection.conditional()

    async def _commit_after_run(self) -> ConditionalCommitResult:
        return await self._collection._client._conditional_commit(
            transaction=self._transaction
        )

    async def run(
        self,
        callback: Callable[
            ["AsyncConditionalCollectionTransaction"], Union[T, Awaitable[T]]
        ],
        max_retries: int = 3,
    ) -> T:
        _validate_max_retries(max_retries)

        attempt = 0
        while True:
            txn = await self._new_attempt(attempt)
            txn._commit_blocked_by_run = True
            try:
                result = callback(txn)
                if inspect.isawaitable(result):
                    value = await cast(Awaitable[T], result)
                else:
                    value = cast(T, result)
            except Exception as exc:
                if txn._retryable_operation_exception is exc and attempt < max_retries:
                    attempt += 1
                    continue
                raise
            finally:
                txn._commit_blocked_by_run = False

            try:
                await txn._commit_after_run()
            except _RUN_RETRYABLE_ERRORS:
                if attempt < max_retries:
                    attempt += 1
                    continue
                raise

            return value

    async def get(
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

        get_results = await self._run_transaction_operation(
            self._collection._client._conditional_get(
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

    async def add(
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

        await self._run_transaction_operation(
            self._collection._client._conditional_add(
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

    async def update(
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

        await self._run_transaction_operation(
            self._collection._client._conditional_update(
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

    async def upsert(
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

        await self._run_transaction_operation(
            self._collection._client._conditional_upsert(
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

    async def delete(self, ids: OneOrMany[ID]) -> None:
        delete_request = self._collection._validate_and_prepare_delete_request(
            ids, None, None
        )
        if delete_request["ids"] is None:
            raise ValueError("ids must be provided for transactional delete")

        await self._run_transaction_operation(
            self._collection._client._conditional_delete(
                transaction=self._transaction,
                collection_id=self._collection.id,
                ids=delete_request["ids"],
                tenant=self._collection.tenant,
                database=self._collection.database,
            )
        )

    async def commit(self) -> ConditionalCommitResult:
        if self._commit_blocked_by_run:
            raise ValueError("txn.commit() cannot be called inside run()")
        return await self._commit_after_run()
