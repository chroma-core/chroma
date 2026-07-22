from typing import TYPE_CHECKING, Optional, Union, List, cast, Dict, Any, Tuple

from chromadb.api.models.CollectionCommon import CollectionCommon
from chromadb.api.types import (
    URI,
    CollectionMetadata,
    Embedding,
    PyEmbedding,
    Include,
    IndexingStatus,
    Metadata,
    Document,
    Image,
    Where,
    IDs,
    GetResult,
    QueryResult,
    ID,
    OneOrMany,
    ReadLevel,
    WhereDocument,
    SearchResult,
    DeleteResult,
    maybe_cast_one_to_many,
)
from chromadb.api.collection_configuration import UpdateCollectionConfiguration
from chromadb.execution.expression.plan import Search

import logging

from chromadb.api.functions import Function

if TYPE_CHECKING:
    from chromadb.api.models.AttachedFunction import AttachedFunction

from chromadb.api.models.ConditionalCollectionTransaction import (
    ConditionalCollectionTransaction,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from chromadb.api import ServerAPI  # noqa: F401


class Collection(CollectionCommon["ServerAPI"]):
    def conditional(self) -> "ConditionalCollectionTransaction":
        """Start a collection-scoped conditional transaction.

        Conditional transactions read from a stable snapshot, buffer writes
        locally, and commit them with optimistic conflict detection.

        Current limitations: transactions cannot span collections, nested
        transaction guarantees are not provided, ``txn.query(...)`` and
        predicate deletes are not supported, reading an ID after buffering a
        write for that ID is an explicit transaction error, only one write per
        ID can be buffered, and filter reads protect only returned IDs.
        """
        return ConditionalCollectionTransaction(self)

    def count(self, read_level: ReadLevel = ReadLevel.INDEX_AND_WAL) -> int:
        """Return the number of records in the collection.

        Args:
            read_level: Controls whether to read from the write-ahead log (WAL):
                - ReadLevel.INDEX_AND_WAL: Read from both the compacted index and WAL (default).
                  All committed writes will be visible.
                - ReadLevel.INDEX_ONLY: Read only from the compacted index, skipping the WAL.
                  Faster, but recent writes that haven't been compacted may not be visible.
                - ReadLevel.INDEX_AND_BOUNDED_WAL: Read from the index and up to a
                  server-configured number of WAL entries for bounded query latency.
        """
        return self._client._count(
            collection_id=self.id,
            tenant=self.tenant,
            database=self.database,
            read_level=read_level,
        )

    def get_indexing_status(self) -> IndexingStatus:
        """Get the indexing status of this collection.

        Returns:
            IndexingStatus: An object containing:
                - num_indexed_ops: Number of user operations that have been indexed
                - num_unindexed_ops: Number of user operations pending indexing
                - total_ops: Total number of user operations in collection
                - op_indexing_progress: Proportion of user operations that have been indexed as a float between 0 and 1
        """
        return self._client._get_indexing_status(
            collection_id=self.id,
            tenant=self.tenant,
            database=self.database,
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
        """Add records to the collection.

        Args:
            ids: Record IDs to add.
            embeddings: Embeddings to add. If None, embeddings are computed.
            metadatas: Optional metadata for each record.
            documents: Optional documents for each record.
            images: Optional images for each record.
            uris: Optional URIs for loading images.

        Raises:
            ValueError: If embeddings and documents are both missing.
            ValueError: If embeddings and documents are both provided.
            ValueError: If lengths of provided fields do not match.
            ValueError: If an ID already exists.
        """

        add_request = self._validate_and_prepare_add_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._client._add(
            collection_id=self.id,
            ids=add_request["ids"],
            embeddings=add_request["embeddings"],
            metadatas=add_request["metadatas"],
            documents=add_request["documents"],
            uris=add_request["uris"],
            tenant=self.tenant,
            database=self.database,
        )

    def get(
        self,
        ids: Optional[OneOrMany[ID]] = None,
        where: Optional[Where] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = ["metadatas", "documents"],
    ) -> GetResult:
        """Retrieve records from the collection.

        If no filters are provided, returns records up to ``limit`` starting at
        ``offset``.

        Args:
            ids: If provided, only return records with these IDs.
            where: A Where filter used to filter based on metadata values.
            limit: Maximum number of results to return.
            offset: Number of results to skip before returning.
            where_document: A WhereDocument filter used to filter based on K.DOCUMENT.
            include: Fields to include in results. Can contain "embeddings", "metadatas", "documents", "uris". Defaults to "metadatas" and "documents".

        Returns:
            GetResult: Retrieved records and requested fields as a GetResult object.
        """
        get_request = self._validate_and_prepare_get_request(
            ids=ids,
            where=where,
            where_document=where_document,
            include=include,
        )

        get_results = self._client._get(
            collection_id=self.id,
            ids=get_request["ids"],
            where=get_request["where"],
            where_document=get_request["where_document"],
            include=get_request["include"],
            limit=limit,
            offset=offset,
            tenant=self.tenant,
            database=self.database,
        )
        return self._transform_get_response(
            response=get_results, include=get_request["include"]
        )

    def peek(self, limit: int = 10) -> GetResult:
        """Return the first ``limit`` records from the collection.

        Args:
            limit: Maximum number of records to return.

        Returns:
            GetResult: Retrieved records and requested fields.
        """
        return self._transform_peek_response(
            self._client._peek(
                collection_id=self.id,
                n=limit,
                tenant=self.tenant,
                database=self.database,
            )
        )

    def query(
        self,
        query_embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        query_texts: Optional[OneOrMany[Document]] = None,
        query_images: Optional[OneOrMany[Image]] = None,
        query_uris: Optional[OneOrMany[URI]] = None,
        ids: Optional[OneOrMany[ID]] = None,
        n_results: int = 10,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        include: Include = [
            "metadatas",
            "documents",
            "distances",
        ],
    ) -> QueryResult:
        """Query for the K nearest neighbor records in the collection.

        This is a batch query API. Multiple queries can be performed at once
        by providing multiple embeddings, texts, or images.

        >>> query_1 = [0.1, 0.2, 0.3]
        >>> query_2 = [0.4, 0.5, 0.6]
        >>> results = collection.query(
        >>>     query_embeddings=[query_1, query_2],
        >>>     n_results=10,
        >>> )

        If query_texts, query_images, or query_uris are provided, the collection's
        embedding function will be used to create embeddings before querying
        the API.

        The `ids`, `where`, `where_document`, and `include` parameters are applied
        to all queries.

        Args:
            query_embeddings: Raw embeddings to query for.
            query_texts: Documents to embed and query against.
            query_images: Images to embed and query against.
            query_uris: URIs to be loaded and embedded.
            ids: Optional subset of IDs to search within.
            n_results: Number of neighbors to return per query.
            where: Metadata filter.
            where_document: Document content filter.
            include: Fields to include in results. Can contain "embeddings", "metadatas", "documents", "uris", "distances". Defaults to "metadatas", "documents", "distances".

        Returns:
            QueryResult: Nearest neighbor results.

        Raises:
            ValueError: If no query input is provided.
            ValueError: If multiple query input types are provided.
        """

        query_request = self._validate_and_prepare_query_request(
            query_embeddings=query_embeddings,
            query_texts=query_texts,
            query_images=query_images,
            query_uris=query_uris,
            ids=ids,
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=include,
        )

        query_results = self._client._query(
            collection_id=self.id,
            ids=query_request["ids"],
            query_embeddings=query_request["embeddings"],
            n_results=query_request["n_results"],
            where=query_request["where"],
            where_document=query_request["where_document"],
            include=query_request["include"],
            tenant=self.tenant,
            database=self.database,
        )

        return self._transform_query_response(
            response=query_results, include=query_request["include"]
        )

    def hybrid_search(
        self,
        query_texts: OneOrMany[Document],
        query_embeddings: Optional[
            Union[
                OneOrMany[Embedding],
                OneOrMany[PyEmbedding],
            ]
        ] = None,
        n_results: int = 10,
        n_candidates: int = 100,
        alpha: float = 0.5,
        fusion_method: str = "rrf",
        rrf_k: int = 60,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        ids: Optional[OneOrMany[ID]] = None,
        include: Include = [
            "metadatas",
            "documents",
            "distances",
        ],
    ) -> QueryResult:
        """Perform hybrid search combining BM25 keyword search with vector similarity.

        Uses Reciprocal Rank Fusion (RRF) or linear combination to merge results
        from both retrieval strategies for improved relevance.

        This is a batch query API. Multiple queries can be performed at once
        by providing multiple query texts.

        >>> collection.hybrid_search(
        >>>     query_texts=["what is machine learning"],
        >>>     n_results=10,
        >>> )

        Args:
            query_texts: Text queries to search for. These are used both for
                        embedding (vector search) and BM25 keyword scoring.
            query_embeddings: Pre-computed embeddings. If not provided,
                            embeddings are computed from query_texts.
            n_results: Number of final fused results to return per query.
            n_candidates: Number of candidates to retrieve from vector search
                        (larger values improve keyword recall).
            alpha: Weight for vector scores in linear fusion (0=keyword only, 1=vector only).
                  Only used when fusion_method="linear".
            fusion_method: Fusion strategy - "rrf" (default) or "linear".
            rrf_k: RRF smoothing constant (default 60).
            where: Metadata filter applied to vector search and keyword search.
            where_document: Document content filter.
            ids: Optional subset of IDs to search within.
            include: Fields to include in results. Defaults to
                    ["metadatas", "documents", "distances"].

        Returns:
            QueryResult: Fused nearest neighbor results with BM25 keyword boosting.

        Raises:
            ValueError: If no query text is provided.
        """
        from chromadb.utils.hybrid_search import (
            BM25Scorer,
            reciprocal_rank_fusion,
            combine_ranked_lists,
            tokenize,
        )

        # Validate fusion_method
        if fusion_method not in ("rrf", "linear"):
            raise ValueError(
                f"fusion_method must be 'rrf' or 'linear', got '{fusion_method}'"
            )

        # Validate and prepare the query request (includes embedding computation)
        query_request = self._validate_and_prepare_query_request(
            query_embeddings=query_embeddings,
            query_texts=query_texts,
            query_images=None,
            query_uris=None,
            ids=ids,
            n_results=max(n_results, n_candidates),
            where=where,
            where_document=where_document,
            include=["metadatas", "documents", "distances"],
        )

        # Get raw query texts (un-embedded) for BM25 keyword scoring
        query_texts_list = self._normalize_hybrid_query_texts(query_texts)

        query_embeddings_list = query_request["embeddings"]
        filter_ids = query_request["ids"]
        filter_where = query_request["where"]
        filter_where_document = query_request["where_document"]

        all_ids_out: List[IDs] = []
        all_documents_out: Optional[List[List[Document]]] = (
            [] if "documents" in include else None
        )
        all_metadatas_out: Optional[List[List[Metadata]]] = (
            [] if "metadatas" in include else None
        )
        all_distances_out: Optional[List[List[float]]] = (
            [] if "distances" in include else None
        )
        all_embeddings_out: Optional[List[PyEmbeddings]] = (
            [] if "embeddings" in include else None
        )

        for i, query_text in enumerate(query_texts_list):
            query_emb = query_embeddings_list[i]

            # Step 1: Vector search to get a pool of candidates
            vector_results = self._client._query(
                collection_id=self.id,
                ids=filter_ids,
                query_embeddings=[query_emb],
                n_results=n_candidates,
                where=filter_where,
                where_document=filter_where_document,
                include=["metadatas", "documents", "distances"],
                tenant=self.tenant,
                database=self.database,
            )

            vec_ids = vector_results["ids"][0] if vector_results["ids"] else []
            vec_docs = (
                vector_results["documents"][0]
                if vector_results["documents"]
                else []
            )
            vec_distances = (
                vector_results["distances"][0]
                if vector_results["distances"]
                else []
            )

            # Build vector results as (id, distance) for fusion
            # Convert distance to similarity-like score (lower distance = better)
            vector_ranked: List[Tuple[str, float]] = []
            for j, vid in enumerate(vec_ids):
                dist = vec_distances[j] if j < len(vec_distances) else float("inf")
                # Use negative distance: higher (less negative) = better match
                vector_ranked.append((vid, -dist))

            # Step 2: Keyword search on documents using $contains
            # Split query into terms and search for documents containing any term
            query_terms = tokenize(query_text)
            keyword_ids: set = set()
            keyword_docs_map: Dict[str, str] = {}

            if query_terms:
                # Search for documents containing the query text
                for term in query_terms[:5]:  # Limit to first 5 terms for efficiency
                    try:
                        keyword_results = self._client._get(
                            collection_id=self.id,
                            ids=filter_ids if filter_ids else None,
                            where=filter_where,
                            where_document={"$contains": term},
                            limit=n_candidates,
                            include=["documents"],
                            tenant=self.tenant,
                            database=self.database,
                        )
                        if keyword_results["ids"]:
                            for kid, kdoc in zip(
                                keyword_results["ids"],
                                keyword_results.get("documents") or [],
                            ):
                                keyword_ids.add(kid)
                                if kdoc:
                                    keyword_docs_map[kid] = kdoc
                    except Exception:
                        # If $contains is not supported, fall back to scoring
                        # vector candidates only
                        pass

            # Step 3: Combine candidate sets from both strategies
            all_candidate_ids = set(vec_ids)
            all_candidate_ids.update(keyword_ids)

            # Build document map for BM25 scoring
            docs_for_bm25: Dict[str, str] = {}
            # From vector results
            for j, vid in enumerate(vec_ids):
                doc = vec_docs[j] if j < len(vec_docs) else None
                if doc:
                    docs_for_bm25[vid] = doc
            # From keyword results
            docs_for_bm25.update(keyword_docs_map)

            # Step 4: BM25 scoring
            bm25_ranked: List[Tuple[str, float]] = []
            if docs_for_bm25:
                doc_ids_list = list(docs_for_bm25.keys())
                doc_texts_list = [docs_for_bm25[did] for did in doc_ids_list]

                bm25 = BM25Scorer()
                bm25.fit(doc_texts_list)

                scored = bm25.score(query_text)
                # Map back to original IDs
                bm25_ranked = [
                    (doc_ids_list[idx], score)
                    for idx, score in scored
                    if idx < len(doc_ids_list)
                ]

            # Step 5: Fuse using RRF or linear combination
            if fusion_method == "rrf":
                fused = reciprocal_rank_fusion(
                    [vector_ranked, bm25_ranked],
                    k=rrf_k,
                    weights=[1.0 - alpha, alpha] if alpha != 0.5 else None,
                )
            else:
                fused = combine_ranked_lists(
                    bm25_ranked, vector_ranked, alpha=alpha
                )

            # Step 6: Take top n_results
            fused = fused[:n_results]
            fused_ids = [fid for fid, _ in fused]
            fused_scores = [fscore for _, fscore in fused]

            # Step 7: Hydrate results with metadata/documents
            hydrated = self._client._get(
                collection_id=self.id,
                ids=fused_ids,
                include=["metadatas", "documents"],
                tenant=self.tenant,
                database=self.database,
            )

            # Reorder hydrated results to match fused order
            id_to_idx = {hid: idx for idx, hid in enumerate(hydrated["ids"])}
            reordered_ids = fused_ids
            reordered_docs: Optional[List[Document]] = None
            reordered_metas: Optional[List[Metadata]] = None

            if hydrated["documents"] is not None:
                reordered_docs = [
                    hydrated["documents"][id_to_idx[fid]]
                    if fid in id_to_idx
                    else None
                    for fid in fused_ids
                ]
            if hydrated["metadatas"] is not None:
                reordered_metas = [
                    hydrated["metadatas"][id_to_idx[fid]]
                    if fid in id_to_idx
                    else None
                    for fid in fused_ids
                ]

            all_ids_out.append(reordered_ids)
            if all_documents_out is not None:
                all_documents_out.append(reordered_docs or [])
            if all_metadatas_out is not None:
                all_metadatas_out.append(reordered_metas or [])
            if all_distances_out is not None:
                all_distances_out.append(fused_scores)
            if all_embeddings_out is not None:
                all_embeddings_out.append([])

        result: QueryResult = {
            "ids": all_ids_out,
            "embeddings": all_embeddings_out,
            "documents": all_documents_out,
            "uris": None,
            "data": None,
            "metadatas": all_metadatas_out,
            "distances": all_distances_out,
            "included": [inc for inc in include if inc != "embeddings"],
        }

        return result

    def _normalize_hybrid_query_texts(
        self,
        query_texts: OneOrMany[Document],
    ) -> List[str]:
        """Normalize query texts for hybrid search into a list of strings.

        Returns:
            List of query text strings.
        """
        texts = maybe_cast_one_to_many(query_texts)
        if texts is None or len(texts) == 0:
            raise ValueError("query_texts must be a non-empty list of strings")
        return texts

    def modify(
        self,
        name: Optional[str] = None,
        metadata: Optional[CollectionMetadata] = None,
        configuration: Optional[UpdateCollectionConfiguration] = None,
    ) -> None:
        """Update collection name, metadata, or configuration.

        Args:
            name: New collection name.
            metadata: New metadata for the collection.
            configuration: New configuration for the collection.
        """

        self._validate_modify_request(metadata)

        # Note there is a race condition here where the metadata can be updated
        # but another thread sees the cached local metadata.
        # TODO: fixme
        self._client._modify(
            id=self.id,
            new_name=name,
            new_metadata=metadata,
            new_configuration=configuration,
            tenant=self.tenant,
            database=self.database,
        )

        self._update_model_after_modify_success(name, metadata, configuration)

    def fork(
        self,
        new_name: str,
    ) -> "Collection":
        """Fork the current collection under a new name. The returning collection should contain identical data to the current collection.
        This only works for Hosted Chroma for now.

        Args:
            new_name: The name of the new collection.

        Returns:
            Collection: A new collection with the specified name and containing identical data to the current collection.
        """
        model = self._client._fork(
            collection_id=self.id,
            new_name=new_name,
            tenant=self.tenant,
            database=self.database,
        )
        return Collection(
            client=self._client,
            model=model,
            embedding_function=self._embedding_function,
            data_loader=self._data_loader,
        )

    def fork_count(self) -> int:
        """Get the number of forks that exist for this collection.
        This only works for Hosted Chroma for now.

        Returns:
            int: The number of forks for this collection.
        """
        return self._client._fork_count(
            collection_id=self.id,
            tenant=self.tenant,
            database=self.database,
        )

    def search(
        self,
        searches: OneOrMany[Search],
        read_level: ReadLevel = ReadLevel.INDEX_AND_WAL,
    ) -> SearchResult:
        """Perform hybrid search on the collection.
        This is an experimental API that only works for distributed and hosted Chroma for now.

        Args:
            searches: A single Search object or a list of Search objects, each containing:
                - where: Where expression for filtering
                - rank: Ranking expression for hybrid search (defaults to Val(0.0))
                - limit: Limit configuration for pagination (defaults to no limit)
                - select: Select configuration for keys to return (defaults to empty)
            read_level: Controls whether to read from the write-ahead log (WAL):
                - ReadLevel.INDEX_AND_WAL: Read from both the compacted index and WAL (default).
                  All committed writes will be visible.
                - ReadLevel.INDEX_ONLY: Read only from the compacted index, skipping the WAL.
                  Faster, but recent writes that haven't been compacted may not be visible.
                - ReadLevel.INDEX_AND_BOUNDED_WAL: Read from the index and up to a
                  server-configured number of WAL entries for bounded query latency.

        Returns:
            SearchResult: Column-major format response with:
                - ids: List of result IDs for each search payload
                - documents: Optional documents for each payload
                - embeddings: Optional embeddings for each payload
                - metadatas: Optional metadata for each payload
                - scores: Optional scores for each payload
                - select: List of selected keys for each payload

        Raises:
            NotImplementedError: For local/segment API implementations

        Examples:
            # Using builder pattern with Key constants
            from chromadb.execution.expression import (
                Search, Key, K, Knn, Val
            )

            # Note: K is an alias for Key, so K.DOCUMENT == Key.DOCUMENT
            search = (Search()
                .where((K("category") == "science") & (K("score") > 0.5))
                .rank(Knn(query=[0.1, 0.2, 0.3]) * 0.8 + Val(0.5) * 0.2)
                .limit(10, offset=0)
                .select(K.DOCUMENT, K.SCORE, "title"))

            # Direct construction
            from chromadb.execution.expression import (
                Search, Eq, And, Gt, Knn, Limit, Select, Key
            )

            search = Search(
                where=And([Eq("category", "science"), Gt("score", 0.5)]),
                rank=Knn(query=[0.1, 0.2, 0.3]),
                limit=Limit(offset=0, limit=10),
                select=Select(keys={Key.DOCUMENT, Key.SCORE, "title"})
            )

            # Single search
            result = collection.search(search)

            # Multiple searches at once
            searches = [
                Search().where(K("type") == "article").rank(Knn(query=[0.1, 0.2])),
                Search().where(K("type") == "paper").rank(Knn(query=[0.3, 0.4]))
            ]
            results = collection.search(searches)

            # Skip WAL for faster queries (may miss recent uncommitted writes)
            from chromadb.api.types import ReadLevel
            result = collection.search(search, read_level=ReadLevel.INDEX_ONLY)
        """
        # Convert single search to list for consistent handling
        searches_list = maybe_cast_one_to_many(searches)
        if searches_list is None:
            searches_list = []

        # Embed any string queries in Knn objects
        embedded_searches = [
            self._embed_search_string_queries(search) for search in searches_list
        ]

        return self._client._search(
            collection_id=self.id,
            searches=cast(List[Search], embedded_searches),
            tenant=self.tenant,
            database=self.database,
            read_level=read_level,
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
        """Update existing records by ID.

        Records are provided in columnar format. If provided, the `embeddings`, `metadatas`, `documents`, and `uris` lists must be the same length.
        Entries in each list correspond to the same record.

        >>> ids = ["id1", "id2", "id3"]
        >>> embeddings = [[0.1, 0.2, 0.3], [0.4, 0.5, 0.6], [0.7, 0.8, 0.9]]
        >>> metadatas = [{"key": "value"}, {"key": "value"}, {"key": "value"}]
        >>> documents = ["document1", "document2", "document3"]
        >>> uris = ["uri1", "uri2", "uri3"]
        >>> collection.update(ids, embeddings, metadatas, documents, uris)

        If `embeddings` are not provided, the embeddings will be computed based on `documents` using the collection's embedding function.

        Args:
            ids: Record IDs to update.
            embeddings: Updated embeddings. If None, embeddings are computed.
            metadatas: Updated metadata.
            documents: Updated documents.
            images: Updated images.
            uris: Updated URIs for loading images.
        """
        update_request = self._validate_and_prepare_update_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._client._update(
            collection_id=self.id,
            ids=update_request["ids"],
            embeddings=update_request["embeddings"],
            metadatas=update_request["metadatas"],
            documents=update_request["documents"],
            uris=update_request["uris"],
            tenant=self.tenant,
            database=self.database,
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
        """Create or update records by ID.

        Args:
            ids: Record IDs to upsert.
            embeddings: Embeddings to add or update. If None, embeddings are computed.
            metadatas: Metadata to add or update.
            documents: Documents to add or update.
            images: Images to add or update.
            uris: URIs for loading images.
        """
        upsert_request = self._validate_and_prepare_upsert_request(
            ids=ids,
            embeddings=embeddings,
            metadatas=metadatas,
            documents=documents,
            images=images,
            uris=uris,
        )

        self._client._upsert(
            collection_id=self.id,
            ids=upsert_request["ids"],
            embeddings=upsert_request["embeddings"],
            metadatas=upsert_request["metadatas"],
            documents=upsert_request["documents"],
            uris=upsert_request["uris"],
            tenant=self.tenant,
            database=self.database,
        )

    def delete(
        self,
        ids: Optional[IDs] = None,
        where: Optional[Where] = None,
        where_document: Optional[WhereDocument] = None,
        limit: Optional[int] = None,
    ) -> DeleteResult:
        """Delete records by ID or filters.

        All documents that match the `ids` or `where` and `where_document` filters will be deleted.

        Args:
            ids: Record IDs to delete.
            where: Metadata filter.
            where_document: Document content filter.
            limit: Maximum number of records to delete. Can only be used with where or where_document filters.

        Returns:
            DeleteResult: A dict containing the number of records deleted.

        Raises:
            ValueError: If no IDs or filters are provided.
            ValueError: If limit is specified without a where or where_document clause.
        """
        delete_request = self._validate_and_prepare_delete_request(
            ids, where, where_document, limit=limit
        )

        return self._client._delete(
            collection_id=self.id,
            ids=delete_request["ids"],
            where=delete_request["where"],
            where_document=delete_request["where_document"],
            limit=delete_request["limit"],
            tenant=self.tenant,
            database=self.database,
        )

    def attach_function(
        self,
        function: Function,
        name: str,
        output_collection: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple["AttachedFunction", bool]:
        """Attach a function to this collection.

        Args:
            function: A Function enum value (e.g., STATISTICS_FUNCTION, RECORD_COUNTER_FUNCTION)
            name: Unique name for this attached function
            output_collection: Name of the collection where function output will be stored
            params: Optional dictionary with function-specific parameters

        Returns:
            Tuple of (AttachedFunction, created) where created is True if newly created,
            False if already existed (idempotent request)

        Example:
            >>> from chromadb.api.functions import STATISTICS_FUNCTION
            >>> attached_fn = collection.attach_function(
            ...     function=STATISTICS_FUNCTION,
            ...     name="mycoll_stats_fn",
            ...     output_collection="mycoll_stats",
            ... )
            >>> if created:
            ...     print("New function attached")
            ... else:
            ...     print("Function already existed")
        """
        function_id = function.value if isinstance(function, Function) else function
        return self._client.attach_function(
            function_id=function_id,
            name=name,
            input_collection_id=self.id,
            output_collection=output_collection,
            params=params,
            tenant=self.tenant,
            database=self.database,
        )

    def get_attached_function(self, name: str) -> "AttachedFunction":
        """Get an attached function by name for this collection.

        Args:
            name: Name of the attached function

        Returns:
            AttachedFunction: The attached function object

        Raises:
            NotFoundError: If the attached function doesn't exist
        """
        return self._client.get_attached_function(
            name=name,
            input_collection_id=self.id,
            tenant=self.tenant,
            database=self.database,
        )

    def detach_function(
        self,
        name: str,
        delete_output_collection: bool = False,
    ) -> bool:
        """Detach a function from this collection.

        Args:
            name: The name of the attached function
            delete_output_collection: Whether to also delete the output collection. Defaults to False.

        Returns:
            bool: True if successful

        Example:
            >>> success = collection.detach_function("my_function", delete_output_collection=True)
        """
        return self._client.detach_function(
            name=name,
            input_collection_id=self.id,
            delete_output=delete_output_collection,
            tenant=self.tenant,
            database=self.database,
        )
