package chroma

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

/*
Unified Options API

This file defines the unified options pattern for Chroma collection operations.
Options are designed to work across multiple operations where semantically appropriate,
reducing API surface and improving discoverability.

# Option Compatibility Matrix

The following table shows which options work with which operations:

	Option              | Get | Query | Delete | Add | Update | Search
	--------------------|-----|-------|--------|-----|--------|-------
	WithIDs             |  ✓  |   ✓   |   ✓    |  ✓  |   ✓    |   ✓
	WithWhere           |  ✓  |   ✓   |   ✓    |     |        |
	WithWhereDocument   |  ✓  |   ✓   |   ✓    |     |        |
	WithInclude         |  ✓  |   ✓   |        |     |        |
	NewPage             |  ✓  |       |        |     |        |   ✓
	WithLimit           |  ✓  |       |        |     |        |   ✓
	WithOffset          |  ✓  |       |        |     |        |   ✓
	WithNResults        |     |   ✓   |        |     |        |
	WithQueryTexts      |     |   ✓   |        |     |        |
	WithQueryEmbeddings |     |   ✓   |        |     |        |
	WithTexts           |     |       |        |  ✓  |   ✓    |
	WithEmbeddings      |     |       |        |  ✓  |   ✓    |
	WithMetadatas       |     |       |        |  ✓  |   ✓    |
	WithIDGenerator     |     |       |        |  ✓  |        |
	WithFilter          |     |       |        |     |        |   ✓
	WithSelect          |     |       |        |     |        |   ✓
	WithRank            |     |       |        |     |        |   ✓
	WithGroupBy         |     |       |        |     |        |   ✓

# Basic Usage

	// Get documents by ID
	results, err := collection.Get(ctx, WithIDs("id1", "id2"))

	// Query with text and metadata filter
	results, err := collection.Query(ctx,
	    WithQueryTexts("machine learning"),
	    WithWhere(EqString("status", "published")),
	    WithNResults(10),
	)

	// Add documents
	err := collection.Add(ctx,
	    WithIDs("doc1", "doc2"),
	    WithTexts("First document", "Second document"),
	    WithMetadatas(meta1, meta2),
	)

	// Delete by filter
	err := collection.Delete(ctx,
	    WithWhere(EqString("status", "archived")),
	)

	// Search with ranking and pagination
	results, err := collection.Search(ctx,
	    NewSearchRequest(
	        WithKnnRank(KnnQueryText("query")),
	        WithFilter(EqString(K("category"), "tech")),
	        NewPage(Limit(20)),
	    ),
	)
*/

// GetOption configures a [Collection.Get] operation.
type GetOption interface {
	ApplyToGet(*CollectionGetOp) error
}

// QueryOption configures a [Collection.Query] operation.
type QueryOption interface {
	ApplyToQuery(*CollectionQueryOp) error
}

// DeleteOption configures a [Collection.Delete] operation.
type DeleteOption interface {
	ApplyToDelete(*CollectionDeleteOp) error
}

// AddOption configures a [Collection.Add] or [Collection.Upsert] operation.
type AddOption interface {
	ApplyToAdd(*CollectionAddOp) error
}

// UpdateOption configures a [Collection.Update] operation.
type UpdateOption interface {
	ApplyToUpdate(*CollectionUpdateOp) error
}

// SearchRequestOption configures a [SearchRequest] for [Collection.Search].
type SearchRequestOption interface {
	ApplyToSearchRequest(*SearchRequest) error
}

// GetOptionFunc wraps a function as a [GetOption].
type GetOptionFunc func(*CollectionGetOp) error

func (f GetOptionFunc) ApplyToGet(op *CollectionGetOp) error { return f(op) }

// QueryOptionFunc wraps a function as a [QueryOption].
type QueryOptionFunc func(*CollectionQueryOp) error

func (f QueryOptionFunc) ApplyToQuery(op *CollectionQueryOp) error { return f(op) }

// DeleteOptionFunc wraps a function as a [DeleteOption].
type DeleteOptionFunc func(*CollectionDeleteOp) error

func (f DeleteOptionFunc) ApplyToDelete(op *CollectionDeleteOp) error { return f(op) }

// AddOptionFunc wraps a function as an [AddOption].
type AddOptionFunc func(*CollectionAddOp) error

func (f AddOptionFunc) ApplyToAdd(op *CollectionAddOp) error { return f(op) }

// UpdateOptionFunc wraps a function as an [UpdateOption].
type UpdateOptionFunc func(*CollectionUpdateOp) error

func (f UpdateOptionFunc) ApplyToUpdate(op *CollectionUpdateOp) error { return f(op) }

// SearchRequestOptionFunc wraps a function as a [SearchRequestOption].
type SearchRequestOptionFunc func(*SearchRequest) error

func (f SearchRequestOptionFunc) ApplyToSearchRequest(req *SearchRequest) error { return f(req) }

// Option validation errors.
var (
	ErrInvalidLimit      = errors.New("limit must be greater than 0")
	ErrInvalidOffset     = errors.New("offset must be greater than or equal to 0")
	ErrInvalidNResults   = errors.New("nResults must be greater than 0")
	ErrNoQueryTexts      = errors.New("at least one query text is required")
	ErrNoQueryEmbeddings = errors.New("at least one query embedding is required")
	ErrNoTexts           = errors.New("at least one text is required")
	ErrNoMetadatas       = errors.New("at least one metadata is required")
	ErrNoEmbeddings      = errors.New("at least one embedding is required")
	ErrNoIDs             = errors.New("at least one id is required")
)

// checkDuplicateIDs validates that ids contains no duplicates and no overlap with existing.
func checkDuplicateIDs(ids []DocumentID, existing []DocumentID) error {
	seen := make(map[DocumentID]struct{}, len(existing)+len(ids))

	for _, id := range existing {
		seen[id] = struct{}{}
	}

	for _, id := range ids {
		if _, exists := seen[id]; exists {
			return fmt.Errorf("duplicate id: %s", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

// idsOption implements ID filtering for all operations.
type idsOption struct {
	ids []DocumentID
}

// WithIDs specifies document IDs for filtering or identification.
//
// This is a unified option that works with multiple operations:
//   - [Collection.Get]: Retrieve specific documents by ID
//   - [Collection.Query]: Limit semantic search to specific documents
//   - [Collection.Delete]: Delete specific documents by ID
//   - [Collection.Add]: Specify IDs for new documents
//   - [Collection.Update]: Identify documents to update
//   - [Collection.Search]: Filter search results to specific IDs
func WithIDs(ids ...DocumentID) *idsOption {
	return &idsOption{ids: ids}
}

func (o *idsOption) ApplyToGet(op *CollectionGetOp) error {
	if len(o.ids) == 0 {
		return ErrNoIDs
	}
	if err := checkDuplicateIDs(o.ids, op.Ids); err != nil {
		return err
	}
	op.Ids = append(op.Ids, o.ids...)
	return nil
}

func (o *idsOption) ApplyToQuery(op *CollectionQueryOp) error {
	if len(o.ids) == 0 {
		return ErrNoIDs
	}
	if err := checkDuplicateIDs(o.ids, op.Ids); err != nil {
		return err
	}
	op.Ids = append(op.Ids, o.ids...)
	return nil
}

func (o *idsOption) ApplyToDelete(op *CollectionDeleteOp) error {
	if len(o.ids) == 0 {
		return ErrNoIDs
	}
	if err := checkDuplicateIDs(o.ids, op.Ids); err != nil {
		return err
	}
	op.Ids = append(op.Ids, o.ids...)
	return nil
}

func (o *idsOption) ApplyToAdd(op *CollectionAddOp) error {
	if len(o.ids) == 0 {
		return ErrNoIDs
	}
	if err := checkDuplicateIDs(o.ids, op.Ids); err != nil {
		return err
	}
	op.Ids = append(op.Ids, o.ids...)
	return nil
}

func (o *idsOption) ApplyToUpdate(op *CollectionUpdateOp) error {
	if len(o.ids) == 0 {
		return ErrNoIDs
	}
	if err := checkDuplicateIDs(o.ids, op.Ids); err != nil {
		return err
	}
	op.Ids = append(op.Ids, o.ids...)
	return nil
}

func (o *idsOption) ApplyToSearchRequest(req *SearchRequest) error {
	if len(o.ids) == 0 {
		return ErrNoIDs
	}
	if req.Filter == nil {
		req.Filter = &SearchFilter{}
	}
	if err := checkDuplicateIDs(o.ids, req.Filter.IDs); err != nil {
		return err
	}
	req.Filter.IDs = append(req.Filter.IDs, o.ids...)
	return nil
}

// whereOption implements metadata filtering for Get, Query, and Delete operations.
type whereOption struct {
	where WhereFilter
}

// WithWhere filters documents by metadata field values.
//
// This is a unified option that works with:
//   - [Collection.Get]: Filter which documents to retrieve
//   - [Collection.Query]: Filter semantic search results
//   - [Collection.Delete]: Delete documents matching the filter
func WithWhere(where WhereFilter) *whereOption {
	return &whereOption{where: where}
}

func (o *whereOption) ApplyToGet(op *CollectionGetOp) error {
	if o.where != nil {
		if err := o.where.Validate(); err != nil {
			return err
		}
	}
	op.Where = o.where
	return nil
}

func (o *whereOption) ApplyToQuery(op *CollectionQueryOp) error {
	if o.where != nil {
		if err := o.where.Validate(); err != nil {
			return err
		}
	}
	op.Where = o.where
	return nil
}

func (o *whereOption) ApplyToDelete(op *CollectionDeleteOp) error {
	if o.where != nil {
		if err := o.where.Validate(); err != nil {
			return err
		}
	}
	op.Where = o.where
	return nil
}

// whereDocumentOption implements document content filtering for Get, Query, and Delete operations.
type whereDocumentOption struct {
	whereDocument WhereDocumentFilter
}

// WithWhereDocument filters documents by their text content.
//
// This is a unified option that works with:
//   - [Collection.Get]: Filter which documents to retrieve
//   - [Collection.Query]: Filter semantic search results
//   - [Collection.Delete]: Delete documents matching the content filter
func WithWhereDocument(whereDocument WhereDocumentFilter) *whereDocumentOption {
	return &whereDocumentOption{whereDocument: whereDocument}
}

func (o *whereDocumentOption) ApplyToGet(op *CollectionGetOp) error {
	if o.whereDocument != nil {
		if err := o.whereDocument.Validate(); err != nil {
			return err
		}
	}
	op.WhereDocument = o.whereDocument
	return nil
}

func (o *whereDocumentOption) ApplyToQuery(op *CollectionQueryOp) error {
	if o.whereDocument != nil {
		if err := o.whereDocument.Validate(); err != nil {
			return err
		}
	}
	op.WhereDocument = o.whereDocument
	return nil
}

func (o *whereDocumentOption) ApplyToDelete(op *CollectionDeleteOp) error {
	if o.whereDocument != nil {
		if err := o.whereDocument.Validate(); err != nil {
			return err
		}
	}
	op.WhereDocument = o.whereDocument
	return nil
}

// includeOption implements projection for Get and Query operations.
type includeOption struct {
	include []Include
}

// WithInclude specifies which fields to include in Get and Query results.
//
// This option works with:
//   - [Collection.Get]: Control which fields are returned
//   - [Collection.Query]: Control which fields are returned with search results
func WithInclude(include ...Include) *includeOption {
	return &includeOption{include: include}
}

func (o *includeOption) ApplyToGet(op *CollectionGetOp) error {
	op.Include = o.include
	return nil
}

func (o *includeOption) ApplyToQuery(op *CollectionQueryOp) error {
	op.Include = o.include
	return nil
}

// limitOption implements limit for Get and Search operations.
type limitOption struct {
	limit int
}

// WithLimit sets the maximum number of results to return.
//
// Works with both [Collection.Get] and [Collection.Search].
// For [Collection.Query], use [WithNResults] instead.
func WithLimit(limit int) *limitOption {
	return &limitOption{limit: limit}
}

func (o *limitOption) ApplyToGet(op *CollectionGetOp) error {
	if o.limit <= 0 {
		return ErrInvalidLimit
	}
	op.Limit = o.limit
	return nil
}

func (o *limitOption) ApplyToSearchRequest(req *SearchRequest) error {
	if o.limit <= 0 {
		return ErrInvalidLimit
	}
	if req.Limit == nil {
		req.Limit = &SearchPage{}
	}
	req.Limit.Limit = o.limit
	return nil
}

// offsetOption implements offset for Get and Search operations.
type offsetOption struct {
	offset int
}

// WithOffset sets the number of results to skip.
//
// Works with both [Collection.Get] and [Collection.Search].
func WithOffset(offset int) *offsetOption {
	return &offsetOption{offset: offset}
}

func (o *offsetOption) ApplyToGet(op *CollectionGetOp) error {
	if o.offset < 0 {
		return ErrInvalidOffset
	}
	op.Offset = o.offset
	return nil
}

func (o *offsetOption) ApplyToSearchRequest(req *SearchRequest) error {
	if o.offset < 0 {
		return ErrInvalidOffset
	}
	if req.Limit == nil {
		req.Limit = &SearchPage{}
	}
	req.Limit.Offset = o.offset
	return nil
}

// nResultsOption implements result limit for Query operations.
type nResultsOption struct {
	nResults int
}

// WithNResults sets the number of nearest neighbors to return from [Collection.Query].
//
// For [Collection.Get], use [WithLimit] instead.
// For [Collection.Search], use [WithLimit] instead.
func WithNResults(nResults int) *nResultsOption {
	return &nResultsOption{nResults: nResults}
}

func (o *nResultsOption) ApplyToQuery(op *CollectionQueryOp) error {
	if o.nResults <= 0 {
		return ErrInvalidNResults
	}
	op.NResults = o.nResults
	return nil
}

// queryTextsOption implements query text input for Query operations.
type queryTextsOption struct {
	texts []string
}

// WithQueryTexts sets the text queries for semantic search in [Collection.Query].
//
// The texts are embedded using the collection's embedding function and used
// to find semantically similar documents.
func WithQueryTexts(queryTexts ...string) *queryTextsOption {
	return &queryTextsOption{texts: queryTexts}
}

func (o *queryTextsOption) ApplyToQuery(op *CollectionQueryOp) error {
	if len(o.texts) == 0 {
		return ErrNoQueryTexts
	}
	op.QueryTexts = o.texts
	return nil
}

// queryEmbeddingsOption implements query embedding input for Query operations.
type queryEmbeddingsOption struct {
	embeddings []embeddings.Embedding
}

// WithQueryEmbeddings sets pre-computed embeddings for semantic search in [Collection.Query].
//
// Use this when you have pre-computed embeddings. Otherwise, use [WithQueryTexts]
// to have the collection embed the texts automatically.
func WithQueryEmbeddings(queryEmbeddings ...embeddings.Embedding) *queryEmbeddingsOption {
	return &queryEmbeddingsOption{embeddings: queryEmbeddings}
}

func (o *queryEmbeddingsOption) ApplyToQuery(op *CollectionQueryOp) error {
	if len(o.embeddings) == 0 {
		return ErrNoQueryEmbeddings
	}
	op.QueryEmbeddings = o.embeddings
	return nil
}

// textsOption implements document text input for Add and Update operations.
type textsOption struct {
	texts []string
}

// WithTexts sets the document text content for [Collection.Add], [Collection.Upsert],
// and [Collection.Update] operations.
//
// The texts are automatically embedded using the collection's embedding function
// unless embeddings are also provided via [WithEmbeddings].
func WithTexts(texts ...string) *textsOption {
	return &textsOption{texts: texts}
}

func (o *textsOption) ApplyToAdd(op *CollectionAddOp) error {
	if len(o.texts) == 0 {
		return ErrNoTexts
	}
	if op.Documents == nil {
		op.Documents = make([]Document, 0, len(o.texts))
	}
	for _, text := range o.texts {
		op.Documents = append(op.Documents, NewTextDocument(text))
	}
	return nil
}

func (o *textsOption) ApplyToUpdate(op *CollectionUpdateOp) error {
	if len(o.texts) == 0 {
		return ErrNoTexts
	}
	if op.Documents == nil {
		op.Documents = make([]Document, 0, len(o.texts))
	}
	for _, text := range o.texts {
		op.Documents = append(op.Documents, NewTextDocument(text))
	}
	return nil
}

// metadatasOption implements metadata input for Add and Update operations.
type metadatasOption struct {
	metadatas []DocumentMetadata
}

// WithMetadatas sets the document metadata for [Collection.Add], [Collection.Upsert],
// and [Collection.Update] operations.
func WithMetadatas(metadatas ...DocumentMetadata) *metadatasOption {
	return &metadatasOption{metadatas: metadatas}
}

func (o *metadatasOption) ApplyToAdd(op *CollectionAddOp) error {
	op.Metadatas = o.metadatas
	return nil
}

func (o *metadatasOption) ApplyToUpdate(op *CollectionUpdateOp) error {
	op.Metadatas = o.metadatas
	return nil
}

// embeddingsOption implements embedding input for Add and Update operations.
type embeddingsOption struct {
	embeddings []embeddings.Embedding
}

// WithEmbeddings sets pre-computed embeddings for [Collection.Add], [Collection.Upsert],
// and [Collection.Update] operations.
//
// Use this when you have pre-computed embeddings. If you provide both texts and embeddings,
// the embeddings will be used directly without re-embedding the texts.
func WithEmbeddings(embs ...embeddings.Embedding) *embeddingsOption {
	return &embeddingsOption{embeddings: embs}
}

func (o *embeddingsOption) ApplyToAdd(op *CollectionAddOp) error {
	if len(o.embeddings) == 0 {
		return ErrNoEmbeddings
	}
	embds := make([]any, 0, len(o.embeddings))
	for _, e := range o.embeddings {
		embds = append(embds, e)
	}
	op.Embeddings = embds
	return nil
}

func (o *embeddingsOption) ApplyToUpdate(op *CollectionUpdateOp) error {
	if len(o.embeddings) == 0 {
		return ErrNoEmbeddings
	}
	embds := make([]any, 0, len(o.embeddings))
	for _, e := range o.embeddings {
		embds = append(embds, e)
	}
	op.Embeddings = embds
	return nil
}

// idGeneratorOption implements automatic ID generation for Add operations.
type idGeneratorOption struct {
	generator IDGenerator
}

// WithIDGenerator sets an ID generator for [Collection.Add] operations.
//
// When set, IDs will be automatically generated for documents that don't have
// explicit IDs provided via [WithIDs].
func WithIDGenerator(generator IDGenerator) *idGeneratorOption {
	return &idGeneratorOption{generator: generator}
}

func (o *idGeneratorOption) ApplyToAdd(op *CollectionAddOp) error {
	op.IDGenerator = o.generator
	return nil
}

// filterOption implements metadata filtering for Search operations.
type filterOption struct {
	where WhereClause
}

// WithFilter adds a metadata filter to the search.
//
// Example:
//
//	WithFilter(And(EqString(K("status"), "published"), GtInt(K("views"), 100)))
func WithFilter(where WhereClause) *filterOption {
	return &filterOption{where: where}
}

func (o *filterOption) ApplyToSearchRequest(req *SearchRequest) error {
	if o.where != nil {
		if err := o.where.Validate(); err != nil {
			return err
		}
	}
	if req.Filter == nil {
		req.Filter = &SearchFilter{}
	}
	req.Filter.Where = o.where
	return nil
}

// selectOption implements field projection for Search operations.
type selectOption struct {
	keys []Key
}

// WithSelect specifies which fields to include in search results.
//
// Example:
//
//	WithSelect(KDocument, KScore, K("title"), K("author"))
func WithSelect(keys ...Key) *selectOption {
	return &selectOption{keys: keys}
}

func (o *selectOption) ApplyToSearchRequest(req *SearchRequest) error {
	if req.Select == nil {
		req.Select = &SearchSelect{}
	}
	req.Select.Keys = append(req.Select.Keys, o.keys...)
	return nil
}

// selectAllOption includes all standard fields in search results.
type selectAllOption struct{}

// WithSelectAll includes all standard fields in search results.
func WithSelectAll() *selectAllOption {
	return &selectAllOption{}
}

func (o *selectAllOption) ApplyToSearchRequest(req *SearchRequest) error {
	req.Select = &SearchSelect{
		Keys: []Key{KID, KDocument, KEmbedding, KMetadata, KScore},
	}
	return nil
}

// rankOption implements custom ranking for Search operations.
type rankOption struct {
	rank Rank
}

// WithRank sets a custom ranking expression on the search request.
func WithRank(rank Rank) *rankOption {
	return &rankOption{rank: rank}
}

func (o *rankOption) ApplyToSearchRequest(req *SearchRequest) error {
	req.Rank = o.rank
	return nil
}

// groupByOption implements result grouping for Search operations.
type groupByOption struct {
	groupBy *GroupBy
}

// WithGroupBy groups results by metadata keys using the specified aggregation.
func WithGroupBy(groupBy *GroupBy) *groupByOption {
	return &groupByOption{groupBy: groupBy}
}

func (o *groupByOption) ApplyToSearchRequest(req *SearchRequest) error {
	if o.groupBy == nil {
		return nil
	}
	if err := o.groupBy.Validate(); err != nil {
		return err
	}
	req.GroupBy = o.groupBy
	return nil
}
