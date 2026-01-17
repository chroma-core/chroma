package chroma

import (
	"bytes"
	"encoding/json"

	"github.com/pkg/errors"
)

// SearchQuery holds one or more search requests to execute as a batch.
type SearchQuery struct {
	Searches []SearchRequest `json:"searches"`
}

// SearchResult represents the result of a search operation.
type SearchResult interface{}

// Key identifies a metadata field for filtering or projection.
// Key is a type alias for string, so raw strings work directly for backward compatibility.
// Use [K] to clearly mark field names in filter expressions:
//
//	EqString(K("status"), "active")  // K() marks the field clearly
//	EqString("status", "active")     // Raw string also works
type Key = string

// K creates a Key for a metadata field name.
// Use this in filter functions to clearly mark field names:
//
//	EqString(K("status"), "active")
//	GtInt(K("price"), 100)
//	WithSelect(K("title"), K("author"), KDocument)
func K(key string) Key {
	return key
}

// Standard keys for document fields.
const (
	KDocument  Key = "#document"  // The document text
	KEmbedding Key = "#embedding" // The vector embedding
	KScore     Key = "#score"     // The ranking score
	KMetadata  Key = "#metadata"  // All metadata fields
	KID        Key = "#id"        // The document ID
)

// SearchFilter specifies which documents to include in search results.
// The filter serializes directly as a where clause (not wrapped in a "where" object).
// Note: For document content filtering, use WithFilter with document-related Where clauses.
type SearchFilter struct {
	IDs   []DocumentID `json:"-"` // Converted to #id $in clause
	Where WhereClause  `json:"-"` // Serialized directly as the filter
}

func (f *SearchFilter) MarshalJSON() ([]byte, error) {
	var clauses []WhereClause

	// Convert IDs to #id $in clause
	if len(f.IDs) > 0 {
		clauses = append(clauses, IDIn(f.IDs...))
	}

	// Add where clause
	if f.Where != nil {
		clauses = append(clauses, f.Where)
	}

	if len(clauses) == 0 {
		return []byte("{}"), nil
	}

	// If single clause, serialize directly; otherwise combine with $and
	var result WhereClause
	if len(clauses) == 1 {
		result = clauses[0]
	} else {
		result = And(clauses...)
	}

	// Validate the composed filter before serializing
	if err := result.Validate(); err != nil {
		return nil, errors.Wrap(err, "invalid search filter")
	}

	return json.Marshal(result)
}

// SearchSelect specifies which fields to include in search results.
type SearchSelect struct {
	Keys []Key `json:"keys,omitempty"`
}

// SearchRequest represents a single search operation with filter, ranking, pagination, and projection.
type SearchRequest struct {
	Filter  *SearchFilter `json:"filter,omitempty"`
	Limit   *SearchPage   `json:"limit,omitempty"`
	Rank    Rank          `json:"rank,omitempty"`
	Select  *SearchSelect `json:"select,omitempty"`
	GroupBy *GroupBy      `json:"group_by,omitempty"`
}

func (r *SearchRequest) MarshalJSON() ([]byte, error) {
	result := make(map[string]interface{})

	if r.Filter != nil {
		filterData, err := r.Filter.MarshalJSON()
		if err != nil {
			return nil, err
		}
		if filterData != nil {
			var filterMap map[string]interface{}
			if err := json.Unmarshal(filterData, &filterMap); err != nil {
				return nil, err
			}
			result["filter"] = filterMap
		}
	}

	if r.Limit != nil {
		result["limit"] = r.Limit
	}

	if r.Rank != nil {
		rankData, err := r.Rank.MarshalJSON()
		if err != nil {
			return nil, err
		}
		var rankMap interface{}
		if err := json.Unmarshal(rankData, &rankMap); err != nil {
			return nil, err
		}
		result["rank"] = rankMap
	}

	if r.Select != nil && len(r.Select.Keys) > 0 {
		keys := make([]string, len(r.Select.Keys))
		for i, k := range r.Select.Keys {
			keys[i] = string(k)
		}
		result["select"] = map[string][]string{"keys": keys}
	}

	if r.GroupBy != nil {
		groupByData, err := r.GroupBy.MarshalJSON()
		if err != nil {
			return nil, err
		}
		var groupByMap any
		if err := json.Unmarshal(groupByData, &groupByMap); err != nil {
			return nil, err
		}
		result["group_by"] = groupByMap
	}

	return json.Marshal(result)
}

// SearchCollectionOption configures a [SearchQuery] for the collection's Search method.
type SearchCollectionOption func(update *SearchQuery) error

// SearchOption configures a [SearchRequest].
type SearchOption func(req *SearchRequest) error

// WithSearchFilter sets a complete filter on the search request.
func WithSearchFilter(filter *SearchFilter) SearchOption {
	return func(req *SearchRequest) error {
		req.Filter = filter
		return nil
	}
}

// WithFilter adds a metadata filter to the search.
// Use [K] to clearly mark field names in filter expressions.
//
// Example:
//
//	WithFilter(And(EqString(K("status"), "published"), GtInt(K("views"), 100)))
func WithFilter(where WhereClause) SearchOption {
	return func(req *SearchRequest) error {
		if req.Filter == nil {
			req.Filter = &SearchFilter{}
		}
		req.Filter.Where = where
		return nil
	}
}

// WithFilterIDs restricts search to specific document IDs.
func WithFilterIDs(ids ...DocumentID) SearchOption {
	return func(req *SearchRequest) error {
		if req.Filter == nil {
			req.Filter = &SearchFilter{}
		}
		req.Filter.IDs = ids
		return nil
	}
}

// SearchPage specifies pagination for search results.
type SearchPage struct {
	Limit  int `json:"limit,omitempty"`
	Offset int `json:"offset,omitempty"`
}

// PageOpts configures pagination options.
type PageOpts func(page *SearchPage) error

// WithLimit sets the maximum number of results to return.
func WithLimit(limit int) PageOpts {
	return func(page *SearchPage) error {
		if limit < 1 {
			return errors.New("invalid limit, must be >= 1")
		}
		page.Limit = limit
		return nil
	}
}

// WithOffset sets the number of results to skip (for pagination).
func WithOffset(offset int) PageOpts {
	return func(page *SearchPage) error {
		if offset < 0 {
			return errors.New("invalid offset, must be >= 0")
		}
		page.Offset = offset
		return nil
	}
}

// WithPage adds pagination to the search request.
//
// Example:
//
//	WithPage(WithLimit(20), WithOffset(40))  // Page 3 of 20 results per page
func WithPage(pageOpts ...PageOpts) SearchOption {
	return func(req *SearchRequest) error {
		page := &SearchPage{}
		for _, opt := range pageOpts {
			if err := opt(page); err != nil {
				return err
			}
		}
		req.Limit = page
		return nil
	}
}

// WithSelect specifies which fields to include in search results.
//
// Example:
//
//	WithSelect(KDocument, KScore, K("title"), K("author"))
func WithSelect(projectionKeys ...Key) SearchOption {
	return func(req *SearchRequest) error {
		if req.Select == nil {
			req.Select = &SearchSelect{}
		}
		req.Select.Keys = append(req.Select.Keys, projectionKeys...)
		return nil
	}
}

// WithSelectAll includes all standard fields in search results.
func WithSelectAll() SearchOption {
	return func(req *SearchRequest) error {
		req.Select = &SearchSelect{
			Keys: []Key{KID, KDocument, KEmbedding, KMetadata, KScore},
		}
		return nil
	}
}

// WithRank sets a custom ranking expression on the search request.
// Use this for complex rank expressions built from arithmetic operations.
//
// Example:
//
//	knn1, _ := NewKnnRank(KnnQueryText("query1"))
//	knn2, _ := NewKnnRank(KnnQueryText("query2"))
//	combined := knn1.Multiply(FloatOperand(0.7)).Add(knn2.Multiply(FloatOperand(0.3)))
//
//	result, err := col.Search(ctx,
//	    NewSearchRequest(
//	        WithRank(combined),
//	        WithPage(WithLimit(10)),
//	    ),
//	)
func WithRank(rank Rank) SearchOption {
	return func(req *SearchRequest) error {
		req.Rank = rank
		return nil
	}
}

// WithGroupBy groups results by metadata keys using the specified aggregation.
//
// Example:
//
//	result, err := col.Search(ctx,
//	    NewSearchRequest(
//	        WithKnnRank(KnnQueryText("query"), WithKnnLimit(100)),
//	        WithGroupBy(NewGroupBy(NewMinK(3, KScore), K("category"))),
//	        WithPage(WithLimit(30)),
//	    ),
//	)
func WithGroupBy(groupBy *GroupBy) SearchOption {
	return func(req *SearchRequest) error {
		if groupBy == nil {
			return nil
		}
		if err := groupBy.Validate(); err != nil {
			return err
		}
		req.GroupBy = groupBy
		return nil
	}
}

// NewSearchRequest creates a search request and adds it to the query.
//
// Example:
//
//	result, err := collection.Search(ctx,
//	    NewSearchRequest(
//	        WithKnnRank(KnnQueryText("machine learning"), WithKnnLimit(50)),
//	        WithFilter(EqString(K("status"), "published")),
//	        WithPage(WithLimit(10)),
//	        WithSelect(KDocument, KScore),
//	    ),
//	)
func NewSearchRequest(opts ...SearchOption) SearchCollectionOption {
	return func(update *SearchQuery) error {
		search := &SearchRequest{}
		for _, opt := range opts {
			if err := opt(search); err != nil {
				return err
			}
		}
		update.Searches = append(update.Searches, *search)
		return nil
	}
}

// SearchResultImpl holds the results of a search operation.
type SearchResultImpl struct {
	IDs        [][]DocumentID       `json:"ids,omitempty"`
	Documents  [][]string           `json:"documents,omitempty"`
	Metadatas  [][]DocumentMetadata `json:"metadatas,omitempty"`
	Embeddings [][][]float32        `json:"embeddings,omitempty"`
	Scores     [][]float64          `json:"scores,omitempty"`
}

// UnmarshalJSON implements custom JSON unmarshalling for SearchResultImpl.
// This is necessary because DocumentMetadata is an interface type that
// cannot be directly unmarshalled by the standard JSON decoder.
func (r *SearchResultImpl) UnmarshalJSON(data []byte) error {
	var temp map[string]interface{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&temp); err != nil {
		return errors.Wrap(err, "failed to unmarshal SearchResult")
	}

	// Parse IDs
	if idsRaw, ok := temp["ids"]; ok && idsRaw != nil {
		if idsList, ok := idsRaw.([]interface{}); ok {
			r.IDs = make([][]DocumentID, 0, len(idsList))
			for _, idsGroup := range idsList {
				if idsGroup == nil {
					r.IDs = append(r.IDs, nil)
					continue
				}
				if group, ok := idsGroup.([]interface{}); ok {
					ids := make([]DocumentID, 0, len(group))
					for _, id := range group {
						if idStr, ok := id.(string); ok {
							ids = append(ids, DocumentID(idStr))
						}
					}
					r.IDs = append(r.IDs, ids)
				}
			}
		}
	}

	// Parse Documents
	if docsRaw, ok := temp["documents"]; ok && docsRaw != nil {
		if docsList, ok := docsRaw.([]interface{}); ok {
			r.Documents = make([][]string, 0, len(docsList))
			for _, docsGroup := range docsList {
				if docsGroup == nil {
					r.Documents = append(r.Documents, nil)
					continue
				}
				if group, ok := docsGroup.([]interface{}); ok {
					docs := make([]string, 0, len(group))
					for _, doc := range group {
						if docStr, ok := doc.(string); ok {
							docs = append(docs, docStr)
						}
					}
					r.Documents = append(r.Documents, docs)
				}
			}
		}
	}

	// Parse Metadatas - needs special handling for interface type
	if metasRaw, ok := temp["metadatas"]; ok && metasRaw != nil {
		if metasList, ok := metasRaw.([]interface{}); ok {
			r.Metadatas = make([][]DocumentMetadata, 0, len(metasList))
			for _, metasGroup := range metasList {
				if metasGroup == nil {
					r.Metadatas = append(r.Metadatas, nil)
					continue
				}
				if group, ok := metasGroup.([]interface{}); ok {
					metas := make([]DocumentMetadata, 0, len(group))
					for _, meta := range group {
						if meta == nil {
							metas = append(metas, nil)
							continue
						}
						if metaMap, ok := meta.(map[string]interface{}); ok {
							docMeta, err := NewDocumentMetadataFromMap(metaMap)
							if err != nil {
								return errors.Wrap(err, "failed to parse document metadata")
							}
							metas = append(metas, docMeta)
						}
					}
					r.Metadatas = append(r.Metadatas, metas)
				}
			}
		}
	}

	// Parse Embeddings
	if embsRaw, ok := temp["embeddings"]; ok && embsRaw != nil {
		if embsList, ok := embsRaw.([]interface{}); ok {
			r.Embeddings = make([][][]float32, 0, len(embsList))
			for _, embsGroup := range embsList {
				if embsGroup == nil {
					r.Embeddings = append(r.Embeddings, nil)
					continue
				}
				if group, ok := embsGroup.([]interface{}); ok {
					embs := make([][]float32, 0, len(group))
					for _, emb := range group {
						if emb == nil {
							embs = append(embs, nil)
							continue
						}
						if embArr, ok := emb.([]interface{}); ok {
							floats := make([]float32, 0, len(embArr))
							for _, f := range embArr {
								switch fVal := f.(type) {
								case float64:
									floats = append(floats, float32(fVal))
								case json.Number:
									v, err := fVal.Float64()
									if err != nil {
										return errors.Wrapf(err, "invalid embedding value: %v", fVal)
									}
									floats = append(floats, float32(v))
								}
							}
							embs = append(embs, floats)
						}
					}
					r.Embeddings = append(r.Embeddings, embs)
				}
			}
		}
	}

	// Parse Scores
	if scoresRaw, ok := temp["scores"]; ok && scoresRaw != nil {
		if scoresList, ok := scoresRaw.([]interface{}); ok {
			r.Scores = make([][]float64, 0, len(scoresList))
			for _, scoresGroup := range scoresList {
				if scoresGroup == nil {
					r.Scores = append(r.Scores, nil)
					continue
				}
				if group, ok := scoresGroup.([]interface{}); ok {
					scores := make([]float64, 0, len(group))
					for _, score := range group {
						switch scoreVal := score.(type) {
						case float64:
							scores = append(scores, scoreVal)
						case json.Number:
							v, err := scoreVal.Float64()
							if err != nil {
								return errors.Wrapf(err, "invalid score value: %v", scoreVal)
							}
							scores = append(scores, v)
						}
					}
					r.Scores = append(r.Scores, scores)
				}
			}
		}
	}

	return nil
}

// Rows returns the first search group's results for easy iteration.
// For multiple search requests, use RowGroups().
func (r *SearchResultImpl) Rows() []ResultRow {
	if len(r.IDs) == 0 {
		return nil
	}
	return r.buildGroupRows(0)
}

// RowGroups returns all search groups as [][]ResultRow.
func (r *SearchResultImpl) RowGroups() [][]ResultRow {
	if len(r.IDs) == 0 {
		return nil
	}
	groups := make([][]ResultRow, len(r.IDs))
	for g := range r.IDs {
		groups[g] = r.buildGroupRows(g)
	}
	return groups
}

// At returns the result at the given group and index with bounds checking.
// Returns false if either index is out of bounds.
func (r *SearchResultImpl) At(group, index int) (ResultRow, bool) {
	if group < 0 || group >= len(r.IDs) {
		return ResultRow{}, false
	}
	ids := r.IDs[group]
	if index < 0 || index >= len(ids) {
		return ResultRow{}, false
	}
	return r.buildRow(group, index), true
}

func (r *SearchResultImpl) buildGroupRows(g int) []ResultRow {
	ids := r.IDs[g]
	if len(ids) == 0 {
		return nil
	}
	rows := make([]ResultRow, len(ids))
	for i := range ids {
		rows[i] = r.buildRow(g, i)
	}
	return rows
}

func (r *SearchResultImpl) buildRow(g, i int) ResultRow {
	row := ResultRow{
		ID: r.IDs[g][i],
	}
	if g < len(r.Documents) && i < len(r.Documents[g]) {
		row.Document = r.Documents[g][i]
	}
	if g < len(r.Metadatas) && i < len(r.Metadatas[g]) {
		row.Metadata = r.Metadatas[g][i]
	}
	if g < len(r.Embeddings) && i < len(r.Embeddings[g]) {
		row.Embedding = r.Embeddings[g][i]
	}
	if g < len(r.Scores) && i < len(r.Scores[g]) {
		row.Score = r.Scores[g][i]
	}
	return row
}
