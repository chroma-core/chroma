package chroma

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type CollectionModel struct {
	ID                string                 `json:"id"`
	Name              string                 `json:"name"`
	ConfigurationJSON map[string]interface{} `json:"configuration_json,omitempty"`
	Metadata          CollectionMetadata     `json:"metadata,omitempty"`
	Dimension         int                    `json:"dimension,omitempty"`
	Tenant            string                 `json:"tenant,omitempty"`
	Database          string                 `json:"database,omitempty"`
	Version           int                    `json:"version,omitempty"`
	LogPosition       int                    `json:"log_position,omitempty"`
	Schema            *Schema                `json:"schema,omitempty"`
}

func (op *CollectionModel) MarshalJSON() ([]byte, error) {
	type Alias CollectionModel
	return json.Marshal(struct{ *Alias }{Alias: (*Alias)(op)})
}

func (op *CollectionModel) UnmarshalJSON(b []byte) error {
	type Alias CollectionModel
	aux := &struct {
		*Alias
		Metadata CollectionMetadata `json:"metadata,omitempty"`
	}{Alias: (*Alias)(op), Metadata: NewMetadata()}
	err := json.Unmarshal(b, aux)
	if err != nil {
		return err
	}
	op.Metadata = aux.Metadata
	return nil
}

type CollectionImpl struct {
	name              string
	id                string
	tenant            Tenant
	database          Database
	metadata          CollectionMetadata
	schema            *Schema
	dimension         int
	configuration     CollectionConfiguration
	client            *APIClientV2
	embeddingFunction embeddings.EmbeddingFunction
}

type Option func(*CollectionImpl) error

func (c *CollectionImpl) Name() string {
	return c.name
}

func (c *CollectionImpl) ID() string {
	return c.id
}

func (c *CollectionImpl) Tenant() Tenant {
	return c.tenant
}

func (c *CollectionImpl) Database() Database {
	return c.database
}

func (c *CollectionImpl) Dimension() int {
	return c.dimension
}

func (c *CollectionImpl) Configuration() CollectionConfiguration {
	return c.configuration
}

func (c *CollectionImpl) Schema() *Schema {
	return c.schema
}

func (c *CollectionImpl) Add(ctx context.Context, opts ...CollectionAddOption) error {
	err := c.client.PreFlight(ctx)
	if err != nil {
		return errors.Wrap(err, "preflight failed")
	}
	addObject, err := NewCollectionAddOp(opts...)
	if err != nil {
		return errors.Wrap(err, "failed to create new collection update operation")
	}
	err = addObject.PrepareAndValidate()
	if err != nil {
		return errors.Wrap(err, "failed to prepare and validate collection update operation")
	}
	err = c.client.Satisfies(addObject, len(addObject.Ids), "documents")
	if err != nil {
		return errors.Wrap(err, "failed to satisfy collection update operation")
	}
	err = addObject.EmbedData(ctx, c.embeddingFunction)
	if err != nil {
		return errors.Wrap(err, "failed to embed data")
	}
	if sbe, ok := c.client.GetPreFlightConditionsRaw()["supports_base64_encoding"]; ok {
		if supportsBase64, ok := sbe.(bool); ok && supportsBase64 {
			packedEmbeddings := make([]any, 0)
			for _, e := range addObject.Embeddings {
				f32Emb, ok := e.(*embeddings.Float32Embedding)
				if !ok {
					// Fallback to JSON encoding for non-Float32 embeddings
					if c.client.logger.IsDebugEnabled() {
						c.client.logger.Debug("base64 encoding not supported for embedding type, falling back to JSON")
					}
					packedEmbeddings = append(packedEmbeddings, e)
					continue
				}
				packedE := packEmbeddingSafely(f32Emb.ContentAsFloat32())
				packedEmbeddings = append(packedEmbeddings, packedE)
			}
			addObject.Embeddings = packedEmbeddings
		}
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "add")
	if err != nil {
		return errors.Wrap(err, "error composing request URL")
	}
	_, err = c.client.ExecuteRequest(ctx, http.MethodPost, reqURL, addObject)
	if err != nil {
		return errors.Wrap(err, "error sending request")
	}
	return nil
}

func (c *CollectionImpl) Upsert(ctx context.Context, opts ...CollectionAddOption) error {
	err := c.client.PreFlight(ctx)
	if err != nil {
		return err
	}
	upsertObject, err := NewCollectionAddOp(opts...)
	if err != nil {
		return err
	}
	err = upsertObject.PrepareAndValidate()
	if err != nil {
		return err
	}
	err = c.client.Satisfies(upsertObject, len(upsertObject.Ids), "documents")
	if err != nil {
		return err
	}
	err = upsertObject.EmbedData(ctx, c.embeddingFunction)
	if err != nil {
		return errors.Wrap(err, "failed to embed data")
	}
	if sbe, ok := c.client.GetPreFlightConditionsRaw()["supports_base64_encoding"]; ok {
		if supportsBase64, ok := sbe.(bool); ok && supportsBase64 {
			packedEmbeddings := make([]any, 0)
			for _, e := range upsertObject.Embeddings {
				f32Emb, ok := e.(*embeddings.Float32Embedding)
				if !ok {
					// Fallback to JSON encoding for non-Float32 embeddings
					if c.client.logger.IsDebugEnabled() {
						c.client.logger.Debug("base64 encoding not supported for embedding type, falling back to JSON")
					}
					packedEmbeddings = append(packedEmbeddings, e)
					continue
				}
				packedE := packEmbeddingSafely(f32Emb.ContentAsFloat32())
				packedEmbeddings = append(packedEmbeddings, packedE)
			}
			upsertObject.Embeddings = packedEmbeddings
		}
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "upsert")
	if err != nil {
		return err
	}
	_, err = c.client.ExecuteRequest(ctx, http.MethodPost, reqURL, upsertObject)
	if err != nil {
		return err
	}
	return nil
}
func (c *CollectionImpl) Update(ctx context.Context, opts ...CollectionUpdateOption) error {
	err := c.client.PreFlight(ctx)
	if err != nil {
		return err
	}
	updateObject, err := NewCollectionUpdateOp(opts...)
	if err != nil {
		return err
	}
	err = updateObject.PrepareAndValidate()
	if err != nil {
		return err
	}
	err = c.client.Satisfies(updateObject, len(updateObject.Ids), "documents")
	if err != nil {
		return err
	}
	err = updateObject.EmbedData(ctx, c.embeddingFunction)
	if err != nil {
		return errors.Wrap(err, "failed to embed data")
	}
	if sbe, ok := c.client.GetPreFlightConditionsRaw()["supports_base64_encoding"]; ok {
		if supportsBase64, ok := sbe.(bool); ok && supportsBase64 {
			packedEmbeddings := make([]any, 0)
			for _, e := range updateObject.Embeddings {
				f32Emb, ok := e.(*embeddings.Float32Embedding)
				if !ok {
					// Fallback to JSON encoding for non-Float32 embeddings
					if c.client.logger.IsDebugEnabled() {
						c.client.logger.Debug("base64 encoding not supported for embedding type, falling back to JSON")
					}
					packedEmbeddings = append(packedEmbeddings, e)
					continue
				}
				packedE := packEmbeddingSafely(f32Emb.ContentAsFloat32())
				packedEmbeddings = append(packedEmbeddings, packedE)
			}
			updateObject.Embeddings = packedEmbeddings
		}
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "update")
	if err != nil {
		return err
	}
	_, err = c.client.ExecuteRequest(ctx, http.MethodPost, reqURL, updateObject)
	if err != nil {
		return err
	}
	return nil
}
func (c *CollectionImpl) Delete(ctx context.Context, opts ...CollectionDeleteOption) error {
	err := c.client.PreFlight(ctx)
	if err != nil {
		return err
	}
	deleteObject, err := NewCollectionDeleteOp(opts...)
	if err != nil {
		return err
	}
	err = deleteObject.PrepareAndValidate()
	if err != nil {
		return err
	}
	err = c.client.Satisfies(deleteObject, len(deleteObject.Ids), "documents")
	if err != nil {
		return err
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "delete")
	if err != nil {
		return err
	}
	_, err = c.client.ExecuteRequest(ctx, http.MethodPost, reqURL, deleteObject)
	if err != nil {
		return err
	}
	return nil
}
func (c *CollectionImpl) Count(ctx context.Context) (int, error) {
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "count")
	if err != nil {
		return 0, errors.Wrap(err, "error composing request URL")
	}
	respBody, err := c.client.ExecuteRequest(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, errors.Wrap(err, "error getting collection count")
	}
	return strconv.Atoi(string(respBody))
}
func (c *CollectionImpl) ModifyName(ctx context.Context, newName string) error {
	// TODO better name validation
	if newName == "" {
		return errors.New("newName cannot be empty")
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID())

	if err != nil {
		return errors.Wrap(err, "error composing request URL")
	}
	_, err = c.client.ExecuteRequest(ctx, http.MethodPut, reqURL, map[string]string{"new_name": newName})
	if err != nil {
		return errors.Wrap(err, "error modifying collection name")
	}
	return nil
}
func (c *CollectionImpl) ModifyMetadata(ctx context.Context, newMetadata CollectionMetadata) error {
	if newMetadata == nil {
		return errors.New("newMetadata cannot be nil")
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID())
	if err != nil {
		return err
	}
	_, err = c.client.ExecuteRequest(ctx, http.MethodPut, reqURL, map[string]interface{}{"new_metadata": newMetadata})
	if err != nil {
		return err
	}
	return nil
}
func (c *CollectionImpl) Get(ctx context.Context, opts ...CollectionGetOption) (GetResult, error) {
	getObject, err := NewCollectionGetOp(opts...)
	if err != nil {
		return nil, err
	}
	err = getObject.PrepareAndValidate()
	if err != nil {
		return nil, err
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "get")
	if err != nil {
		return nil, err
	}
	respBody, err := c.client.ExecuteRequest(ctx, http.MethodPost, reqURL, getObject)
	if err != nil {
		return nil, errors.Wrap(err, "error getting collection")
	}
	getResult := &GetResultImpl{}
	err = json.Unmarshal(respBody, getResult)
	if err != nil {
		return nil, errors.Wrap(err, "error unmarshalling get result")
	}
	return getResult, nil
}
func (c *CollectionImpl) Query(ctx context.Context, opts ...CollectionQueryOption) (QueryResult, error) {
	querybject, err := NewCollectionQueryOp(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "error creating new collection query operation")
	}
	err = querybject.PrepareAndValidate()
	if err != nil {
		return nil, errors.Wrap(err, "error validating query object")
	}
	err = querybject.EmbedData(ctx, c.embeddingFunction)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to embed data")
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "query")
	if err != nil {
		return nil, errors.Wrap(err, "error building query url")
	}
	respBody, err := c.client.ExecuteRequest(ctx, http.MethodPost, reqURL, querybject)
	if err != nil {
		return nil, errors.Wrap(err, "error sending query request")
	}
	queryResult := &QueryResultImpl{}
	err = json.Unmarshal(respBody, queryResult)
	if err != nil {
		return nil, errors.Wrap(err, "error unmarshalling query result")
	}
	return queryResult, nil
}

func (c *CollectionImpl) ModifyConfiguration(ctx context.Context, newConfig CollectionConfiguration) error {
	return errors.New("not yet supported")
}

func (c *CollectionImpl) Metadata() CollectionMetadata {
	return c.metadata
}

func (c *CollectionImpl) Fork(ctx context.Context, newName string) (Collection, error) {
	if newName == "" {
		return nil, errors.New("newName cannot be empty")
	}
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "fork")

	if err != nil {
		return nil, errors.Wrap(err, "error composing request URL")
	}
	respBody, err := c.client.ExecuteRequest(ctx, http.MethodPost, reqURL, map[string]string{"new_name": newName})
	if err != nil {
		return nil, errors.Wrap(err, "error forking collection")
	}
	var cm CollectionModel
	err = json.Unmarshal(respBody, &cm)
	if err != nil {
		return nil, errors.Wrap(err, "error decoding response")
	}
	forkedCollection := &CollectionImpl{
		name:              cm.Name,
		id:                cm.ID,
		tenant:            NewTenant(cm.Tenant),
		database:          NewDatabase(cm.Database, NewTenant(cm.Tenant)),
		metadata:          cm.Metadata,
		schema:            cm.Schema,
		client:            c.client,
		dimension:         cm.Dimension,
		embeddingFunction: c.embeddingFunction,
	}
	c.client.addCollectionToCache(forkedCollection)
	return forkedCollection, nil
}
func (c *CollectionImpl) Search(ctx context.Context, opts ...SearchCollectionOption) (SearchResult, error) {
	sq := &SearchQuery{}
	for _, opt := range opts {
		if err := opt(sq); err != nil {
			return nil, errors.Wrap(err, "error applying search option")
		}
	}

	// Embed any text queries in KnnRank expressions
	for i := range sq.Searches {
		if err := c.embedTextQueries(ctx, &sq.Searches[i]); err != nil {
			return nil, errors.Wrap(err, "error embedding text queries")
		}
	}

	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "search")
	if err != nil {
		return nil, errors.Wrap(err, "error composing request URL")
	}

	respBody, err := c.client.ExecuteRequest(ctx, http.MethodPost, reqURL, sq)
	if err != nil {
		return nil, errors.Wrap(err, "error sending search request")
	}

	var result SearchResultImpl
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, errors.Wrap(err, "error unmarshalling search result")
	}

	return &result, nil
}

// embedTextQueries embeds any text queries in the search request.
// It clones the rank tree first to avoid mutating the user's original rank objects,
// which allows safe reuse of rank definitions across multiple searches.
func (c *CollectionImpl) embedTextQueries(ctx context.Context, req *SearchRequest) error {
	if req.Rank == nil {
		return nil
	}
	req.Rank = cloneRank(req.Rank)
	return c.embedRankTextQueries(ctx, req.Rank)
}

// cloneRank creates a deep copy of a rank tree to avoid mutating user-provided rank objects.
func cloneRank(rank Rank) Rank {
	if rank == nil {
		return nil
	}
	switch r := rank.(type) {
	case *KnnRank:
		clone := *r
		return &clone
	case *RrfRank:
		clone := *r
		clone.Ranks = make([]RankWithWeight, len(r.Ranks))
		for i, rw := range r.Ranks {
			clone.Ranks[i] = RankWithWeight{
				Rank:   cloneRank(rw.Rank),
				Weight: rw.Weight,
			}
		}
		return &clone
	case *ValRank:
		clone := *r
		return &clone
	case *SumRank:
		clone := *r
		clone.ranks = make([]Rank, len(r.ranks))
		for i, child := range r.ranks {
			clone.ranks[i] = cloneRank(child)
		}
		return &clone
	case *SubRank:
		return &SubRank{
			left:  cloneRank(r.left),
			right: cloneRank(r.right),
		}
	case *MulRank:
		clone := *r
		clone.ranks = make([]Rank, len(r.ranks))
		for i, child := range r.ranks {
			clone.ranks[i] = cloneRank(child)
		}
		return &clone
	case *DivRank:
		return &DivRank{
			left:  cloneRank(r.left),
			right: cloneRank(r.right),
		}
	case *AbsRank:
		return &AbsRank{rank: cloneRank(r.rank)}
	case *ExpRank:
		return &ExpRank{rank: cloneRank(r.rank)}
	case *LogRank:
		return &LogRank{rank: cloneRank(r.rank)}
	case *MaxRank:
		clone := *r
		clone.ranks = make([]Rank, len(r.ranks))
		for i, child := range r.ranks {
			clone.ranks[i] = cloneRank(child)
		}
		return &clone
	case *MinRank:
		clone := *r
		clone.ranks = make([]Rank, len(r.ranks))
		for i, child := range r.ranks {
			clone.ranks[i] = cloneRank(child)
		}
		return &clone
	default:
		return rank
	}
}

// embedRankTextQueries recursively embeds text queries in rank expressions.
// It validates expression depth and checks context cancellation.
func (c *CollectionImpl) embedRankTextQueries(ctx context.Context, rank Rank) error {
	return c.embedRankTextQueriesWithDepth(ctx, rank, 0)
}

// embedRankTextQueriesWithDepth is the internal implementation that tracks recursion depth.
func (c *CollectionImpl) embedRankTextQueriesWithDepth(ctx context.Context, rank Rank, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if rank == nil {
		return nil
	}
	if depth > MaxExpressionDepth {
		return errors.Errorf("rank expression exceeds maximum depth of %d", MaxExpressionDepth)
	}
	switch r := rank.(type) {
	case *KnnRank:
		if text, ok := r.Query.(string); ok {
			if c.embeddingFunction == nil {
				return errors.New("embedding function required for text queries")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			emb, err := c.embeddingFunction.EmbedQuery(ctx, text)
			if err != nil {
				return errors.Wrap(err, "error embedding text query")
			}
			r.Query = emb.ContentAsFloat32()
		}
	case *RrfRank:
		for _, rw := range r.Ranks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if rw.Rank == nil {
				continue
			}
			if err := c.embedRankTextQueriesWithDepth(ctx, rw.Rank, depth+1); err != nil {
				return err
			}
		}
	case *SumRank:
		for _, child := range r.ranks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if child == nil {
				continue
			}
			if err := c.embedRankTextQueriesWithDepth(ctx, child, depth+1); err != nil {
				return err
			}
		}
	case *MulRank:
		for _, child := range r.ranks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if child == nil {
				continue
			}
			if err := c.embedRankTextQueriesWithDepth(ctx, child, depth+1); err != nil {
				return err
			}
		}
	case *SubRank:
		if r.left != nil {
			if err := c.embedRankTextQueriesWithDepth(ctx, r.left, depth+1); err != nil {
				return err
			}
		}
		if r.right != nil {
			if err := c.embedRankTextQueriesWithDepth(ctx, r.right, depth+1); err != nil {
				return err
			}
		}
	case *DivRank:
		if r.left != nil {
			if err := c.embedRankTextQueriesWithDepth(ctx, r.left, depth+1); err != nil {
				return err
			}
		}
		if r.right != nil {
			if err := c.embedRankTextQueriesWithDepth(ctx, r.right, depth+1); err != nil {
				return err
			}
		}
	case *AbsRank:
		if r.rank != nil {
			return c.embedRankTextQueriesWithDepth(ctx, r.rank, depth+1)
		}
	case *ExpRank:
		if r.rank != nil {
			return c.embedRankTextQueriesWithDepth(ctx, r.rank, depth+1)
		}
	case *LogRank:
		if r.rank != nil {
			return c.embedRankTextQueriesWithDepth(ctx, r.rank, depth+1)
		}
	case *MaxRank:
		for _, child := range r.ranks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if child == nil {
				continue
			}
			if err := c.embedRankTextQueriesWithDepth(ctx, child, depth+1); err != nil {
				return err
			}
		}
	case *MinRank:
		for _, child := range r.ranks {
			if err := ctx.Err(); err != nil {
				return err
			}
			if child == nil {
				continue
			}
			if err := c.embedRankTextQueriesWithDepth(ctx, child, depth+1); err != nil {
				return err
			}
		}
	default:
		// No action needed for other rank types
	}
	return nil
}

func (c *CollectionImpl) IndexingStatus(ctx context.Context) (*IndexingStatus, error) {
	reqURL, err := url.JoinPath("tenants", c.Tenant().Name(), "databases", c.Database().Name(), "collections", c.ID(), "indexing_status")
	if err != nil {
		return nil, errors.Wrap(err, "error composing request URL")
	}
	respBody, err := c.client.ExecuteRequest(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error getting indexing status")
	}
	result := &IndexingStatus{}
	if err := json.Unmarshal(respBody, result); err != nil {
		return nil, errors.Wrap(err, "error unmarshalling indexing status")
	}
	return result, nil
}

func (c *CollectionImpl) Close() error {
	if c.embeddingFunction != nil {
		if closer, ok := c.embeddingFunction.(io.Closer); ok {
			return closer.Close()
		}
	}
	return nil
}

// TODO add utility methods for metadata lookups
