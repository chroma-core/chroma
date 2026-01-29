package chroma

import (
	"bytes"
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"

	chhttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/logger"
)

type APIClientV2 struct {
	BaseAPIClient
	preflightConditionsRaw map[string]interface{}
	preflightLimits        map[string]interface{}
	preflightCompleted     bool
	preflightMu            sync.Mutex
	collectionCache        map[string]Collection
	collectionMu           sync.RWMutex
}

func NewHTTPClient(opts ...ClientOption) (Client, error) {
	updatedOpts := make([]ClientOption, 0)
	updatedOpts = append(updatedOpts, WithDatabaseAndTenantFromEnv()) // prepend env vars as first default
	for _, option := range opts {
		if option != nil {
			updatedOpts = append(updatedOpts, option)
		}
	}
	updatedOpts = append(updatedOpts, WithDefaultDatabaseAndTenant())
	bc, err := newBaseAPIClient(updatedOpts...)
	if err != nil {
		return nil, err
	}
	if bc.BaseURL() == "" {
		bc.SetBaseURL("http://localhost:8000/api/v2")
	} else if !strings.HasSuffix(bc.BaseURL(), "/api/v2") {
		newBasePath, err := url.JoinPath(bc.BaseURL(), "/api/v2")
		if err != nil {
			return nil, err
		}
		bc.SetBaseURL(newBasePath)
	}
	c := &APIClientV2{
		BaseAPIClient:      *bc,
		preflightLimits:    map[string]interface{}{},
		preflightCompleted: false,
		collectionCache:    map[string]Collection{},
	}
	return c, nil
}

func (client *APIClientV2) PreFlight(ctx context.Context) error {
	client.preflightMu.Lock()
	defer client.preflightMu.Unlock()

	if client.preflightCompleted {
		return nil
	}

	reqURL, err := url.JoinPath(client.BaseURL(), "pre-flight-checks")
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	var preflightLimits map[string]interface{}
	if json.NewDecoder(resp.Body).Decode(&preflightLimits) != nil {
		return errors.New("error decoding preflight response")
	}
	client.preflightConditionsRaw = preflightLimits
	if mbs, ok := preflightLimits["max_batch_size"]; ok {
		if maxBatchSize, ok := mbs.(float64); ok {
			client.preflightLimits[fmt.Sprintf("%s#%s", string(ResourceCollection), string(OperationCreate))] = int(maxBatchSize)
			client.preflightLimits[fmt.Sprintf("%s#%s", string(ResourceCollection), string(OperationGet))] = int(maxBatchSize)
			client.preflightLimits[fmt.Sprintf("%s#%s", string(ResourceCollection), string(OperationQuery))] = int(maxBatchSize)
			client.preflightLimits[fmt.Sprintf("%s#%s", string(ResourceCollection), string(OperationUpdate))] = int(maxBatchSize)
			client.preflightLimits[fmt.Sprintf("%s#%s", string(ResourceCollection), string(OperationDelete))] = int(maxBatchSize)
		}
	}
	client.preflightCompleted = true
	return nil
}

func (client *APIClientV2) GetVersion(ctx context.Context) (string, error) {
	reqURL, err := url.JoinPath(client.BaseURL(), "version")
	if err != nil {
		return "", err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := chhttp.ReadRespBody(resp.Body)
	if err != nil {
		return "", errors.Wrap(err, "error reading response body")
	}
	version := strings.ReplaceAll(respBody, `"`, "")
	return version, nil
}

func (client *APIClientV2) Heartbeat(ctx context.Context) error {
	reqURL, err := url.JoinPath(client.BaseURL(), "heartbeat")
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := chhttp.ReadRespBody(resp.Body)
	if err != nil {
		return errors.Wrap(err, "error reading response body")
	}
	if strings.Contains(respBody, "nanosecond heartbeat") {
		return nil
	} else {
		return errors.Errorf("heartbeat failed")
	}
}

func (client *APIClientV2) GetTenant(ctx context.Context, tenant Tenant) (Tenant, error) {
	err := tenant.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "error validating tenant")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", tenant.Name())
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := chhttp.ReadRespBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "error reading response body")
	}
	return NewTenantFromJSON(respBody)
}

func (client *APIClientV2) CreateTenant(ctx context.Context, tenant Tenant) (Tenant, error) {
	err := tenant.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "error validating tenant")
	}
	reqJSON, err := json.Marshal(tenant)
	if err != nil {
		return nil, err
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants")
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(reqJSON))
	if err != nil {
		return nil, err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating tenant %s", tenant.Name())
	}
	defer func() { _ = resp.Body.Close() }()
	return tenant, nil
}

func (client *APIClientV2) ListDatabases(ctx context.Context, tenant Tenant) ([]Database, error) {
	err := tenant.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "error validating tenant")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", tenant.Name(), "databases")
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := chhttp.ReadRespBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "error reading response body")
	}
	var dbs []map[string]interface{}
	if err := json.Unmarshal([]byte(respBody), &dbs); err != nil {
		return nil, errors.Wrap(err, "error decoding response")
	}
	var databases []Database
	for _, db := range dbs {
		database, err := NewDatabaseFromMap(db)
		if err != nil {
			return nil, errors.Wrap(err, "error decoding database")
		}
		databases = append(databases, database)
	}
	return databases, nil
}

func (client *APIClientV2) GetDatabase(ctx context.Context, db Database) (Database, error) {
	err := db.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "error validating database")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", db.Tenant().Name(), "databases", db.Name())
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := chhttp.ReadRespBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "error reading response body")
	}
	newDB, err := NewDatabaseFromJSON(respBody)
	if err != nil {
		return nil, errors.Wrap(err, "error decoding response")
	}
	return newDB, nil
}

func (client *APIClientV2) CreateDatabase(ctx context.Context, db Database) (Database, error) {
	err := db.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "error validating database")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", db.Tenant().Name(), "databases")
	if err != nil {
		return nil, err
	}
	reqJSON, err := json.Marshal(db)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(reqJSON))
	if err != nil {
		return nil, err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return nil, errors.Wrapf(err, "error creating database %s", db.Name())
	}
	defer func() { _ = resp.Body.Close() }()
	return db, nil
}

func (client *APIClientV2) DeleteDatabase(ctx context.Context, db Database) error {
	err := db.Validate()
	if err != nil {
		return errors.Wrap(err, "error validating database")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", db.Tenant().Name(), "databases", db.Name())
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return errors.Wrapf(err, "error deleting database %s", db.Name())
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

func (client *APIClientV2) Reset(ctx context.Context) error {
	reqURL, err := url.JoinPath(client.BaseURL(), "reset")
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, nil)
	if err != nil {
		return err
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return errors.Wrap(err, "error resetting server")
	}
	defer func() { _ = resp.Body.Close() }()
	return nil
}

func (client *APIClientV2) CreateCollection(ctx context.Context, name string, options ...CreateCollectionOption) (Collection, error) {
	newOptions := append([]CreateCollectionOption{WithDatabaseCreate(client.CurrentDatabase())}, options...)
	req, err := NewCreateCollectionOp(name, newOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "error preparing collection create request")
	}
	err = req.PrepareAndValidateCollectionRequest()
	if err != nil {
		return nil, errors.Wrap(err, "error validating collection create request")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", req.Database.Tenant().Name(), "databases", req.Database.Name(), "collections")
	if err != nil {
		return nil, errors.Wrap(err, "error composing request URL")
	}
	reqJSON, err := req.MarshalJSON()
	if err != nil {
		return nil, errors.Wrap(err, "error marshalling request JSON")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(reqJSON))
	if err != nil {
		return nil, errors.Wrap(err, "error creating HTTP request")
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "error sending request")
	}
	defer func() { _ = resp.Body.Close() }()
	var cm CollectionModel
	if err := json.NewDecoder(resp.Body).Decode(&cm); err != nil {
		return nil, errors.Wrap(err, "error decoding response")
	}
	c := &CollectionImpl{
		name:              cm.Name,
		id:                cm.ID,
		tenant:            NewTenant(cm.Tenant),
		database:          NewDatabase(cm.Database, NewTenant(cm.Tenant)),
		metadata:          cm.Metadata,
		schema:            cm.Schema,
		configuration:     NewCollectionConfigurationFromMap(cm.ConfigurationJSON),
		client:            client,
		embeddingFunction: req.embeddingFunction,
		dimension:         cm.Dimension,
	}
	client.addCollectionToCache(c)
	return c, nil
}

func (client *APIClientV2) GetOrCreateCollection(ctx context.Context, name string, options ...CreateCollectionOption) (Collection, error) {
	options = append(options, WithIfNotExistsCreate())
	return client.CreateCollection(ctx, name, options...)
}

func (client *APIClientV2) DeleteCollection(ctx context.Context, name string, options ...DeleteCollectionOption) error {
	newOpts := append([]DeleteCollectionOption{WithDatabaseDelete(client.CurrentDatabase())}, options...)
	req, err := NewDeleteCollectionOp(newOpts...)
	if err != nil {
		return errors.Wrap(err, "error preparing collection delete request")
	}
	err = req.PrepareAndValidateCollectionRequest()
	if err != nil {
		return errors.Wrap(err, "error validating collection delete request")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", req.Database.Tenant().Name(), "databases", req.Database.Name(), "collections", name)
	if err != nil {
		return errors.Wrap(err, "error composing delete request URL")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, reqURL, nil)
	if err != nil {
		return errors.Wrap(err, "error creating HTTP request")
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return errors.Wrap(err, "delete request error")
	}
	defer func() { _ = resp.Body.Close() }()
	client.deleteCollectionFromCache(name)
	return nil
}

func (client *APIClientV2) GetCollection(ctx context.Context, name string, opts ...GetCollectionOption) (Collection, error) {
	newOpts := append([]GetCollectionOption{WithCollectionNameGet(name), WithDatabaseGet(client.CurrentDatabase())}, opts...)
	req, err := NewGetCollectionOp(newOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "error preparing collection get request")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", req.Database.Tenant().Name(), "databases", req.Database.Name(), "collections", name)
	if err != nil {
		return nil, errors.Wrap(err, "error composing request URL")
	}
	err = req.PrepareAndValidateCollectionRequest()
	if err != nil {
		return nil, errors.Wrap(err, "error validating collection get request")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating HTTP request")
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "error sending request")
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := chhttp.ReadRespBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "error reading response body")
	}
	var cm CollectionModel
	err = json.Unmarshal([]byte(respBody), &cm)
	if err != nil {
		return nil, errors.Wrap(err, "error decoding response")
	}
	configuration := NewCollectionConfigurationFromMap(cm.ConfigurationJSON)
	// Auto-wire EF: explicit option takes priority, otherwise build from server config
	ef := req.embeddingFunction
	if ef == nil {
		autoWiredEF, buildErr := BuildEmbeddingFunctionFromConfig(configuration)
		if buildErr != nil {
			client.logger.Warn("failed to auto-wire embedding function", logger.ErrorField("error", buildErr))
		}
		ef = autoWiredEF
	}
	c := &CollectionImpl{
		name:              cm.Name,
		id:                cm.ID,
		tenant:            NewTenant(cm.Tenant),
		database:          NewDatabase(cm.Database, NewTenant(cm.Tenant)),
		metadata:          cm.Metadata,
		schema:            cm.Schema,
		configuration:     configuration,
		client:            client,
		dimension:         cm.Dimension,
		embeddingFunction: ef,
	}
	client.addCollectionToCache(c)
	return c, nil
}

func (client *APIClientV2) CountCollections(ctx context.Context, opts ...CountCollectionsOption) (int, error) {
	newOpts := append([]CountCollectionsOption{WithDatabaseCount(client.CurrentDatabase())}, opts...)
	req, err := NewCountCollectionsOp(newOpts...)
	if err != nil {
		return 0, errors.Wrap(err, "error preparing collection count request")
	}
	err = req.PrepareAndValidateCollectionRequest()
	if err != nil {
		return 0, errors.Wrap(err, "error validating collection count request")
	}
	reqURL, err := url.JoinPath(client.BaseURL(), "tenants", req.Database.Tenant().Name(), "databases", req.Database.Name(), "collections_count")
	if err != nil {
		return 0, errors.Wrap(err, "error composing request URL")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return 0, errors.Wrap(err, "error creating HTTP request")
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return 0, errors.Wrap(err, "error sending request")
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := chhttp.ReadRespBody(resp.Body)
	if err != nil {
		return 0, errors.Wrap(err, "error reading response body")
	}
	count, err := strconv.Atoi(respBody)
	if err != nil {
		return 0, errors.Wrap(err, "error converting response to int")
	}
	return count, nil
}

func (client *APIClientV2) ListCollections(ctx context.Context, opts ...ListCollectionsOption) ([]Collection, error) {
	newOpts := append([]ListCollectionsOption{WithDatabaseList(client.CurrentDatabase())}, opts...)
	req, err := NewListCollectionsOp(newOpts...)
	if err != nil {
		return nil, errors.Wrap(err, "error preparing collection list request")
	}
	err = req.PrepareAndValidateCollectionRequest()
	if err != nil {
		return nil, errors.Wrap(err, "error validating collection list request")
	}
	reqURL, err := url.JoinPath("tenants", req.Database.Tenant().Name(), "databases", req.Database.Name(), "collections")
	if err != nil {
		return nil, errors.Wrap(err, "error composing request URL")
	}
	queryParams := url.Values{}
	if req.Limit() > 0 {
		queryParams.Set("limit", strconv.Itoa(req.Limit()))
	}
	if req.Offset() > 0 {
		queryParams.Set("offset", strconv.Itoa(req.Offset()))
	}
	reqURL = fmt.Sprintf("%s?%s", reqURL, queryParams.Encode())
	resp, err := client.ExecuteRequest(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error executing request")
	}
	var cols []CollectionModel
	if err := json.Unmarshal(resp, &cols); err != nil {
		return nil, errors.Wrap(err, "error decoding response")
	}

	var apiCollections = make([]Collection, 0)
	if len(cols) > 0 {
		for _, cm := range cols {
			configuration := NewCollectionConfigurationFromMap(cm.ConfigurationJSON)
			// Auto-wire EF from configuration
			ef, buildErr := BuildEmbeddingFunctionFromConfig(configuration)
			if buildErr != nil {
				client.logger.Warn("failed to auto-wire embedding function for collection",
					logger.String("collection", cm.Name),
					logger.ErrorField("error", buildErr))
			}
			c := &CollectionImpl{
				name:              cm.Name,
				id:                cm.ID,
				tenant:            NewTenant(cm.Tenant),
				database:          NewDatabase(cm.Database, NewTenant(cm.Tenant)),
				metadata:          cm.Metadata,
				configuration:     configuration,
				dimension:         cm.Dimension,
				client:            client,
				embeddingFunction: ef,
			}
			apiCollections = append(apiCollections, c)
		}
	}
	return apiCollections, nil
}

func (client *APIClientV2) UseTenant(ctx context.Context, tenant Tenant) error {
	t, err := client.GetTenant(ctx, tenant)
	if err != nil {
		return err
	}
	client.SetTenant(t)
	client.SetDatabase(t.Database(DefaultDatabase)) // TODO is this optimal?
	return nil
}

func (client *APIClientV2) UseDatabase(ctx context.Context, database Database) error {
	err := database.Validate()
	if err != nil {
		return errors.Wrap(err, "error validating database")
	}
	d, err := client.GetDatabase(ctx, database)
	if err != nil {
		return errors.Wrap(err, "error getting database")
	}
	client.SetDatabase(d)
	client.SetTenant(d.Tenant())
	return nil
}

func (client *APIClientV2) CurrentTenant() Tenant {
	return client.Tenant()
}

func (client *APIClientV2) CurrentDatabase() Database {
	return client.Database()
}

func (client *APIClientV2) GetPreFlightConditionsRaw() map[string]interface{} {
	return client.preflightConditionsRaw
}

func (client *APIClientV2) Satisfies(resourceOperation ResourceOperation, metric interface{}, metricName string) error {
	m, ok := client.preflightLimits[fmt.Sprintf("%s#%s", string(resourceOperation.Resource()), string(resourceOperation.Operation()))]
	if !ok {
		return nil
	}

	// preflightLimits always stores int values, use comma-ok idiom to avoid panics
	limit, ok := m.(int)
	if !ok {
		return nil
	}

	// Convert metric to int for comparison
	var metricVal int
	switch v := metric.(type) {
	case int:
		metricVal = v
	case int32:
		metricVal = int(v)
	case int64:
		metricVal = int(v)
	case float64:
		metricVal = int(v)
	case float32:
		metricVal = int(v)
	default:
		return nil
	}

	if limit < metricVal {
		return errors.Errorf("%s count limit exceeded for %s %s. Expected less than or equal %v but got %v", metricName, string(resourceOperation.Resource()), string(resourceOperation.Operation()), limit, metricVal)
	}

	return nil
}

func (client *APIClientV2) GetIdentity(ctx context.Context) (Identity, error) {
	var identity Identity
	reqURL, err := url.JoinPath(client.BaseURL(), "auth", "identity")
	if err != nil {
		return identity, errors.Wrap(err, "error composing request URL")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return identity, errors.Wrap(err, "error creating HTTP request")
	}
	resp, err := client.SendRequest(httpReq)
	if err != nil {
		return identity, errors.Wrap(err, "error sending request")
	}
	defer func() { _ = resp.Body.Close() }()
	if err := json.NewDecoder(resp.Body).Decode(&identity); err != nil {
		return identity, errors.Wrap(err, "error decoding response")
	}
	return identity, nil
}

func (client *APIClientV2) Close() error {
	if client.httpClient != nil {
		client.httpClient.CloseIdleConnections()
	}
	var errs []error
	// Copy collections while holding lock to avoid race conditions
	client.collectionMu.RLock()
	collections := make([]Collection, 0, len(client.collectionCache))
	for _, c := range client.collectionCache {
		collections = append(collections, c)
	}
	client.collectionMu.RUnlock()
	// Close collections without holding the lock to avoid deadlocks
	for _, c := range collections {
		err := c.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	// Sync the logger to flush any buffered log entries
	if client.logger != nil {
		if err := client.logger.Sync(); err != nil {
			// Ignore sync errors for stderr/stdout which are common in tests
			// These occur when the underlying file descriptor is invalid (e.g., in tests)
			// See: https://github.com/uber-go/zap/issues/991
			if !strings.Contains(err.Error(), "bad file descriptor") &&
				!strings.Contains(err.Error(), "/dev/stderr") &&
				!strings.Contains(err.Error(), "/dev/stdout") {
				errs = append(errs, errors.Wrap(err, "error syncing logger"))
			}
		}
	}
	if len(errs) > 0 {
		return stderrors.Join(errs...)
	}
	return nil
}

func (client *APIClientV2) addCollectionToCache(c Collection) {
	client.collectionMu.Lock()
	defer client.collectionMu.Unlock()
	client.collectionCache[c.Name()] = c
}

func (client *APIClientV2) deleteCollectionFromCache(name string) {
	client.collectionMu.Lock()
	defer client.collectionMu.Unlock()
	delete(client.collectionCache, name)
}
