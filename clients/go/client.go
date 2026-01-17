package chroma

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"time"

	"github.com/pkg/errors"

	chhttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
	defaultef "github.com/chroma-core/chroma/clients/go/pkg/embeddings/default_ef"
	"github.com/chroma-core/chroma/clients/go/pkg/logger"
)

type Client interface {
	PreFlight(ctx context.Context) error
	// Heartbeat checks if the chroma instance is alive.
	Heartbeat(ctx context.Context) error
	// GetVersion returns the version of the chroma instance.
	GetVersion(ctx context.Context) (string, error)
	// GetIdentity returns the identity of the chroma instance. This is noop for v1 API.
	GetIdentity(ctx context.Context) (Identity, error)
	// GetTenant gets a tenant with the given name.
	GetTenant(ctx context.Context, tenant Tenant) (Tenant, error)
	// UseTenant sets the current tenant to the given name.
	UseTenant(ctx context.Context, tenant Tenant) error
	// UseDatabase sets a database to use for all collection operations.
	UseDatabase(ctx context.Context, database Database) error
	// CreateTenant creates a new tenant with the given name.
	CreateTenant(ctx context.Context, tenant Tenant) (Tenant, error)
	// ListDatabases returns a list of databases in the given tenant.
	ListDatabases(ctx context.Context, tenant Tenant) ([]Database, error)
	// GetDatabase gets a database with the given name from the given tenant.
	GetDatabase(ctx context.Context, db Database) (Database, error)
	// CreateDatabase creates a new database with the given name in the given tenant.
	CreateDatabase(ctx context.Context, db Database) (Database, error)
	// DeleteDatabase deletes a database with the given name from the given tenant.
	DeleteDatabase(ctx context.Context, db Database) error
	// CurrentTenant returns the current tenant.
	CurrentTenant() Tenant
	// CurrentDatabase returns the current database.
	CurrentDatabase() Database
	// Reset resets the chroma instance by all data. Use with caution.
	// Returns an error if ALLOW_RESET is not set to true.
	Reset(ctx context.Context) error
	// CreateCollection creates a new collection with the given name and options.
	CreateCollection(ctx context.Context, name string, options ...CreateCollectionOption) (Collection, error)
	// GetOrCreateCollection gets a collection with the given name. If the collection does not exist, it creates a new collection with the given options.
	// If the collection exists but the metadata does not match the options, it returns an error. Use Collection.ModifyMetadata to update the metadata.
	GetOrCreateCollection(ctx context.Context, name string, options ...CreateCollectionOption) (Collection, error)
	// DeleteCollection deletes the collection with the given name.
	DeleteCollection(ctx context.Context, name string, options ...DeleteCollectionOption) error
	// GetCollection gets a collection with the given name.
	GetCollection(ctx context.Context, name string, opts ...GetCollectionOption) (Collection, error)
	// CountCollections returns the number of collections in the current tenant and database.
	CountCollections(ctx context.Context, opts ...CountCollectionsOption) (int, error)
	// ListCollections returns a list of collections in the current tenant and database.
	ListCollections(ctx context.Context, opts ...ListCollectionsOption) ([]Collection, error)
	// Close closes the client and releases any resources.
	Close() error
}

type CollectionLifecycleOp interface {
	PrepareAndValidateCollectionRequest() error
}

type ListCollectionOp struct {
	limit    int
	offset   int
	Database Database `json:"-"`
}

func (op *ListCollectionOp) Limit() int {
	return op.limit
}

func (op *ListCollectionOp) Offset() int {
	return op.offset
}

func (op *ListCollectionOp) Resource() Resource {
	return ResourceDatabase
}

func (op *ListCollectionOp) Operation() OperationType {
	return OperationGet
}

type ListCollectionsOption func(*ListCollectionOp) error

func ListWithLimit(limit int) ListCollectionsOption {
	return func(op *ListCollectionOp) error {
		op.limit = limit
		return nil
	}
}

func ListWithOffset(offset int) ListCollectionsOption {
	return func(op *ListCollectionOp) error {
		op.offset = offset
		return nil
	}
}

func WithDatabaseList(database Database) ListCollectionsOption {
	return func(op *ListCollectionOp) error {
		if database == nil {
			return errors.New("database cannot be nil")
		}
		err := database.Validate()
		if err != nil {
			return errors.Wrap(err, "error validating database")
		}
		op.Database = database
		return nil
	}
}

func (op *ListCollectionOp) PrepareAndValidateCollectionRequest() error {
	if op.limit < 1 {
		return errors.New("limit cannot be less than 1")
	}
	if op.offset < 0 {
		return errors.New("offset cannot be negative")
	}
	if op.Database == nil {
		return errors.New("database cannot be nil")
	}
	err := op.Database.Validate()
	if err != nil {
		return errors.Wrap(err, "error validating database")
	}
	return nil
}

func NewListCollectionsOp(opts ...ListCollectionsOption) (*ListCollectionOp, error) {
	op := &ListCollectionOp{
		limit:  100,
		offset: 0,
	}
	for _, opt := range opts {
		err := opt(op)
		if err != nil {
			return nil, err
		}
	}
	return op, nil
}

type GetCollectionOp struct {
	embeddingFunction embeddings.EmbeddingFunction
	name              string
	Database          Database `json:"-"`
}

func (op *GetCollectionOp) Resource() Resource {
	return ResourceDatabase
}

func (op *GetCollectionOp) Operation() OperationType {
	return OperationGet
}

type GetCollectionOption func(*GetCollectionOp) error

func WithCollectionNameGet(name string) GetCollectionOption {
	return func(op *GetCollectionOp) error {
		if name == "" {
			return errors.New("collection name cannot be empty")
		}
		op.name = name
		return nil
	}
}

func WithEmbeddingFunctionGet(embeddingFunction embeddings.EmbeddingFunction) GetCollectionOption {
	return func(op *GetCollectionOp) error {
		if embeddingFunction == nil {
			return errors.New("embedding function cannot be nil")
		}
		op.embeddingFunction = embeddingFunction
		return nil
	}
}

func WithDatabaseGet(database Database) GetCollectionOption {
	return func(op *GetCollectionOp) error {
		if database == nil {
			return errors.New("database cannot be nil")
		}
		err := database.Validate()
		if err != nil {
			return errors.Wrap(err, "error validating database")
		}
		op.Database = database
		return nil
	}
}

func (op *GetCollectionOp) PrepareAndValidateCollectionRequest() error {
	if op.name == "" {
		return errors.New("collection name cannot be empty")
	}
	// EF validation removed - will be auto-wired from server config or user provides explicitly
	return nil
}

func NewGetCollectionOp(opts ...GetCollectionOption) (*GetCollectionOp, error) {
	op := &GetCollectionOp{}
	for _, opt := range opts {
		err := opt(op)
		if err != nil {
			return nil, err
		}
	}
	return op, nil
}

type CreateCollectionOption func(*CreateCollectionOp) error

type CreateCollectionOp struct {
	Name                   string                       `json:"name"`
	CreateIfNotExists      bool                         `json:"get_or_create,omitempty"`
	embeddingFunction      embeddings.EmbeddingFunction `json:"-"`
	Metadata               CollectionMetadata           `json:"metadata,omitempty"`
	Configuration          *CollectionConfigurationImpl `json:"configuration,omitempty"`
	Schema                 *Schema                      `json:"schema,omitempty"`
	Database               Database                     `json:"-"`
	disableEFConfigStorage bool                         `json:"-"`
}

func NewCreateCollectionOp(name string, opts ...CreateCollectionOption) (*CreateCollectionOp, error) {
	op := &CreateCollectionOp{
		Name: name,
	}
	for _, opt := range opts {
		err := opt(op)
		if err != nil {
			return nil, err
		}
	}
	return op, nil
}

func (op *CreateCollectionOp) PrepareAndValidateCollectionRequest() error {
	if op.Name == "" {
		return errors.New("collection name cannot be empty")
	}
	if op.embeddingFunction == nil {
		ef, _, err := defaultef.NewDefaultEmbeddingFunction()
		if err != nil {
			return errors.Wrap(err, "error creating default embedding function")
		}
		op.embeddingFunction = ef
	}

	// Skip EF config storage if explicitly disabled (for older Chroma versions)
	if op.disableEFConfigStorage {
		return nil
	}

	// Inject EF config into Schema or Configuration for server-side storage
	if op.Schema != nil {
		// Inject EF into the schema's vector index config (#embedding key)
		op.Schema.SetEmbeddingFunction(op.embeddingFunction)
	} else {
		// Inject EF into Configuration
		if op.Configuration == nil {
			op.Configuration = NewCollectionConfiguration()
		}
		op.Configuration.SetEmbeddingFunction(op.embeddingFunction)
	}
	return nil
}

func (op *CreateCollectionOp) MarshalJSON() ([]byte, error) {
	type Alias CreateCollectionOp
	return json.Marshal(struct{ *Alias }{Alias: (*Alias)(op)})
}

func (op *CreateCollectionOp) UnmarshalJSON(b []byte) error {
	type Alias CreateCollectionOp
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

func (op *CreateCollectionOp) Resource() Resource {
	return ResourceDatabase
}

func (op *CreateCollectionOp) Operation() OperationType {
	return OperationCreate
}

func WithCollectionMetadataCreate(metadata CollectionMetadata) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		op.Metadata = metadata
		return nil
	}
}

// WithDatabaseCreate allows the creation of a collection in a specific database, different from the default one set at Client level.
func WithDatabaseCreate(database Database) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if database == nil {
			return errors.New("database cannot be nil")
		}
		err := database.Validate()
		if err != nil {
			return errors.Wrap(err, "error validating database")
		}
		op.Database = database
		return nil
	}
}

func WithHNSWSpaceCreate(metric embeddings.DistanceMetric) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if op.Metadata == nil {
			op.Metadata = NewMetadata()
		}
		op.Metadata.SetString(HNSWSpace, string(metric))
		return nil
	}
}
func WithHNSWBatchSizeCreate(batchSize int) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if op.Metadata == nil {
			op.Metadata = NewMetadata()
		}
		if batchSize < 1 {
			return errors.New("batch size must be greater than 0")
		}
		op.Metadata.SetInt(HNSWBatchSize, int64(batchSize))
		return nil
	}
}

func WithHNSWSyncThresholdCreate(syncThreshold int) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if op.Metadata == nil {
			op.Metadata = NewMetadata()
		}
		if syncThreshold < 1 {
			return errors.New("sync threshold must be greater than 0")
		}
		op.Metadata.SetInt(HNSWSyncThreshold, int64(syncThreshold))
		return nil
	}
}

func WithHNSWMCreate(m int) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if op.Metadata == nil {
			op.Metadata = NewMetadata()
		}
		if m < 1 {
			return errors.New("m must be greater than 0")
		}
		op.Metadata.SetInt(HNSWM, int64(m))
		return nil
	}
}

func WithHNSWConstructionEfCreate(efConstruction int) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if op.Metadata == nil {
			op.Metadata = NewMetadata()
		}
		if efConstruction < 1 {
			return errors.New("efConstruction must be greater than 0")
		}
		op.Metadata.SetInt(HNSWConstructionEF, int64(efConstruction))
		return nil
	}
}

func WithHNSWSearchEfCreate(efSearch int) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if op.Metadata == nil {
			op.Metadata = NewMetadata()
		}
		if efSearch < 1 {
			return errors.New("efSearch must be greater than 0")
		}
		op.Metadata.SetInt(HNSWSearchEF, int64(efSearch))
		return nil
	}
}

func WithHNSWNumThreadsCreate(numThreads int) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if op.Metadata == nil {
			op.Metadata = NewMetadata()
		}
		if numThreads < 1 {
			return errors.New("numThreads must be greater than 0")
		}
		op.Metadata.SetInt(HNSWNumThreads, int64(numThreads))
		return nil
	}
}

func WithHNSWResizeFactorCreate(resizeFactor float64) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if op.Metadata == nil {
			op.Metadata = NewMetadata()
		}
		if resizeFactor <= 0 {
			return errors.New("resizeFactor must be greater than 0")
		}
		op.Metadata.SetFloat(HNSWResizeFactor, resizeFactor)
		return nil
	}
}

func WithEmbeddingFunctionCreate(embeddingFunction embeddings.EmbeddingFunction) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if embeddingFunction == nil {
			return errors.New("embeddingFunction cannot be nil")
		}
		op.embeddingFunction = embeddingFunction
		return nil
	}
}

func WithIfNotExistsCreate() CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		op.CreateIfNotExists = true
		return nil
	}
}

// WithSchemaCreate sets the schema for the collection
func WithSchemaCreate(schema *Schema) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if schema == nil {
			return errors.New("schema cannot be nil")
		}
		op.Schema = schema
		return nil
	}
}

// WithConfigurationCreate sets the complete configuration for the collection
func WithConfigurationCreate(config *CollectionConfigurationImpl) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if config == nil {
			return errors.New("configuration cannot be nil")
		}
		op.Configuration = config
		return nil
	}
}

// WithDisableEFConfigStorage disables storing embedding function configuration
// in the collection's server-side configuration. Use this when connecting to
// Chroma versions prior to 1.0.0 that don't support configuration.embedding_function.
func WithDisableEFConfigStorage() CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		op.disableEFConfigStorage = true
		return nil
	}
}

// WithVectorIndexCreate adds a vector index configuration to the collection schema.
// If a schema already exists on the operation, the vector index is merged into it.
func WithVectorIndexCreate(config *VectorIndexConfig) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if config == nil {
			return errors.New("vector index config cannot be nil")
		}
		if op.Schema != nil {
			return WithDefaultVectorIndex(config)(op.Schema)
		}
		schema, err := NewSchema(WithDefaultVectorIndex(config))
		if err != nil {
			return errors.Wrap(err, "failed to create schema with vector index")
		}
		op.Schema = schema
		return nil
	}
}

// WithFtsIndexCreate adds a full-text search index configuration to the collection schema.
// If a schema already exists on the operation, the FTS index is merged into it.
func WithFtsIndexCreate(config *FtsIndexConfig) CreateCollectionOption {
	return func(op *CreateCollectionOp) error {
		if config == nil {
			return errors.New("FTS index config cannot be nil")
		}
		if op.Schema != nil {
			return WithDefaultFtsIndex(config)(op.Schema)
		}
		schema, err := NewSchema(WithDefaultFtsIndex(config))
		if err != nil {
			return errors.Wrap(err, "failed to create schema with FTS index")
		}
		op.Schema = schema
		return nil
	}
}

func (op *CreateCollectionOp) String() string {
	j, err := json.Marshal(op)
	if err != nil {
		return ""
	}
	return string(j)
}

type DeleteCollectionOp struct {
	Database Database `json:"-"`
}
type DeleteCollectionOption func(*DeleteCollectionOp) error

func WithDatabaseDelete(database Database) DeleteCollectionOption {
	return func(op *DeleteCollectionOp) error {
		if database == nil {
			return errors.New("database cannot be nil")
		}
		err := database.Validate()
		if err != nil {
			return errors.Wrap(err, "error validating database")
		}
		op.Database = database
		return nil
	}
}

func (op *DeleteCollectionOp) Resource() Resource {
	return ResourceDatabase
}

func (op *DeleteCollectionOp) Operation() OperationType {
	return OperationDelete
}

func (op *DeleteCollectionOp) PrepareAndValidateCollectionRequest() error {
	if op.Database == nil {
		return errors.New("database cannot be nil")
	}
	err := op.Database.Validate()
	if err != nil {
		return errors.Wrap(err, "error validating database")
	}
	return nil
}

func NewDeleteCollectionOp(opts ...DeleteCollectionOption) (*DeleteCollectionOp, error) {
	op := &DeleteCollectionOp{}
	for _, opt := range opts {
		err := opt(op)
		if err != nil {
			return nil, err
		}
	}
	return op, nil
}

type CountCollectionsOp struct {
	Database Database `json:"-"`
}

type CountCollectionsOption func(*CountCollectionsOp) error

func WithDatabaseCount(database Database) CountCollectionsOption {
	return func(op *CountCollectionsOp) error {
		if database == nil {
			return errors.New("database cannot be nil")
		}
		err := database.Validate()
		if err != nil {
			return errors.Wrap(err, "error validating database")
		}
		op.Database = database
		return nil
	}
}

func (op *CountCollectionsOp) Resource() Resource {
	return ResourceDatabase
}

func (op *CountCollectionsOp) Operation() OperationType {
	return OperationGet
}

func (op *CountCollectionsOp) PrepareAndValidateCollectionRequest() error {
	if op.Database == nil {
		return errors.New("database cannot be nil")
	}
	err := op.Database.Validate()
	if err != nil {
		return errors.Wrap(err, "error validating database")
	}
	return nil
}

func NewCountCollectionsOp(opts ...CountCollectionsOption) (*CountCollectionsOp, error) {
	op := &CountCollectionsOp{}
	for _, opt := range opts {
		err := opt(op)
		if err != nil {
			return nil, err
		}
	}
	return op, nil
}

type BaseAPIClient struct {
	httpClient        *http.Client
	baseURL           string
	tenant            Tenant
	database          Database
	defaultHeaders    map[string]string
	httpTransport     *http.Transport
	timeout           time.Duration
	activeCollections []Collection
	preFlightConfig   map[string]interface{}
	authProvider      CredentialsProvider
	logger            logger.Logger
}

type ClientOption func(client *BaseAPIClient) error

func WithBaseURL(baseURL string) ClientOption {
	return func(c *BaseAPIClient) error {
		if baseURL == "" {
			return errors.New("baseUrl cannot be empty")
		}
		c.baseURL = baseURL
		return nil
	}
}

func WithTenant(tenant string) ClientOption {
	return func(c *BaseAPIClient) error {
		if tenant == "" {
			return errors.New("tenant cannot be empty")
		}
		c.tenant = NewTenant(tenant)
		return nil
	}
}

func WithAuth(authProvider CredentialsProvider) ClientOption {
	return func(c *BaseAPIClient) error {
		if authProvider == nil {
			return errors.New("authProvider cannot be nil")
		}
		c.authProvider = authProvider
		return nil
	}
}
func WithDatabaseAndTenant(database string, tenant string) ClientOption {
	return func(c *BaseAPIClient) error {
		if database == "" {
			return errors.New("database cannot be empty")
		}
		if tenant == "" {
			return errors.New("tenant cannot be empty")
		}
		c.tenant = NewTenant(tenant)
		c.database = NewDatabase(database, NewTenant(tenant))
		return nil
	}
}

func WithDefaultDatabaseAndTenant() ClientOption {
	return func(c *BaseAPIClient) error {
		if c.tenant == nil {
			c.tenant = NewDefaultTenant()
		}
		if c.database == nil {
			c.database = NewDefaultDatabase()
		}
		return nil
	}
}

// WithDatabaseAndTenantFromEnv sets the tenant and database from environment variables CHROMA_TENANT and CHROMA_DATABASE
func WithDatabaseAndTenantFromEnv() ClientOption {
	return func(c *BaseAPIClient) error {
		if os.Getenv("CHROMA_TENANT") != "" {
			if c.tenant == nil || c.tenant.Name() == DefaultTenant {
				c.tenant = NewTenant(os.Getenv("CHROMA_TENANT"))
			}
		}
		if os.Getenv("CHROMA_DATABASE") != "" {
			if c.database == nil || c.database.Name() == DefaultDatabase {
				c.database = NewDatabase(os.Getenv("CHROMA_DATABASE"), c.tenant)
			}
		}
		return nil
	}
}

func WithHTTPClient(httpClient *http.Client) ClientOption {
	return func(c *BaseAPIClient) error {
		if httpClient == nil {
			return errors.New("httpClient cannot be nil")
		}
		c.httpClient = httpClient
		return nil
	}
}

func WithDefaultHeaders(headers map[string]string) ClientOption {
	return func(c *BaseAPIClient) error {
		if headers == nil {
			return errors.New("headers cannot be nil")
		}
		if c.defaultHeaders == nil {
			c.defaultHeaders = make(map[string]string)
		}
		for k, v := range headers {
			c.defaultHeaders[k] = v
		}
		return nil
	}
}

func WithTimeout(timeout time.Duration) ClientOption {
	return func(c *BaseAPIClient) error {
		if timeout < 0 {
			return errors.New("timeout cannot be negative")
		}
		c.timeout = timeout
		return nil
	}
}

func WithTransport(transport *http.Transport) ClientOption {
	return func(c *BaseAPIClient) error {
		if transport == nil {
			return errors.New("transport cannot be nil")
		}
		c.httpTransport = transport
		return nil
	}
}

// Deprecated: Use WithLogger with debug level enabled. See https://github.com/chroma-core/chroma/clients/go/blob/ad35b6d37f9be4431687945ae4a77470e0832cf4/examples/v2/logging/main.go
// This function now automatically creates a development logger for backward compatibility.
// Will be removed in v0.3.0.
func WithDebug() ClientOption {
	return func(c *BaseAPIClient) error {
		_, _ = fmt.Fprintln(os.Stderr, "WARNING: WithDebug is deprecated and will be removed in v0.3.0. Use WithLogger with debug level enabled. See https://github.com/chroma-core/chroma/clients/go/blob/main/examples/v2/logging/main.go")

		// For backward compatibility, automatically enable debug logging
		if devLogger, err := logger.NewDevelopmentZapLogger(); err == nil {
			c.logger = devLogger
			c.logger.Info("Debug logging enabled via deprecated WithDebug(). Please migrate to WithLogger().")
		} else {
			// If we can't create a logger, at least log the error
			_, _ = fmt.Fprintf(os.Stderr, "Failed to create debug logger: %v\n", err)
		}
		return nil
	}
}

// WithLogger sets a custom logger for the client. If not set, a NoopLogger is used by default.
func WithLogger(l logger.Logger) ClientOption {
	return func(c *BaseAPIClient) error {
		if l == nil {
			return errors.New("logger cannot be nil")
		}
		c.logger = l
		return nil
	}
}

// WithSSLCert adds a custom SSL certificate to the client. The certificate must be in PEM format. The Option can be added multiple times to add multiple certificates. The option is mutually exclusive with WithHttpClient.
func WithSSLCert(certPath string) ClientOption {
	return func(c *BaseAPIClient) error {
		if _, err := os.Stat(certPath); certPath == "" || err != nil {
			return errors.Errorf("invalid cert path %v", err)
		}
		if c.httpTransport == nil {
			c.httpTransport = &http.Transport{}
		}
		cert, err := os.ReadFile(certPath)
		if err != nil {
			return err
		}

		// Create or reuse existing a certificate pool and add the custom certificate
		var certPool *x509.CertPool
		switch {
		case c.httpTransport.TLSClientConfig == nil:
			c.httpTransport.TLSClientConfig = &tls.Config{}
			certPool = x509.NewCertPool()
			c.httpTransport.TLSClientConfig.RootCAs = certPool
		case c.httpTransport.TLSClientConfig.RootCAs == nil:
			certPool = x509.NewCertPool()
			c.httpTransport.TLSClientConfig.RootCAs = certPool
		default:
			certPool = c.httpTransport.TLSClientConfig.RootCAs
		}
		if ok := certPool.AppendCertsFromPEM(cert); !ok {
			return errors.New("failed to append cert to pool")
		}
		c.httpTransport.TLSClientConfig.RootCAs = certPool
		return nil
	}
}

// WithInsecure skips SSL verification. The option is mutually exclusive with WithHttpClient.
// DO NOT USE IN PRODUCTION.
func WithInsecure() ClientOption {
	return func(c *BaseAPIClient) error {
		if c.httpTransport == nil {
			c.httpTransport = &http.Transport{}
		}
		if c.httpTransport.TLSClientConfig == nil {
			c.httpTransport.TLSClientConfig = &tls.Config{
				InsecureSkipVerify: true,
			}
		} else {
			c.httpTransport.TLSClientConfig.InsecureSkipVerify = true
		}
		return nil
	}
}

func newBaseAPIClient(options ...ClientOption) (*BaseAPIClient, error) {
	client := &BaseAPIClient{
		baseURL:           "http://localhost:8000/api/v2",
		httpClient:        http.DefaultClient,
		httpTransport:     &http.Transport{TLSClientConfig: &tls.Config{}},
		activeCollections: make([]Collection, 0),
		defaultHeaders: map[string]string{
			"User-Agent": "chroma-go-client/1.0",
		},
		logger: logger.NewNoopLogger(), // Default to no-op logger
	}
	client.httpClient.Transport = client.httpTransport
	for _, opt := range options {
		err := opt(client)
		if err != nil {
			return nil, err
		}
	}

	// Ensure logger is never nil
	if client.logger == nil {
		client.logger = logger.NewNoopLogger()
	}

	return client, nil
}

func (bc *BaseAPIClient) BaseURL() string {
	return bc.baseURL
}

func (bc *BaseAPIClient) SendRequest(httpReq *http.Request) (*http.Response, error) {
	for k, v := range map[string]string{
		"Accept":       "application/json",
		"Content-Type": "application/json",
	} {
		httpReq.Header.Set(k, v)
	}
	if bc.authProvider != nil {
		err := bc.authProvider.Authenticate(bc)
		if err != nil {
			return nil, errors.Wrap(err, "error getting authorization header")
		}
	}
	for k, v := range bc.defaultHeaders {
		httpReq.Header.Set(k, v)
	}
	if bc.logger.IsDebugEnabled() {
		dump, err := httputil.DumpRequestOut(httpReq, true)
		if err == nil {
			bc.logger.Debug("HTTP Request", logger.String("request", _sanitizeRequestDump(string(dump))))
		}
	}
	resp, err := bc.httpClient.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(chhttp.ChromaErrorFromHTTPResponse(nil, err), "error sending request")
	} else if resp.StatusCode >= 400 && resp.StatusCode < 599 {
		if bc.logger.IsDebugEnabled() {
			dump, err := httputil.DumpResponse(resp, true)
			if err == nil {
				bc.logger.Debug("HTTP Response (Error)", logger.String("response", _sanitizeResponseDump(string(dump))))
			} else {
				bc.logger.Debug("Failed to get body response", logger.ErrorField("error", err))
			}
		}
		chErr := chhttp.ChromaErrorFromHTTPResponse(resp, err)
		return nil, errors.Wrap(chErr, "error sending request")
	}
	if bc.logger.IsDebugEnabled() {
		dump, err := httputil.DumpResponse(resp, true)
		if err == nil {
			bc.logger.Debug("HTTP Response", logger.String("response", _sanitizeResponseDump(string(dump))))
		}
	}
	return resp, nil
}

func (bc *BaseAPIClient) ExecuteRequest(ctx context.Context, method string, path string, request interface{}) ([]byte, error) {
	var err error
	reqURL := fmt.Sprintf("%s/%s", bc.BaseURL(), path)
	var httpReq *http.Request
	if method != http.MethodDelete && method != http.MethodGet {
		reqJSON, err := json.Marshal(request)
		if err != nil {
			return nil, errors.Wrap(err, "error marshalling request JSON")
		}
		reader := bytes.NewReader(reqJSON)
		httpReq, err = http.NewRequestWithContext(ctx, method, reqURL, reader)
		if err != nil {
			return nil, errors.Wrap(err, "error creating request")
		}
	} else {
		httpReq, err = http.NewRequestWithContext(ctx, method, reqURL, nil)
		if err != nil {
			return nil, errors.Wrap(err, "error creating request")
		}
	}

	for k, v := range map[string]string{
		"Accept":       "application/json",
		"Content-Type": "application/json",
	} {
		httpReq.Header.Set(k, v)
	}
	if bc.authProvider != nil {
		err := bc.authProvider.Authenticate(bc)
		if err != nil {
			return nil, errors.Wrap(err, "error getting authorization header")
		}
	}
	for k, v := range bc.defaultHeaders {
		httpReq.Header.Set(k, v)
	}
	if bc.logger.IsDebugEnabled() {
		dump, err := httputil.DumpRequestOut(httpReq, true)
		if err == nil {
			bc.logger.Debug("HTTP Request", logger.String("request", _sanitizeRequestDump(string(dump))))
		}
	}
	resp, err := bc.httpClient.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(chhttp.ChromaErrorFromHTTPResponse(nil, err), "error sending request")
	}
	if bc.logger.IsDebugEnabled() {
		if resp == nil {
			bc.logger.Debug("HTTP Response is nil")
			return nil, errors.New("received nil response from server")
		}
		dump, dumpErr := httputil.DumpResponse(resp, true)
		if dumpErr == nil {
			bc.logger.Debug("HTTP Response", logger.String("response", _sanitizeResponseDump(string(dump))))
		}
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 599 {
		chErr := chhttp.ChromaErrorFromHTTPResponse(resp, err)
		return nil, errors.Wrap(chErr, "error sending request")
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := chhttp.ReadRespBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "error reading response body")
	}
	return []byte(respBody), nil
}

func (bc *BaseAPIClient) HTTPClient() *http.Client {
	return bc.httpClient
}
func (bc *BaseAPIClient) Tenant() Tenant {
	return bc.tenant
}

func (bc *BaseAPIClient) Database() Database {
	return bc.database
}

func (bc *BaseAPIClient) DefaultHeaders() map[string]string {
	return bc.defaultHeaders
}

func (bc *BaseAPIClient) Timeout() time.Duration {
	return bc.timeout
}

func (bc *BaseAPIClient) SetTenant(tenant Tenant) {
	bc.tenant = tenant
}

func (bc *BaseAPIClient) SetDatabase(database Database) {
	bc.database = database
}

func (bc *BaseAPIClient) SetDefaultHeaders(headers map[string]string) {
	bc.defaultHeaders = headers
}

func (bc *BaseAPIClient) SetPreFlightConfig(config map[string]interface{}) {
	bc.preFlightConfig = config
}

func (bc *BaseAPIClient) SetBaseURL(baseURL string) {
	bc.baseURL = baseURL
}

type Resource string
type OperationType string

const (
	OperationCreate    OperationType = "create"
	OperationGet       OperationType = "read"
	OperationQuery     OperationType = "query"
	OperationUpdate    OperationType = "update"
	OperationDelete    OperationType = "delete"
	ResourceTenant     Resource      = "tenant"
	ResourceDatabase   Resource      = "database"
	ResourceCollection Resource      = "collection"
	ResourceInstance   Resource      = "instance"
)

type ResourceOperation interface {
	Resource() Resource
	Operation() OperationType
}

type PreFlightConditioner interface {
	// GetPreFlightConditionsRaw returns the raw preflight response.
	GetPreFlightConditionsRaw() map[string]interface{}
	// Satisfies evaluates the resource type and a given metric to determine if the preflight condition applies.
	Satisfies(resourceOperation ResourceOperation, metric interface{}, metricName string) error
}
