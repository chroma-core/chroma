package chroma

import (
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/logger"
)

const ChromaCloudEndpoint = "https://api.trychroma.com:8000/api/v2"

type CloudClientOption func(client *CloudAPIClient) error
type CloudAPIClient struct {
	*APIClientV2
}

func NewCloudClient(options ...ClientOption) (*CloudAPIClient, error) {
	bc, err := newBaseAPIClient()
	if err != nil {
		return nil, err
	}
	c := &CloudAPIClient{
		&APIClientV2{
			BaseAPIClient:      *bc,
			preflightLimits:    map[string]interface{}{},
			preflightCompleted: false,
			collectionCache:    map[string]Collection{},
		},
	}
	updatedOpts := make([]ClientOption, 0)
	updatedOpts = append(updatedOpts, WithDatabaseAndTenantFromEnv())
	for _, option := range options {
		if option != nil {
			updatedOpts = append(updatedOpts, option)
		}
	}
	// we override the base URL for the cloud client
	updatedOpts = append(updatedOpts, WithBaseURL(ChromaCloudEndpoint))

	for _, option := range updatedOpts {
		if err := option(&c.BaseAPIClient); err != nil {
			return nil, err
		}
	}

	if c.tenant == nil || c.tenant.Name() == DefaultTenant || c.database == nil || c.database.Name() == DefaultDatabase {
		return nil, errors.New("tenant and database must be set for cloud client. Use WithDatabaseAndTenantFromEnv option or set CHROMA_TENANT and CHROMA_DATABASE environment variables")
	}

	if c.authProvider == nil && os.Getenv("CHROMA_API_KEY") == "" {
		return nil, errors.New("api key not provided. Use WithCloudAPIKey option or set CHROMA_API_KEY environment variable")
	} else if c.authProvider == nil {
		c.authProvider = NewTokenAuthCredentialsProvider(os.Getenv("CHROMA_API_KEY"), XChromaTokenHeader)
	}

	// Ensure logger is never nil - but don't override if already set by options like WithDebug()
	if c.logger == nil {
		c.logger = logger.NewNoopLogger()
	}

	return c, nil
}

// Deprecated: use NewCloudClient instead
func NewCloudAPIClient(options ...ClientOption) (*CloudAPIClient, error) {
	return NewCloudClient(options...)
}

// WithCloudAPIKey sets the API key for the cloud client. It will automatically set a new TokenAuthCredentialsProvider.
func WithCloudAPIKey(apiKey string) ClientOption {
	return func(c *BaseAPIClient) error {
		if apiKey == "" {
			return errors.New("api key is empty")
		}
		c.authProvider = NewTokenAuthCredentialsProvider(apiKey, XChromaTokenHeader)
		return nil
	}
}
