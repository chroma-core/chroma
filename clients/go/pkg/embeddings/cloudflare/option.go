package cloudflare

import (
	"net/http"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(p *CloudflareClient) error

func WithGatewayEndpoint(endpoint string) Option {
	return func(p *CloudflareClient) error {
		if endpoint == "" {
			return errors.New("endpoint cannot be empty")
		}
		p.BaseAPI = endpoint
		p.IsGateway = true
		return nil
	}
}

func WithDefaultModel(model embeddings.EmbeddingModel) Option {
	return func(p *CloudflareClient) error {
		p.DefaultModel = model
		return nil
	}
}

func WithMaxBatchSize(size int) Option {
	return func(p *CloudflareClient) error {
		if size <= 0 {
			return errors.New("max batch size must be greater than 0")
		}
		p.MaxBatchSize = size
		return nil
	}
}

func WithDefaultHeaders(headers map[string]string) Option {
	return func(p *CloudflareClient) error {
		p.DefaultHeaders = headers
		return nil
	}
}

func WithAPIToken(apiToken string) Option {
	return func(p *CloudflareClient) error {
		p.APIToken = embeddings.NewSecret(apiToken)
		return nil
	}
}

func WithAccountID(accountID string) Option {
	return func(p *CloudflareClient) error {
		if accountID == "" {
			return errors.New("account ID cannot be empty")
		}
		p.AccountID = accountID
		return nil
	}
}

func WithEnvAPIToken() Option {
	const cfAPITokenEnvVar = "CF_API_TOKEN"
	return func(p *CloudflareClient) error {
		if apiToken := os.Getenv(cfAPITokenEnvVar); apiToken != "" {
			p.APIToken = embeddings.NewSecret(apiToken)
			p.APIKeyEnvVar = cfAPITokenEnvVar
			return nil
		}
		return errors.Errorf("%s not set", cfAPITokenEnvVar)
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *CloudflareClient) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIToken = embeddings.NewSecret(apiKey)
			p.APIKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

func WithEnvAccountID() Option {
	return func(p *CloudflareClient) error {
		if accountID := os.Getenv("CF_ACCOUNT_ID"); accountID != "" {
			p.AccountID = accountID
			return nil
		}
		return errors.Errorf("CF_ACCOUNT_ID not set")
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *CloudflareClient) error {
		if client == nil {
			return errors.New("http client cannot be nil")
		}
		p.Client = client
		return nil
	}
}

func WithEnvGatewayEndpoint() Option {
	return func(p *CloudflareClient) error {
		if endpoint := os.Getenv("CF_GATEWAY_ENDPOINT"); endpoint != "" {
			p.BaseAPI = endpoint
			p.IsGateway = true
			return nil
		}
		return errors.Errorf("CF_GATEWAY_ENDPOINT not set")
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(p *CloudflareClient) error {
		p.Insecure = true
		return nil
	}
}
