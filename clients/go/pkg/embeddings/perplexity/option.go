package perplexity

import (
	"net/http"
	"net/url"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(p *PerplexityClient) error

func WithModel(model embeddings.EmbeddingModel) Option {
	return func(p *PerplexityClient) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		p.defaultModel = model
		return nil
	}
}

func WithDimensions(dimensions int) Option {
	return func(p *PerplexityClient) error {
		if dimensions <= 0 {
			return errors.New("dimensions must be greater than 0")
		}
		p.dimensions = &dimensions
		return nil
	}
}

func WithAPIKey(apiKey string) Option {
	return func(p *PerplexityClient) error {
		if apiKey == "" {
			return errors.New("API key cannot be empty")
		}
		p.APIKey = embeddings.NewSecret(apiKey)
		return nil
	}
}

func WithEnvAPIKey() Option {
	return func(p *PerplexityClient) error {
		if apiKey := os.Getenv(APIKeyEnvVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.apiKeyEnvVar = APIKeyEnvVar
			return nil
		}
		return errors.Errorf("%s not set", APIKeyEnvVar)
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable.
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *PerplexityClient) error {
		if envVar == "" {
			return errors.New("env var cannot be empty")
		}
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.apiKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

func WithBaseURL(baseURL string) Option {
	return func(p *PerplexityClient) error {
		if baseURL == "" {
			return errors.New("base URL cannot be empty")
		}
		if _, err := url.ParseRequestURI(baseURL); err != nil {
			return errors.Wrap(err, "invalid base URL")
		}
		p.baseAPI = baseURL
		p.customBaseURL = true
		return nil
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *PerplexityClient) error {
		if client == nil {
			return errors.New("HTTP client cannot be nil")
		}
		p.client = client
		return nil
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(p *PerplexityClient) error {
		p.insecure = true
		return nil
	}
}
