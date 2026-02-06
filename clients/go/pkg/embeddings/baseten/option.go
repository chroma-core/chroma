package baseten

import (
	"net/http"
	"net/url"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// Option is a function type that can be used to modify the client.
type Option func(c *BasetenClient) error

// WithAPIKey sets the API key for the client directly.
func WithAPIKey(apiKey string) Option {
	return func(c *BasetenClient) error {
		if apiKey == "" {
			return errors.New("API key cannot be empty")
		}
		c.APIKey = embeddings.NewSecret(apiKey)
		return nil
	}
}

// WithEnvAPIKey sets the API key for the client from the environment variable BASETEN_API_KEY.
func WithEnvAPIKey() Option {
	return func(c *BasetenClient) error {
		if apiKey := os.Getenv(APIKeyEnvVar); apiKey != "" {
			c.APIKey = embeddings.NewSecret(apiKey)
			c.APIKeyEnvVar = APIKeyEnvVar
			return nil
		}
		return errors.Errorf("%s not set", APIKeyEnvVar)
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable.
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(c *BasetenClient) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			c.APIKey = embeddings.NewSecret(apiKey)
			c.APIKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

// WithBaseURL sets the base URL for the Baseten deployment (required).
func WithBaseURL(baseURL string) Option {
	return func(c *BasetenClient) error {
		if baseURL == "" {
			return errors.New("base URL cannot be empty")
		}
		if _, err := url.ParseRequestURI(baseURL); err != nil {
			return errors.Wrap(err, "invalid base URL")
		}
		c.BaseURL = baseURL
		return nil
	}
}

// WithModelID sets the model identifier for the embedding request.
func WithModelID(modelID string) Option {
	return func(c *BasetenClient) error {
		if modelID == "" {
			return errors.New("model ID cannot be empty")
		}
		c.Model = modelID
		return nil
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(c *BasetenClient) error {
		if client == nil {
			return errors.New("HTTP client cannot be nil")
		}
		c.Client = client
		return nil
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
func WithInsecure() Option {
	return func(c *BasetenClient) error {
		c.Insecure = true
		return nil
	}
}
