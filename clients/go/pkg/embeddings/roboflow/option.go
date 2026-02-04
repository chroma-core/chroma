package roboflow

import (
	"net/http"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(c *RoboflowEmbeddingFunction) error

// WithAPIKey sets the API key directly.
func WithAPIKey(apiKey string) Option {
	return func(c *RoboflowEmbeddingFunction) error {
		c.APIKey = embeddings.NewSecret(apiKey)
		return nil
	}
}

// WithEnvAPIKey sets the API key from the default environment variable (ROBOFLOW_API_KEY).
func WithEnvAPIKey() Option {
	return func(c *RoboflowEmbeddingFunction) error {
		if os.Getenv(APIKeyEnvVar) == "" {
			return errors.Errorf("%s not set", APIKeyEnvVar)
		}
		c.APIKey = embeddings.NewSecret(os.Getenv(APIKeyEnvVar))
		c.apiKeyEnvVar = APIKeyEnvVar
		return nil
	}
}

// WithAPIKeyFromEnvVar sets the API key from a specified environment variable.
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(c *RoboflowEmbeddingFunction) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			c.APIKey = embeddings.NewSecret(apiKey)
			c.apiKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

// WithBaseURL sets a custom base URL for the Roboflow API.
func WithBaseURL(baseURL string) Option {
	return func(c *RoboflowEmbeddingFunction) error {
		if baseURL == "" {
			return errors.New("base URL cannot be empty")
		}
		c.baseURL = baseURL
		return nil
	}
}

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(client *http.Client) Option {
	return func(c *RoboflowEmbeddingFunction) error {
		if client == nil {
			return errors.New("HTTP client cannot be nil")
		}
		c.httpClient = client
		return nil
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(c *RoboflowEmbeddingFunction) error {
		c.insecure = true
		return nil
	}
}

// WithCLIPVersion sets the CLIP model version for embeddings.
// Available versions: CLIPVersionViTB16 (default), CLIPVersionViTB32, CLIPVersionViTL14, etc.
// Using the same version for both text and image embeddings ensures they share the same embedding space.
func WithCLIPVersion(version CLIPVersion) Option {
	return func(c *RoboflowEmbeddingFunction) error {
		if version == "" {
			return errors.New("CLIP version cannot be empty")
		}
		c.clipVersion = version
		return nil
	}
}
