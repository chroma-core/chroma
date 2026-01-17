package chromacloudsplade

import (
	"net/http"
	"net/url"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(c *Client) error

func WithModel(model embeddings.EmbeddingModel) Option {
	return func(c *Client) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		c.Model = model
		return nil
	}
}

func WithAPIKey(apiKey string) Option {
	return func(c *Client) error {
		if apiKey == "" {
			return errors.New("API key cannot be empty")
		}
		c.APIKey = embeddings.NewSecret(apiKey)
		return nil
	}
}

func WithEnvAPIKey() Option {
	return func(c *Client) error {
		if apiKey := os.Getenv(APIKeyEnvVar); apiKey != "" {
			c.APIKey = embeddings.NewSecret(apiKey)
			return nil
		}
		return errors.Errorf("%s not set", APIKeyEnvVar)
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *Client) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) error {
		if client == nil {
			return errors.New("HTTP client cannot be nil")
		}
		c.HTTPClient = client
		return nil
	}
}

func WithBaseURL(baseURL string) Option {
	return func(c *Client) error {
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

func WithInsecure() Option {
	return func(c *Client) error {
		c.Insecure = true
		return nil
	}
}
