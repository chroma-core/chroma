package morph

import (
	"net/url"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(c *MorphClient) error

func WithBaseURL(baseURL string) Option {
	return func(c *MorphClient) error {
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

func WithModel(model string) Option {
	return func(c *MorphClient) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		c.Model = model
		return nil
	}
}

func WithEnvAPIKey() Option {
	return func(c *MorphClient) error {
		apiKey := os.Getenv(APIKeyEnvVar)
		if apiKey == "" {
			return errors.Errorf("%s not set", APIKeyEnvVar)
		}
		c.APIKey = embeddings.NewSecret(apiKey)
		c.APIKeyEnvVar = APIKeyEnvVar
		return nil
	}
}

func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(c *MorphClient) error {
		apiKey := os.Getenv(envVar)
		if apiKey == "" {
			return errors.Errorf("%s not set", envVar)
		}
		c.APIKey = embeddings.NewSecret(apiKey)
		c.APIKeyEnvVar = envVar
		return nil
	}
}

func WithAPIKey(apiKey string) Option {
	return func(c *MorphClient) error {
		if apiKey == "" {
			return errors.New("API key cannot be empty")
		}
		c.APIKey = embeddings.NewSecret(apiKey)
		return nil
	}
}

func WithInsecure() Option {
	return func(c *MorphClient) error {
		c.Insecure = true
		return nil
	}
}
