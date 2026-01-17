package hf

import (
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(p *HuggingFaceClient) error

func WithBaseURL(baseURL string) Option {
	return func(p *HuggingFaceClient) error {
		if baseURL == "" {
			return errors.New("base URL cannot be empty")
		}
		p.BaseURL = baseURL
		return nil
	}
}

func WithAPIKey(apiKey string) Option {
	return func(p *HuggingFaceClient) error {
		if apiKey == "" {
			return errors.New("API key cannot be empty")
		}
		p.APIKey = embeddings.NewSecret(apiKey)
		return nil
	}
}

func WithEnvAPIKey() Option {
	return func(p *HuggingFaceClient) error {
		if os.Getenv(APIKeyEnvVar) == "" {
			return errors.Errorf("%s not set", APIKeyEnvVar)
		}
		p.APIKey = embeddings.NewSecret(os.Getenv(APIKeyEnvVar))
		p.APIKeyEnvVar = APIKeyEnvVar
		return nil
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *HuggingFaceClient) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.APIKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

func WithModel(model string) Option {
	return func(p *HuggingFaceClient) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		p.Model = model
		return nil
	}
}

func WithDefaultHeaders(headers map[string]string) Option {
	return func(p *HuggingFaceClient) error {
		p.DefaultHeaders = headers
		return nil
	}
}

func WithIsHFEIEndpoint() Option {
	return func(p *HuggingFaceClient) error {
		p.IsHFEIEndpoint = true
		return nil
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(p *HuggingFaceClient) error {
		p.Insecure = true
		return nil
	}
}
