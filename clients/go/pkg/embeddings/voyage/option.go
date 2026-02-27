package voyage

import (
	"net/http"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(p *VoyageAIClient) error

func WithDefaultModel(model embeddings.EmbeddingModel) Option {
	return func(p *VoyageAIClient) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		p.DefaultModel = model
		return nil
	}
}

func WithMaxBatchSize(size int) Option {
	return func(p *VoyageAIClient) error {
		if size <= 0 {
			return errors.New("max batch size must be greater than 0")
		}
		p.MaxBatchSize = size
		return nil
	}
}

func WithDefaultHeaders(headers map[string]string) Option {
	return func(p *VoyageAIClient) error {
		p.DefaultHeaders = headers
		return nil
	}
}

func WithAPIKey(apiToken string) Option {
	return func(p *VoyageAIClient) error {
		if apiToken == "" {
			return errors.New("API key cannot be empty")
		}
		p.APIKey = embeddings.NewSecret(apiToken)
		return nil
	}
}

func WithEnvAPIKey() Option {
	return func(p *VoyageAIClient) error {
		if apiToken := os.Getenv(APIKeyEnvVar); apiToken != "" {
			p.APIKey = embeddings.NewSecret(apiToken)
			p.APIKeyEnvVar = APIKeyEnvVar
			return nil
		}
		return errors.Errorf("%s not set", APIKeyEnvVar)
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *VoyageAIClient) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.APIKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *VoyageAIClient) error {
		if client == nil {
			return errors.New("HTTP client cannot be nil")
		}
		p.Client = client
		return nil
	}
}

func WithTruncation(truncation bool) Option {
	return func(p *VoyageAIClient) error {
		p.DefaultTruncation = &truncation
		return nil
	}
}

func WithEncodingFormat(format EncodingFormat) Option {
	return func(p *VoyageAIClient) error {
		if format == "" {
			return errors.New("encoding format cannot be empty")
		}
		var defaultEncodingFormat = format
		p.DefaultEncodingFormat = &defaultEncodingFormat
		return nil
	}
}

func WithBaseURL(baseURL string) Option {
	return func(p *VoyageAIClient) error {
		if baseURL == "" {
			return errors.New("base URL cannot be empty")
		}
		p.BaseAPI = baseURL
		return nil
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(p *VoyageAIClient) error {
		p.Insecure = true
		return nil
	}
}
