package together

import (
	"net/http"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(p *TogetherAIClient) error

func WithDefaultModel(model embeddings.EmbeddingModel) Option {
	return func(p *TogetherAIClient) error {
		if model == "" {
			return errors.New("default model cannot be empty")
		}
		p.DefaultModel = model
		return nil
	}
}

func WithMaxBatchSize(size int) Option {
	return func(p *TogetherAIClient) error {
		if size <= 0 {
			return errors.New("max batch size must be greater than 0")
		}
		p.MaxBatchSize = size
		return nil
	}
}

func WithDefaultHeaders(headers map[string]string) Option {
	return func(p *TogetherAIClient) error {
		p.DefaultHeaders = headers
		return nil
	}
}

func WithAPIToken(apiToken string) Option {
	return func(p *TogetherAIClient) error {
		if apiToken == "" {
			return errors.New("API token cannot be empty")
		}
		p.APIToken = embeddings.NewSecret(apiToken)
		return nil
	}
}

func WithEnvAPIToken() Option {
	return func(p *TogetherAIClient) error {
		if apiToken := os.Getenv(APIKeyEnvVar); apiToken != "" {
			p.APIToken = embeddings.NewSecret(apiToken)
			p.APIKeyEnvVar = APIKeyEnvVar
			return nil
		}
		return errors.Errorf("%s not set", APIKeyEnvVar)
	}
}

// WithAPITokenFromEnvVar sets the API key for the client from a specified environment variable
func WithAPITokenFromEnvVar(envVar string) Option {
	return func(p *TogetherAIClient) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIToken = embeddings.NewSecret(apiKey)
			p.APIKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *TogetherAIClient) error {
		if client == nil {
			return errors.New("HTTP client cannot be nil")
		}
		p.Client = client
		return nil
	}
}
