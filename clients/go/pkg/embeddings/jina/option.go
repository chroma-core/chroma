package jina

import (
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(c *JinaEmbeddingFunction) error

func WithAPIKey(apiKey string) Option {
	return func(c *JinaEmbeddingFunction) error {
		c.APIKey = embeddings.NewSecret(apiKey)
		return nil
	}
}

func WithEnvAPIKey() Option {
	return func(c *JinaEmbeddingFunction) error {
		if os.Getenv(APIKeyEnvVar) == "" {
			return errors.Errorf("%s not set", APIKeyEnvVar)
		}
		c.APIKey = embeddings.NewSecret(os.Getenv(APIKeyEnvVar))
		c.apiKeyEnvVar = APIKeyEnvVar
		return nil
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *JinaEmbeddingFunction) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.apiKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

func WithModel(model embeddings.EmbeddingModel) Option {
	return func(c *JinaEmbeddingFunction) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		c.defaultModel = model
		return nil
	}
}

func WithEmbeddingEndpoint(endpoint string) Option {
	return func(c *JinaEmbeddingFunction) error {
		if endpoint == "" {
			return errors.New("embedding endpoint cannot be empty")
		}
		c.embeddingEndpoint = endpoint
		return nil
	}
}

// WithNormalized sets the flag to indicate to Jina whether to normalize (L2 norm) the output embeddings or not. Defaults to true
func WithNormalized(normalized bool) Option {
	return func(c *JinaEmbeddingFunction) error {
		c.normalized = normalized
		return nil
	}
}

// WithEmbeddingType sets the type of the embedding to be returned by Jina. The default is float. Right now no other options are supported
func WithEmbeddingType(embeddingType EmbeddingType) Option {
	return func(c *JinaEmbeddingFunction) error {
		if embeddingType == "" {
			return errors.New("embedding type cannot be empty")
		}
		c.embeddingType = embeddingType
		return nil
	}
}

// WithTask sets the task type for the embedding. Valid values are retrieval.query, retrieval.passage,
// classification, text-matching, and separation. If not set, defaults to retrieval.passage for
// EmbedDocuments and retrieval.query for EmbedQuery.
func WithTask(task TaskType) Option {
	return func(c *JinaEmbeddingFunction) error {
		if task == "" {
			return errors.New("task cannot be empty")
		}
		c.task = task
		return nil
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(c *JinaEmbeddingFunction) error {
		c.insecure = true
		return nil
	}
}
