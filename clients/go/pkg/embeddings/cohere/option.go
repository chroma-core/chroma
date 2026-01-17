package cohere

import (
	"github.com/pkg/errors"

	ccommons "github.com/chroma-core/chroma/clients/go/pkg/commons/cohere"
	httpc "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type Option func(p *CohereEmbeddingFunction) ccommons.Option

// WithBaseURL sets the base URL for the Cohere API - the default is https://api.cohere.ai
func WithBaseURL(baseURL string) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		return ccommons.WithBaseURL(baseURL)
	}
}

func WithAPIKey(apiKey string) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		return ccommons.WithAPIKey(apiKey)
	}
}

// WithEnvAPIKey configures the client to use the COHERE_API_KEY environment variable as the API key
func WithEnvAPIKey() Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		return ccommons.WithEnvAPIKey()
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option { return ccommons.WithAPIKeyFromEnvVar(envVar) }
}

func WithAPIVersion(apiVersion ccommons.APIVersion) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		return ccommons.WithAPIVersion(apiVersion)
	}
}

// WithModel sets the default model for the Cohere API - Available models:
// embed-english-v3.0 1024
// embed-multilingual-v3.0 1024
// embed-english-light-v3.0 384
// embed-multilingual-light-v3.0 384
// embed-english-v2.0 4096 (default)
// embed-english-light-v2.0 1024
// embed-multilingual-v2.0 768
func WithModel(model embeddings.EmbeddingModel) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		return ccommons.WithDefaultModel(model)
	}
}

// WithDefaultModel sets the default model for the Cohere. This can be overridden in the context of EF embed call. Available models:
// embed-english-v3.0 1024
// embed-multilingual-v3.0 1024
// embed-english-light-v3.0 384
// embed-multilingual-light-v3.0 384
// embed-english-v2.0 4096 (default)
// embed-english-light-v2.0 1024
// embed-multilingual-v2.0 768
func WithDefaultModel(model embeddings.EmbeddingModel) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		return ccommons.WithDefaultModel(model)
	}
}

// WithTruncateMode sets the default truncate mode for the Cohere API - Available modes:
// NONE
// START
// END (default)
func WithTruncateMode(truncate TruncateMode) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		if truncate != NONE && truncate != START && truncate != END {
			return func(c *ccommons.CohereClient) error {
				return errors.Errorf("invalid truncate mode %s", truncate)
			}
		}
		p.DefaultTruncateMode = truncate
		return ccommons.NoOp()
	}
}

// WithEmbeddingTypes sets the default embedding types for the Cohere API - Available types:
// float (default)
// int8
// uint8
// binary
// ubinary
// TODO we do not have support for returning multiple embedding types from the EmbeddingFunction, so for float->int8, unit8 are supported and returned in the that order
func WithEmbeddingTypes(embeddingTypes ...EmbeddingType) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		// if embeddingstypes contains binary or ubinary error
		for _, et := range embeddingTypes {
			if et == EmbeddingTypeBinary || et == EmbeddingTypeUBinary {
				return func(c *ccommons.CohereClient) error {
					return errors.Errorf("embedding type %s is not supported", et)
				}
			}
		}
		// if embeddingstypes is empty, set to default
		if len(embeddingTypes) == 0 {
			embeddingTypes = []EmbeddingType{EmbeddingTypeFloat32}
		}
		p.DefaultEmbeddingTypes = embeddingTypes
		return ccommons.NoOp()
	}
}

// WithRetryStrategy configures the client to use the specified retry strategy
func WithRetryStrategy(retryStrategy httpc.RetryStrategy) Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		return ccommons.WithRetryStrategy(retryStrategy)
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(p *CohereEmbeddingFunction) ccommons.Option {
		return ccommons.WithInsecure()
	}
}
