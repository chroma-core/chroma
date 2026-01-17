package openai

import (
	"net/url"
	"os"

	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// Option is a function type that can be used to modify the client.
type Option func(c *OpenAIClient) error

func WithBaseURL(baseURL string) Option {
	return func(p *OpenAIClient) error {
		if baseURL == "" {
			return errors.New("Base URL cannot be empty")
		}
		if _, err := url.ParseRequestURI(baseURL); err != nil {
			return errors.Wrap(err, "invalid base URL")
		}
		p.BaseURL = baseURL
		return nil
	}
}

// WithOpenAIOrganizationID is an option for setting the OpenAI org id.
func WithOpenAIOrganizationID(orgID string) Option {
	return func(c *OpenAIClient) error {
		if orgID == "" {
			return errors.New("OrgID cannot be empty")
		}
		c.OrgID = orgID
		return nil
	}
}

// WithOpenAIUser is an option for setting the OpenAI user. The user is passed with every request to OpenAI. It serves for auditing purposes. If not set the user defaults to ChromaGo client.
func WithOpenAIUser(user string) Option {
	return func(c *OpenAIClient) error {
		if user == "" {
			return errors.New("User cannot be empty")
		}
		c.User = user
		return nil
	}
}

// WithModel is an option for setting the model to use. Must be one of: text-embedding-ada-002, text-embedding-3-small, text-embedding-3-large
func WithModel(model EmbeddingModel) Option {
	return func(c *OpenAIClient) error {
		if string(model) == "" {
			return errors.New("Model cannot be empty")
		}
		if model != TextEmbeddingAda002 && model != TextEmbedding3Small && model != TextEmbedding3Large {
			return errors.Errorf("invalid model name %s. Must be one of: %v", model, []string{string(TextEmbeddingAda002), string(TextEmbedding3Small), string(TextEmbedding3Large)})
		}
		c.Model = string(model)
		return nil
	}
}
func WithDimensions(dimensions int) Option {
	return func(c *OpenAIClient) error {
		if dimensions <= 0 {
			return errors.Errorf("dimensions must be greater than 0, got %d", dimensions)
		}
		c.Dimensions = &dimensions
		return nil
	}
}

// WithEnvAPIKey sets the API key for the client from the environment variable OPENAI_API_KEY
func WithEnvAPIKey() Option {
	return func(p *OpenAIClient) error {
		if apiKey := os.Getenv(APIKeyEnvVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.APIKeyEnvVar = APIKeyEnvVar
			return nil
		}
		return errors.Errorf("%s not set", APIKeyEnvVar)
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *OpenAIClient) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.APIKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(c *OpenAIClient) error {
		c.Insecure = true
		return nil
	}
}
