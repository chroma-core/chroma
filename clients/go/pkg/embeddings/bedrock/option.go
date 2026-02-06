package bedrock

import (
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/pkg/errors"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const BearerTokenEnvVar = "AWS_BEARER_TOKEN_BEDROCK"

type Option func(c *Client) error

// WithModel sets the Bedrock model ID (e.g., "amazon.titan-embed-text-v2:0").
func WithModel(model string) Option {
	return func(c *Client) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		c.model = model
		return nil
	}
}

// WithRegion sets the AWS region for the Bedrock client.
func WithRegion(region string) Option {
	return func(c *Client) error {
		if region == "" {
			return errors.New("region cannot be empty")
		}
		c.region = region
		return nil
	}
}

// WithProfile sets the AWS profile name for shared credentials.
func WithProfile(profile string) Option {
	return func(c *Client) error {
		if profile == "" {
			return errors.New("profile cannot be empty")
		}
		c.profile = profile
		return nil
	}
}

// WithAWSConfig injects a pre-configured AWS config, bypassing region/profile options.
func WithAWSConfig(cfg aws.Config) Option {
	return func(c *Client) error {
		c.awsConfig = &cfg
		return nil
	}
}

// WithBedrockClient injects a pre-built Bedrock runtime client (useful for testing).
func WithBedrockClient(client invoker) Option {
	return func(c *Client) error {
		if client == nil {
			return errors.New("bedrock client cannot be nil")
		}
		c.invoker = client
		return nil
	}
}

// WithDimensions sets the output embedding dimensions (Titan v2 only).
func WithDimensions(n int) Option {
	return func(c *Client) error {
		if n <= 0 {
			return errors.New("dimensions must be greater than 0")
		}
		c.dimensions = &n
		return nil
	}
}

// WithNormalize enables output normalization (Titan v2 only).
func WithNormalize(b bool) Option {
	return func(c *Client) error {
		c.normalize = &b
		return nil
	}
}

// WithBearerToken sets a Bedrock API key (bearer token) for direct HTTP auth.
func WithBearerToken(token string) Option {
	return func(c *Client) error {
		if token == "" {
			return errors.New("bearer token cannot be empty")
		}
		c.bearerToken = embeddings.NewSecret(token)
		return nil
	}
}

// WithBearerTokenFromEnvVar reads the bearer token from a specified environment variable.
func WithBearerTokenFromEnvVar(envVar string) Option {
	return func(c *Client) error {
		tok := os.Getenv(envVar)
		if tok == "" {
			return errors.Errorf("%s not set", envVar)
		}
		c.bearerToken = embeddings.NewSecret(tok)
		c.bearerTokenEnvVar = envVar
		return nil
	}
}

// WithEnvBearerToken reads the bearer token from the default AWS_BEARER_TOKEN_BEDROCK env var.
func WithEnvBearerToken() Option {
	return func(c *Client) error {
		tok := os.Getenv(BearerTokenEnvVar)
		if tok == "" {
			return errors.Errorf("%s not set", BearerTokenEnvVar)
		}
		c.bearerToken = embeddings.NewSecret(tok)
		c.bearerTokenEnvVar = BearerTokenEnvVar
		return nil
	}
}
