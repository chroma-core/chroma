package cohere

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/pkg/errors"

	httpc "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type APIVersion string

type CohereModel string // generic type for Cohere models

func (m CohereModel) String() string {
	return string(m)
}

const (
	APIKeyEnv                    = "COHERE_API_KEY"
	DefaultBaseURL               = "https://api.cohere.ai"
	APIVersionV1      APIVersion = "v1"
	DefaultAPIVersion            = APIVersionV1
	ClientName                   = "chroma-go-client"
)

// CohereClient is a common struct for various Cohere integrations - Embeddings, Rerank etc.
type CohereClient struct {
	BaseURL       string            `validate:"required"`
	APIVersion    APIVersion        `validate:"required"`
	APIKey        embeddings.Secret `json:"-" validate:"required"`
	APIKeyEnvVar  string            `json:"-"`
	Client        *http.Client
	DefaultModel  embeddings.EmbeddingModel `validate:"required"`
	RetryStrategy httpc.RetryStrategy
	Insecure      bool
}

func NewCohereClient(opts ...Option) (*CohereClient, error) {
	client := &CohereClient{
		Client:     http.DefaultClient,
		BaseURL:    DefaultBaseURL,
		APIVersion: DefaultAPIVersion,
	}

	for _, opt := range opts {
		err := opt(client)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply Cohere option")
		}
	}
	if err := embeddings.NewValidator().Struct(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate Cohere client options")
	}
	parsed, err := url.Parse(client.BaseURL)
	if err != nil {
		return nil, errors.Wrap(err, "invalid base URL")
	}
	if !client.Insecure && !strings.EqualFold(parsed.Scheme, "https") {
		return nil, errors.New("base URL must use HTTPS scheme for secure API key transmission; use WithInsecure() to override")
	}
	if client.RetryStrategy == nil {
		var err error
		client.RetryStrategy, err = httpc.NewSimpleRetryStrategy(httpc.WithRetryableStatusCodes(429), httpc.WithExponentialBackOff())
		if err != nil {
			return nil, errors.Wrap(err, "failed to create retry strategy")
		}
	}
	return client, nil
}

func (c *CohereClient) GetAPIEndpoint(endpoint string) string {
	return strings.ReplaceAll(fmt.Sprintf("%s/%s/%s", c.BaseURL, c.APIVersion, endpoint), "^[:]//", "/")
}

// TODO GetRequest is misleading, it should be renamed to GetHTTPRequest
func (c *CohereClient) GetRequest(ctx context.Context, method string, endpoint string, content string) (*http.Request, error) {
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return nil, errors.Wrap(err, "failed to parse endpoint URL")
	}
	httpReq, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewBufferString(content))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Cohere request")
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", ClientName)
	httpReq.Header.Set("X-Client-Name", ClientName)
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey.Value())
	return httpReq, nil
}

func (c *CohereClient) DoRequest(req *http.Request) (*http.Response, error) {
	if c.RetryStrategy != nil {
		return c.RetryStrategy.DoWithRetry(c.Client, req)
	} else {
		return c.Client.Do(req)
	}
}

type Option func(p *CohereClient) error

func NoOp() Option {
	return func(p *CohereClient) error {
		return nil
	}
}

func WithBaseURL(baseURL string) Option {
	return func(p *CohereClient) error {
		if baseURL == "" {
			return errors.New("base URL cannot be empty")
		}
		p.BaseURL = strings.TrimSuffix(baseURL, "/")
		return nil
	}
}

func WithAPIKey(apiKey string) Option {
	return func(p *CohereClient) error {
		if apiKey == "" {
			return errors.New("API key cannot be empty")
		}
		p.APIKey = embeddings.NewSecret(apiKey)
		return nil
	}
}

func WithEnvAPIKey() Option {
	return func(p *CohereClient) error {
		if apiKey := os.Getenv(APIKeyEnv); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.APIKeyEnvVar = APIKeyEnv
			return nil
		}
		return errors.Errorf("API key env variable %s not found or does not contain a key", APIKeyEnv)
	}
}

// WithAPIKeyFromEnvVar sets the API key for the client from a specified environment variable
func WithAPIKeyFromEnvVar(envVar string) Option {
	return func(p *CohereClient) error {
		if apiKey := os.Getenv(envVar); apiKey != "" {
			p.APIKey = embeddings.NewSecret(apiKey)
			p.APIKeyEnvVar = envVar
			return nil
		}
		return errors.Errorf("%s not set", envVar)
	}
}

func WithAPIVersion(version APIVersion) Option {
	return func(p *CohereClient) error {
		if version == "" {
			return errors.New("API Version cannot be empty")
		}
		p.APIVersion = version
		return nil
	}
}

// WithHTTPClient sets the HTTP client for the Cohere client
func WithHTTPClient(client *http.Client) Option {
	return func(p *CohereClient) error {
		if client == nil {
			return errors.New("http client cannot be nil")
		}
		p.Client = client
		return nil
	}
}

// WithDefaultModel sets the default model for the Cohere client
func WithDefaultModel(model embeddings.EmbeddingModel) Option {
	return func(p *CohereClient) error {
		if model == "" {
			return errors.New("model cannot be empty")
		}
		p.DefaultModel = model
		return nil
	}
}

// WithRetryStrategy sets the retry strategy for the Cohere client
func WithRetryStrategy(retryStrategy httpc.RetryStrategy) Option {
	return func(p *CohereClient) error {
		if retryStrategy == nil {
			return errors.New("retry strategy cannot be nil")
		}
		p.RetryStrategy = retryStrategy
		return nil
	}
}

// WithInsecure allows the client to connect to HTTP endpoints without TLS.
// This should only be used for local development or testing.
func WithInsecure() Option {
	return func(p *CohereClient) error {
		p.Insecure = true
		return nil
	}
}
