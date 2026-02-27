package bedrock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"
	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const (
	DefaultModel  = "amazon.titan-embed-text-v1"
	DefaultRegion = "us-east-1"
)

// invoker abstracts the Bedrock runtime client for testability.
type invoker interface {
	InvokeModel(ctx context.Context, params *bedrockruntime.InvokeModelInput,
		optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error)
}

type Client struct {
	invoker           invoker
	httpClient        *http.Client
	model             string
	region            string
	profile           string
	awsConfig         *aws.Config
	bearerToken       embeddings.Secret
	bearerTokenEnvVar string
	dimensions        *int
	normalize         *bool
}

type titanRequest struct {
	InputText  string `json:"inputText"`
	Dimensions *int   `json:"dimensions,omitempty"`
	Normalize  *bool  `json:"normalize,omitempty"`
}

type titanResponse struct {
	Embedding           []float32 `json:"embedding"`
	InputTextTokenCount int       `json:"inputTextTokenCount"`
}

var regionPattern = `^[a-z]{2}(-[a-z]+-\d+){1,2}$`

func applyDefaults(c *Client) {
	if c.model == "" {
		c.model = DefaultModel
	}
	if c.region == "" {
		c.region = DefaultRegion
	}
	if c.httpClient == nil {
		c.httpClient = http.DefaultClient
	}
}

func validate(c *Client) error {
	regionRE, err := regexp.Compile(regionPattern)
	if err != nil {
		return errors.Wrap(err, "failed to compile region regex")
	}
	if !regionRE.MatchString(c.region) {
		return errors.Errorf("invalid AWS region %q", c.region)
	}
	return nil
}

func NewClient(opts ...Option) (*Client, error) {
	c := &Client{}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, errors.Wrap(err, "failed to apply Bedrock option")
		}
	}
	applyDefaults(c)
	if err := validate(c); err != nil {
		return nil, errors.Wrap(err, "failed to validate Bedrock client")
	}
	if c.invoker == nil && c.bearerToken.IsEmpty() {
		var cfg aws.Config
		if c.awsConfig != nil {
			cfg = *c.awsConfig
		} else {
			loadOpts := []func(*awsconfig.LoadOptions) error{
				awsconfig.WithRegion(c.region),
			}
			if c.profile != "" {
				loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(c.profile))
			}
			var err error
			cfg, err = awsconfig.LoadDefaultConfig(context.Background(), loadOpts...)
			if err != nil {
				return nil, errors.Wrap(err, "failed to load AWS config")
			}
		}
		c.invoker = bedrockruntime.NewFromConfig(cfg)
	}
	return c, nil
}

func (c *Client) marshalRequest(text string) ([]byte, error) {
	req := titanRequest{
		InputText:  text,
		Dimensions: c.dimensions,
		Normalize:  c.normalize,
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal Bedrock request")
	}
	return body, nil
}

// embedBearer calls the Bedrock REST API directly using a bearer token.
func (c *Client) embedBearer(ctx context.Context, text string) ([]float32, error) {
	body, err := c.marshalRequest(text)
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("https://bedrock-runtime.%s.amazonaws.com/model/%s/invoke", c.region, url.PathEscape(c.model))
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HTTP request")
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.bearerToken.Value())
	httpReq.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "Bedrock bearer request failed")
	}
	defer resp.Body.Close()

	respBody, err := chttp.ReadLimitedBody(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read Bedrock response body")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("Bedrock API returned %s: %s", resp.Status, string(respBody))
	}

	var titanResp titanResponse
	if err := json.Unmarshal(respBody, &titanResp); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal Bedrock response")
	}
	return titanResp.Embedding, nil
}

// embedSDK calls Bedrock via the AWS SDK (SigV4 auth).
func (c *Client) embedSDK(ctx context.Context, text string) ([]float32, error) {
	body, err := c.marshalRequest(text)
	if err != nil {
		return nil, err
	}

	out, err := c.invoker.InvokeModel(ctx, &bedrockruntime.InvokeModelInput{
		ModelId:     aws.String(c.model),
		ContentType: aws.String("application/json"),
		Accept:      aws.String("application/json"),
		Body:        body,
	})
	if err != nil {
		return nil, errors.Wrap(err, "Bedrock InvokeModel failed")
	}

	var resp titanResponse
	if err := json.Unmarshal(out.Body, &resp); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal Bedrock response")
	}
	return resp.Embedding, nil
}

func (c *Client) embed(ctx context.Context, text string) ([]float32, error) {
	if !c.bearerToken.IsEmpty() {
		return c.embedBearer(ctx, text)
	}
	return c.embedSDK(ctx, text)
}

var _ embeddings.EmbeddingFunction = (*BedrockEmbeddingFunction)(nil)

type BedrockEmbeddingFunction struct {
	client *Client
}

func NewBedrockEmbeddingFunction(opts ...Option) (*BedrockEmbeddingFunction, error) {
	c, err := NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize Bedrock client")
	}
	return &BedrockEmbeddingFunction{client: c}, nil
}

func (e *BedrockEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) == 0 {
		return embeddings.NewEmptyEmbeddings(), nil
	}
	result := make([]embeddings.Embedding, 0, len(documents))
	for _, doc := range documents {
		vec, err := e.client.embed(ctx, doc)
		if err != nil {
			return nil, errors.Wrap(err, "failed to embed document")
		}
		result = append(result, embeddings.NewEmbeddingFromFloat32(vec))
	}
	return result, nil
}

func (e *BedrockEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	vec, err := e.client.embed(ctx, document)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	return embeddings.NewEmbeddingFromFloat32(vec), nil
}

func (e *BedrockEmbeddingFunction) Name() string {
	return "amazon_bedrock"
}

func (e *BedrockEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	cfg := embeddings.EmbeddingFunctionConfig{
		"model_name": e.client.model,
		"region":     e.client.region,
	}
	if e.client.profile != "" {
		cfg["profile"] = e.client.profile
	}
	if !e.client.bearerToken.IsEmpty() {
		envVar := e.client.bearerTokenEnvVar
		if envVar == "" {
			envVar = BearerTokenEnvVar
		}
		cfg["api_key_env_var"] = envVar
	}
	if e.client.dimensions != nil {
		cfg["dimensions"] = *e.client.dimensions
	}
	if e.client.normalize != nil {
		cfg["normalize"] = *e.client.normalize
	}
	return cfg
}

func (e *BedrockEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *BedrockEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewBedrockEmbeddingFunctionFromConfig reconstructs from a persisted config.
func NewBedrockEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*BedrockEmbeddingFunction, error) {
	var opts []Option
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithModel(model))
	}
	if region, ok := cfg["region"].(string); ok && region != "" {
		opts = append(opts, WithRegion(region))
	}
	if profile, ok := cfg["profile"].(string); ok && profile != "" {
		opts = append(opts, WithProfile(profile))
	}
	if envVar, ok := cfg["api_key_env_var"].(string); ok && envVar != "" {
		opts = append(opts, WithBearerTokenFromEnvVar(envVar))
	}
	if dim, ok := embeddings.ConfigInt(cfg, "dimensions"); ok && dim > 0 {
		opts = append(opts, WithDimensions(dim))
	}
	if norm, ok := cfg["normalize"].(bool); ok {
		opts = append(opts, WithNormalize(norm))
	}
	return NewBedrockEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("amazon_bedrock", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewBedrockEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
