package chromacloud

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

const (
	defaultBaseURL = "https://embed.trychroma.com"
	defaultModel   = "Qwen/Qwen3-Embedding-0.6B"
	defaultTimeout = 60 * time.Second
	APIKeyEnvVar   = "CHROMA_API_KEY"
)

type Task string

const (
	TaskDefault  Task = ""
	TaskNLToCode Task = "nl_to_code"
)

type instructionPair struct {
	Document string
	Query    string
}

var taskInstructions = map[Task]instructionPair{
	TaskDefault: {
		Document: "",
		Query:    "",
	},
	TaskNLToCode: {
		Document: "Retrieve relevant code snippets based on the natural language query",
		Query:    "Find code implementations that match this description",
	},
}

type Client struct {
	BaseURL      string
	APIKey       embeddings.Secret `json:"-"`
	APIKeyEnvVar string
	Model        embeddings.EmbeddingModel
	Task         Task
	HTTPClient   *http.Client
	Insecure     bool
}

type embeddingRequest struct {
	Instructions string   `json:"instructions"`
	Texts        []string `json:"texts"`
}

type embeddingResponse struct {
	Embeddings [][]float32 `json:"embeddings,omitempty"`
	Error      string      `json:"error,omitempty"`
}

func applyDefaults(c *Client) {
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{Timeout: defaultTimeout}
	}
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	if c.Model == "" {
		c.Model = defaultModel
	}
}

func validate(c *Client) error {
	if c.APIKey.IsEmpty() {
		return errors.New("API key is required")
	}
	parsed, err := url.Parse(c.BaseURL)
	if err != nil {
		return errors.Wrap(err, "invalid base URL")
	}
	if !c.Insecure && !strings.EqualFold(parsed.Scheme, "https") {
		return errors.New("base URL must use HTTPS scheme for secure API key transmission; use WithInsecure() to override")
	}
	return nil
}

func NewClient(opts ...Option) (*Client, error) {
	client := &Client{}
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, errors.Wrap(err, "failed to apply option")
		}
	}
	applyDefaults(client)
	if err := validate(client); err != nil {
		return nil, errors.Wrap(err, "failed to validate client")
	}
	return client, nil
}

func (c *Client) getInstruction(forQuery bool) string {
	pair, ok := taskInstructions[c.Task]
	if !ok {
		pair = taskInstructions[TaskDefault]
	}
	if forQuery {
		return pair.Query
	}
	return pair.Document
}

func (c *Client) embed(ctx context.Context, texts []string, forQuery bool) ([][]float32, error) {
	if len(texts) == 0 {
		return make([][]float32, 0), nil
	}

	reqBody := embeddingRequest{
		Instructions: c.getInstruction(forQuery),
		Texts:        texts,
	}
	reqData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal request")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL, bytes.NewReader(reqData))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)
	req.Header.Set("Cache-Control", "no-store")
	req.Header.Set("x-chroma-token", c.APIKey.Value())
	req.Header.Set("x-chroma-embedding-model", string(c.Model))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var embResp embeddingResponse
	if err := json.Unmarshal(body, &embResp); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal response")
	}

	if embResp.Error != "" {
		return nil, errors.Errorf("API error [status %d]: %s", resp.StatusCode, embResp.Error)
	}

	return embResp.Embeddings, nil
}

var _ embeddings.EmbeddingFunction = (*EmbeddingFunction)(nil)

type EmbeddingFunction struct {
	client *Client
}

func NewEmbeddingFunction(opts ...Option) (*EmbeddingFunction, error) {
	client, err := NewClient(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create client")
	}
	return &EmbeddingFunction{client: client}, nil
}

func (e *EmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) == 0 {
		return embeddings.NewEmptyEmbeddings(), nil
	}

	vectors, err := e.client.embed(ctx, documents, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}

	result := make([]embeddings.Embedding, len(vectors))
	for i, vec := range vectors {
		result[i] = embeddings.NewEmbeddingFromFloat32(vec)
	}
	return result, nil
}

func (e *EmbeddingFunction) EmbedQuery(ctx context.Context, query string) (embeddings.Embedding, error) {
	vectors, err := e.client.embed(ctx, []string{query}, true)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(vectors) == 0 {
		return nil, errors.New("no embedding returned")
	}
	return embeddings.NewEmbeddingFromFloat32(vectors[0]), nil
}

func (e *EmbeddingFunction) Name() string {
	return "chroma_cloud"
}

func (e *EmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.client.APIKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"model_name":      string(e.client.Model),
		"api_key_env_var": envVar,
	}
	if e.client.Insecure {
		cfg["insecure"] = true
	}
	if e.client.BaseURL != "" {
		cfg["base_url"] = e.client.BaseURL
	}
	return cfg
}

func (e *EmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *EmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewEmbeddingFunctionFromConfig creates a ChromaCloud embedding function from a config map.
// Uses schema-compliant field names: api_key_env_var, model_name, base_url, insecure.
func NewEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*EmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar)}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithModel(embeddings.EmbeddingModel(model)))
	}
	if baseURL, ok := cfg["base_url"].(string); ok && baseURL != "" {
		opts = append(opts, WithBaseURL(baseURL))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("ChromaCloud")
		opts = append(opts, WithInsecure())
	}
	return NewEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("chroma_cloud", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
