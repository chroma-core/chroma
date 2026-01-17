package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type OllamaClient struct {
	BaseURL        string
	Model          embeddings.EmbeddingModel
	Client         *http.Client
	DefaultHeaders map[string]string
}

type EmbeddingInput struct {
	Input  string
	Inputs []string
}

func (e EmbeddingInput) MarshalJSON() ([]byte, error) {
	if e.Input != "" {
		b, err := json.Marshal(e.Input)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal embedding input")
		}
		return b, nil
	} else if len(e.Inputs) > 0 {
		b, err := json.Marshal(e.Inputs)
		if err != nil {
			return nil, errors.Wrap(err, "failed to marshal embedding input")
		}
		return b, nil
	}
	return json.Marshal(nil)
}

type CreateEmbeddingRequest struct {
	Model string          `json:"model"`
	Input *EmbeddingInput `json:"input"`
}

type CreateEmbeddingResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

func (c *CreateEmbeddingRequest) JSON() (string, error) {
	data, err := json.Marshal(c)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal embedding request JSON")
	}
	return string(data), nil
}

func NewOllamaClient(opts ...Option) (*OllamaClient, error) {
	client := &OllamaClient{
		Client: &http.Client{},
	}
	for _, opt := range opts {
		err := opt(client)
		if err != nil {
			return nil, errors.Wrap(err, "failed to apply Ollama option")
		}
	}
	return client, nil
}

func (c *OllamaClient) createEmbedding(ctx context.Context, req *CreateEmbeddingRequest) (*CreateEmbeddingResponse, error) {
	reqJSON, err := req.JSON()
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal embedding request JSON")
	}
	endpoint, err := url.JoinPath(c.BaseURL, "/api/embed")
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse Ollama embedding endpoint")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(reqJSON))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create HTTP request")
	}
	for k, v := range c.DefaultHeaders {
		httpReq.Header.Set(k, v)
	}
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)

	resp, err := c.Client.Do(httpReq)
	if err != nil {
		return nil, errors.Wrap(err, "failed to make HTTP request to Ollama embedding endpoint")
	}
	defer resp.Body.Close()

	respData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected code [%v] while making a request to %v: %v", resp.Status, endpoint, string(respData))
	}

	var embeddingResponse CreateEmbeddingResponse
	if err := json.Unmarshal(respData, &embeddingResponse); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal embedding response")
	}
	return &embeddingResponse, nil
}

type OllamaEmbeddingFunction struct {
	apiClient *OllamaClient
}

var _ embeddings.EmbeddingFunction = (*OllamaEmbeddingFunction)(nil)

func NewOllamaEmbeddingFunction(option ...Option) (*OllamaEmbeddingFunction, error) {
	client, err := NewOllamaClient(option...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize OllamaClient")
	}
	return &OllamaEmbeddingFunction{
		apiClient: client,
	}, nil
}

func (e *OllamaEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	response, err := e.apiClient.createEmbedding(ctx, &CreateEmbeddingRequest{
		Model: string(e.apiClient.Model),
		Input: &EmbeddingInput{Inputs: documents},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed documents")
	}
	return embeddings.NewEmbeddingsFromFloat32(response.Embeddings)
}

func (e *OllamaEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	response, err := e.apiClient.createEmbedding(ctx, &CreateEmbeddingRequest{
		Model: string(e.apiClient.Model),
		Input: &EmbeddingInput{Input: document},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed query")
	}
	if len(response.Embeddings) == 0 {
		return nil, errors.New("no embedding returned from Ollama API")
	}
	return embeddings.NewEmbeddingFromFloat32(response.Embeddings[0]), nil
}

func (e *OllamaEmbeddingFunction) Name() string {
	return "ollama"
}

func (e *OllamaEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	cfg := embeddings.EmbeddingFunctionConfig{
		"model_name": string(e.apiClient.Model),
	}
	if e.apiClient.BaseURL != "" {
		cfg["url"] = e.apiClient.BaseURL
	}
	return cfg
}

func (e *OllamaEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *OllamaEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewOllamaEmbeddingFunctionFromConfig creates an Ollama embedding function from a config map.
// Uses schema-compliant field names: url, model_name.
func NewOllamaEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*OllamaEmbeddingFunction, error) {
	opts := make([]Option, 0)
	if url, ok := cfg["url"].(string); ok && url != "" {
		opts = append(opts, WithBaseURL(url))
	}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithModel(embeddings.EmbeddingModel(model)))
	}
	return NewOllamaEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("ollama", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewOllamaEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
