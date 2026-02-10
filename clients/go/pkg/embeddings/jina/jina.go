package jina

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	chttp "github.com/chroma-core/chroma/clients/go/pkg/commons/http"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

type EmbeddingType string

type TaskType string

const (
	EmbeddingTypeFloat     EmbeddingType             = "float"
	DefaultBaseAPIEndpoint                           = "https://api.jina.ai/v1/embeddings"
	DefaultEmbeddingModel  embeddings.EmbeddingModel = "jina-embeddings-v3"
	APIKeyEnvVar                                     = "JINA_API_KEY"

	TaskRetrievalQuery   TaskType = "retrieval.query"
	TaskRetrievalPassage TaskType = "retrieval.passage"
	TaskClassification   TaskType = "classification"
	TaskTextMatching     TaskType = "text-matching"
	TaskSeparation       TaskType = "separation"
)

type EmbeddingRequest struct {
	Model         string              `json:"model"`
	Normalized    bool                `json:"normalized,omitempty"`
	EmbeddingType EmbeddingType       `json:"embedding_type,omitempty"`
	Input         []map[string]string `json:"input"`
	Task          TaskType            `json:"task,omitempty"`
	LateChunking  bool                `json:"late_chunking,omitempty"`
}

type EmbeddingResponse struct {
	Model  string `json:"model"`
	Object string `json:"object"`
	Usage  struct {
		TotalTokens  int `json:"total_tokens"`
		PromptTokens int `json:"prompt_tokens"`
	}
	Data []struct {
		Object    string    `json:"object"`
		Index     int       `json:"index"`
		Embedding []float32 `json:"embedding"` // TODO what about other embedding types - see cohere for example
	}
}

var _ embeddings.EmbeddingFunction = (*JinaEmbeddingFunction)(nil)

func getDefaults() *JinaEmbeddingFunction {
	return &JinaEmbeddingFunction{
		httpClient:        http.DefaultClient,
		defaultModel:      DefaultEmbeddingModel,
		embeddingEndpoint: DefaultBaseAPIEndpoint,
		normalized:        true,
		embeddingType:     EmbeddingTypeFloat,
	}
}

type JinaEmbeddingFunction struct {
	httpClient        *http.Client
	APIKey            embeddings.Secret `json:"-" validate:"required"`
	apiKeyEnvVar      string
	defaultModel      embeddings.EmbeddingModel
	embeddingEndpoint string
	normalized        bool
	embeddingType     EmbeddingType
	task              TaskType
	lateChunking      bool
	insecure          bool
}

func validate(ef *JinaEmbeddingFunction) error {
	if err := embeddings.NewValidator().Struct(ef); err != nil {
		return err
	}
	parsed, err := url.Parse(ef.embeddingEndpoint)
	if err != nil {
		return errors.Wrap(err, "invalid base URL")
	}
	if !ef.insecure && !strings.EqualFold(parsed.Scheme, "https") {
		return errors.New("base URL must use HTTPS scheme for secure API key transmission; use WithInsecure() to override")
	}
	return nil
}

func NewJinaEmbeddingFunction(opts ...Option) (*JinaEmbeddingFunction, error) {
	ef := getDefaults()
	for _, opt := range opts {
		err := opt(ef)
		if err != nil {
			return nil, err
		}
	}
	if err := validate(ef); err != nil {
		return nil, errors.Wrap(err, "failed to validate Jina embedding function options")
	}
	return ef, nil
}

func (e *JinaEmbeddingFunction) sendRequest(ctx context.Context, req *EmbeddingRequest) (*EmbeddingResponse, error) {
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal embedding request body")
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, e.embeddingEndpoint, bytes.NewBuffer(payload))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create embedding request")
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	httpReq.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)
	httpReq.Header.Set("Authorization", "Bearer "+e.APIKey.Value())

	resp, err := e.httpClient.Do(httpReq)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to send embedding request")
	}
	defer resp.Body.Close()

	respData, err := chttp.ReadLimitedBody(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read response body")
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected response %v: %s", resp.Status, string(respData))
	}
	var response *EmbeddingResponse
	if err := json.Unmarshal(respData, &response); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal embedding response")
	}

	return response, nil
}

func (e *JinaEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) == 0 {
		return nil, nil
	}
	var Input = make([]map[string]string, len(documents))

	for i, doc := range documents {
		Input[i] = map[string]string{
			"text": doc,
		}
	}
	task := e.task
	if task == "" {
		task = TaskRetrievalPassage
	}
	req := &EmbeddingRequest{
		Model:         string(e.defaultModel),
		Input:         Input,
		Task:          task,
		Normalized:    e.normalized,
		EmbeddingType: e.embeddingType,
		LateChunking:  e.lateChunking,
	}
	response, err := e.sendRequest(ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to embed documents")
	}
	if len(response.Data) == 0 {
		return nil, errors.New("empty embedding response from Jina API")
	}
	if len(response.Data) != len(documents) {
		return nil, errors.Errorf("embedding count mismatch: got %d, expected %d", len(response.Data), len(documents))
	}
	embs := make([]embeddings.Embedding, len(documents))
	for _, data := range response.Data {
		if data.Index < 0 || data.Index >= len(documents) {
			return nil, errors.Errorf("invalid embedding index %d for %d documents", data.Index, len(documents))
		}
		embs[data.Index] = embeddings.NewEmbeddingFromFloat32(data.Embedding)
	}
	return embs, nil
}

func (e *JinaEmbeddingFunction) EmbedQuery(ctx context.Context, document string) (embeddings.Embedding, error) {
	var Input = make([]map[string]string, 1)

	Input[0] = map[string]string{
		"text": document,
	}
	task := e.task
	if task == "" {
		task = TaskRetrievalQuery
	}
	req := &EmbeddingRequest{
		Model:         string(e.defaultModel),
		Input:         Input,
		Task:          task,
		Normalized:    e.normalized,
		EmbeddingType: e.embeddingType,
		LateChunking:  e.lateChunking,
	}
	response, err := e.sendRequest(ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to embed query")
	}
	if len(response.Data) == 0 {
		return nil, errors.New("empty embedding response from Jina API")
	}
	return embeddings.NewEmbeddingFromFloat32(response.Data[0].Embedding), nil
}

func (e *JinaEmbeddingFunction) Name() string {
	return "jina"
}

func (e *JinaEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"model_name":      string(e.defaultModel),
		"api_key_env_var": envVar,
	}
	if e.insecure {
		cfg["insecure"] = true
	}
	if e.embeddingEndpoint != "" {
		cfg["base_url"] = e.embeddingEndpoint
	}
	if e.lateChunking {
		cfg["late_chunking"] = true
	}
	if e.task != "" {
		cfg["task"] = string(e.task)
	}
	if !e.normalized {
		cfg["normalized"] = false
	}
	if e.embeddingType != "" && e.embeddingType != EmbeddingTypeFloat {
		cfg["embedding_type"] = string(e.embeddingType)
	}
	return cfg
}

func (e *JinaEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *JinaEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewJinaEmbeddingFunctionFromConfig creates a Jina embedding function from a config map.
// Uses schema-compliant field names: api_key_env_var, model_name, base_url, insecure,
// late_chunking, task, normalized, embedding_type.
func NewJinaEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*JinaEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar)}
	if model, ok := cfg["model_name"].(string); ok && model != "" {
		opts = append(opts, WithModel(embeddings.EmbeddingModel(model)))
	}
	if baseURL, ok := cfg["base_url"].(string); ok && baseURL != "" {
		opts = append(opts, WithEmbeddingEndpoint(baseURL))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("Jina")
		opts = append(opts, WithInsecure())
	}
	if lateChunking, ok := cfg["late_chunking"].(bool); ok && lateChunking {
		opts = append(opts, WithLateChunking(true))
	}
	if task, ok := cfg["task"].(string); ok && task != "" {
		opts = append(opts, WithTask(TaskType(task)))
	}
	if normalized, ok := cfg["normalized"].(bool); ok {
		opts = append(opts, WithNormalized(normalized))
	}
	if embeddingType, ok := cfg["embedding_type"].(string); ok && embeddingType != "" {
		opts = append(opts, WithEmbeddingType(EmbeddingType(embeddingType)))
	}
	return NewJinaEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("jina", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewJinaEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
