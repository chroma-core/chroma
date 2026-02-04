// Package roboflow provides a Roboflow CLIP embedding function for text and images.
//
// Roboflow Inference provides a hosted API for generating CLIP embeddings that map
// text and images into the same embedding space, enabling cross-modal similarity search.
//
// API Documentation: https://inference.roboflow.com/foundation/clip/
// Getting Started: https://inference.roboflow.com/start/overview/
// OpenAPI Spec: https://inference.roboflow.com/openapi.json
package roboflow

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
	DefaultBaseURL     = "https://infer.roboflow.com"
	APIKeyEnvVar       = "ROBOFLOW_API_KEY"
	DefaultHTTPTimeout = 60 * time.Second
)

// CLIPVersion represents the available CLIP model versions.
// See https://inference.roboflow.com/foundation/clip/ for details on each model variant.
type CLIPVersion string

const (
	CLIPVersionViTB16      CLIPVersion = "ViT-B-16"
	CLIPVersionViTB32      CLIPVersion = "ViT-B-32"
	CLIPVersionViTL14      CLIPVersion = "ViT-L-14"
	CLIPVersionViTL14336px CLIPVersion = "ViT-L-14-336px"
	CLIPVersionRN50        CLIPVersion = "RN50"
	CLIPVersionRN101       CLIPVersion = "RN101"
	CLIPVersionRN50x4      CLIPVersion = "RN50x4"
	CLIPVersionRN50x16     CLIPVersion = "RN50x16"
	CLIPVersionRN50x64     CLIPVersion = "RN50x64"
)

// DefaultCLIPVersion is the default CLIP model version.
const DefaultCLIPVersion = CLIPVersionViTB16

type textEmbeddingRequest struct {
	Text string `json:"text"`
}

type imageEmbeddingRequest struct {
	Image imageData `json:"image"`
}

type imageData struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type embeddingResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
}

var (
	_ embeddings.EmbeddingFunction           = (*RoboflowEmbeddingFunction)(nil)
	_ embeddings.MultimodalEmbeddingFunction = (*RoboflowEmbeddingFunction)(nil)
)

func getDefaults() *RoboflowEmbeddingFunction {
	return &RoboflowEmbeddingFunction{
		httpClient:  &http.Client{Timeout: DefaultHTTPTimeout},
		baseURL:     DefaultBaseURL,
		clipVersion: DefaultCLIPVersion,
	}
}

// RoboflowEmbeddingFunction generates CLIP embeddings using the Roboflow Inference API.
// It supports both text and image inputs, producing embeddings in a shared vector space
// that enables cross-modal similarity search (e.g., searching images with text queries).
//
// For URL image inputs, the URL is passed directly to the Roboflow API for fetching.
// For file inputs, the image is read locally and sent as base64.
type RoboflowEmbeddingFunction struct {
	httpClient   *http.Client
	APIKey       embeddings.Secret `json:"-" validate:"required"`
	apiKeyEnvVar string
	baseURL      string
	clipVersion  CLIPVersion
	insecure     bool
}

func validate(ef *RoboflowEmbeddingFunction) error {
	if err := embeddings.NewValidator().Struct(ef); err != nil {
		return err
	}
	parsed, err := url.Parse(ef.baseURL)
	if err != nil {
		return errors.Wrap(err, "invalid base URL")
	}
	if !ef.insecure && !strings.EqualFold(parsed.Scheme, "https") {
		return errors.New("base URL must use HTTPS scheme for secure API key transmission; use WithInsecure() to override")
	}
	return nil
}

// NewRoboflowEmbeddingFunction creates a new Roboflow CLIP embedding function.
// Requires an API key via WithAPIKey, WithEnvAPIKey, or WithAPIKeyFromEnvVar.
//
// Example:
//
//	ef, err := NewRoboflowEmbeddingFunction(
//	    WithEnvAPIKey(),
//	    WithCLIPVersion(CLIPVersionViTL14),
//	)
func NewRoboflowEmbeddingFunction(opts ...Option) (*RoboflowEmbeddingFunction, error) {
	ef := getDefaults()
	for _, opt := range opts {
		if err := opt(ef); err != nil {
			return nil, err
		}
	}
	if err := validate(ef); err != nil {
		return nil, errors.Wrap(err, "failed to validate Roboflow embedding function options")
	}
	return ef, nil
}

func (e *RoboflowEmbeddingFunction) sendTextRequest(ctx context.Context, text string) (*embeddingResponse, error) {
	endpoint := e.baseURL + "/clip/embed_text?api_key=" + e.APIKey.Value() + "&clip_version_id=" + string(e.clipVersion)

	payload, err := json.Marshal(textEmbeddingRequest{Text: text})
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal text embedding request")
	}

	return e.doRequest(ctx, endpoint, payload)
}

func (e *RoboflowEmbeddingFunction) sendImageRequest(ctx context.Context, imageType, imageValue string) (*embeddingResponse, error) {
	endpoint := e.baseURL + "/clip/embed_image?api_key=" + e.APIKey.Value() + "&clip_version_id=" + string(e.clipVersion)

	payload, err := json.Marshal(imageEmbeddingRequest{
		Image: imageData{
			Type:  imageType,
			Value: imageValue,
		},
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal image embedding request")
	}

	return e.doRequest(ctx, endpoint, payload)
}

// maxAPIResponseSize limits API response body reads to prevent memory exhaustion.
const maxAPIResponseSize = 10 * 1024 * 1024 // 10 MB

func (e *RoboflowEmbeddingFunction) doRequest(ctx context.Context, endpoint string, payload []byte) (*embeddingResponse, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(payload))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create embedding request")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", chttp.ChromaGoClientUserAgent)

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to send embedding request")
	}
	defer resp.Body.Close()

	limitedReader := io.LimitReader(resp.Body, maxAPIResponseSize+1)
	respData, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read response body")
	}
	if len(respData) > maxAPIResponseSize {
		return nil, errors.Errorf("response exceeds maximum size of %d bytes", maxAPIResponseSize)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.Errorf("unexpected response %v: %s", resp.Status, string(respData))
	}

	var response embeddingResponse
	if err := json.Unmarshal(respData, &response); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal embedding response")
	}

	return &response, nil
}

func (e *RoboflowEmbeddingFunction) EmbedDocuments(ctx context.Context, documents []string) ([]embeddings.Embedding, error) {
	if len(documents) == 0 {
		return nil, nil
	}

	result := make([]embeddings.Embedding, len(documents))
	for i, doc := range documents {
		emb, err := e.EmbedQuery(ctx, doc)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to embed document %d", i)
		}
		result[i] = emb
	}
	return result, nil
}

func (e *RoboflowEmbeddingFunction) EmbedQuery(ctx context.Context, text string) (embeddings.Embedding, error) {
	response, err := e.sendTextRequest(ctx, text)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed text")
	}
	if len(response.Embeddings) == 0 {
		return nil, errors.New("empty embedding response from Roboflow API")
	}
	return embeddings.NewEmbeddingFromFloat32(response.Embeddings[0]), nil
}

func (e *RoboflowEmbeddingFunction) EmbedImages(ctx context.Context, images []embeddings.ImageInput) ([]embeddings.Embedding, error) {
	if len(images) == 0 {
		return nil, nil
	}

	result := make([]embeddings.Embedding, len(images))
	for i, img := range images {
		emb, err := e.EmbedImage(ctx, img)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to embed image %d", i)
		}
		result[i] = emb
	}
	return result, nil
}

func (e *RoboflowEmbeddingFunction) EmbedImage(ctx context.Context, image embeddings.ImageInput) (embeddings.Embedding, error) {
	if err := image.Validate(); err != nil {
		return nil, err
	}

	var imageType, imageValue string

	switch image.Type() {
	case embeddings.ImageInputTypeURL:
		// Pass URL directly to Roboflow API - let them handle fetching
		imageType = "url"
		imageValue = image.URL
	case embeddings.ImageInputTypeBase64:
		imageType = "base64"
		imageValue = image.Base64
	case embeddings.ImageInputTypeFilePath:
		// Read file and convert to base64
		base64Data, err := image.ToBase64(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read image file")
		}
		imageType = "base64"
		imageValue = base64Data
	default:
		return nil, errors.New("unknown image input type")
	}

	response, err := e.sendImageRequest(ctx, imageType, imageValue)
	if err != nil {
		return nil, errors.Wrap(err, "failed to embed image")
	}
	if len(response.Embeddings) == 0 {
		return nil, errors.New("empty embedding response from Roboflow API")
	}
	return embeddings.NewEmbeddingFromFloat32(response.Embeddings[0]), nil
}

func (e *RoboflowEmbeddingFunction) Name() string {
	return "roboflow"
}

func (e *RoboflowEmbeddingFunction) GetConfig() embeddings.EmbeddingFunctionConfig {
	envVar := e.apiKeyEnvVar
	if envVar == "" {
		envVar = APIKeyEnvVar
	}
	apiURL := e.baseURL
	if apiURL == "" {
		apiURL = DefaultBaseURL
	}
	clipVersion := string(e.clipVersion)
	if clipVersion == "" {
		clipVersion = string(DefaultCLIPVersion)
	}
	cfg := embeddings.EmbeddingFunctionConfig{
		"api_key_env_var": envVar,
		"api_url":         apiURL,
		"clip_version":    clipVersion,
	}
	if e.insecure {
		cfg["insecure"] = true
	}
	return cfg
}

func (e *RoboflowEmbeddingFunction) DefaultSpace() embeddings.DistanceMetric {
	return embeddings.COSINE
}

func (e *RoboflowEmbeddingFunction) SupportedSpaces() []embeddings.DistanceMetric {
	return []embeddings.DistanceMetric{embeddings.COSINE, embeddings.L2, embeddings.IP}
}

// NewRoboflowEmbeddingFunctionFromConfig creates a Roboflow embedding function from a config map.
// Uses schema-compliant field names: api_key_env_var, api_url, clip_version, insecure.
func NewRoboflowEmbeddingFunctionFromConfig(cfg embeddings.EmbeddingFunctionConfig) (*RoboflowEmbeddingFunction, error) {
	envVar, ok := cfg["api_key_env_var"].(string)
	if !ok || envVar == "" {
		return nil, errors.New("api_key_env_var is required in config")
	}
	apiURL, ok := cfg["api_url"].(string)
	if !ok || apiURL == "" {
		return nil, errors.New("api_url is required in config")
	}
	opts := []Option{WithAPIKeyFromEnvVar(envVar), WithBaseURL(apiURL)}
	if clipVersion, ok := cfg["clip_version"].(string); ok && clipVersion != "" {
		opts = append(opts, WithCLIPVersion(CLIPVersion(clipVersion)))
	}
	if insecure, ok := cfg["insecure"].(bool); ok && insecure {
		opts = append(opts, WithInsecure())
	} else if embeddings.AllowInsecureFromEnv() {
		embeddings.LogInsecureEnvVarWarning("Roboflow")
		opts = append(opts, WithInsecure())
	}
	return NewRoboflowEmbeddingFunction(opts...)
}

func init() {
	if err := embeddings.RegisterDense("roboflow", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.EmbeddingFunction, error) {
		return NewRoboflowEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}

	if err := embeddings.RegisterMultimodal("roboflow", func(cfg embeddings.EmbeddingFunctionConfig) (embeddings.MultimodalEmbeddingFunction, error) {
		return NewRoboflowEmbeddingFunctionFromConfig(cfg)
	}); err != nil {
		panic(err)
	}
}
