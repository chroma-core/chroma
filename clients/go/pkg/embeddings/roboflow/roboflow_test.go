//go:build ef

package roboflow

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/joho/godotenv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

// testImageURL is a stable public image endpoint used for testing.
const testImageURL = "https://httpbin.org/image/png"

// skipIfImageURLUnavailable checks if the test image URL is accessible and skips the test if not.
func skipIfImageURLUnavailable(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Head(testImageURL)
	if err != nil {
		t.Skipf("Skipping: test image URL unavailable: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Skipf("Skipping: test image URL returned status %d", resp.StatusCode)
	}
}

func TestRoboflowEmbeddingFunction(t *testing.T) {
	apiKey := os.Getenv("ROBOFLOW_API_KEY")
	if apiKey == "" {
		err := godotenv.Load("../../../.env")
		if err != nil {
			assert.Failf(t, "Error loading .env file", "%s", err)
		}
		apiKey = os.Getenv("ROBOFLOW_API_KEY")
	}

	t.Run("Test text embedding with defaults", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey(apiKey))
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
			"Document 2 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Greater(t, resp[0].Len(), 0)
	})

	t.Run("Test text embedding with env API key", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		ef, err := NewRoboflowEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		documents := []string{
			"Document 1 content here",
		}
		resp, err := ef.EmbedDocuments(context.Background(), documents)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Greater(t, resp[0].Len(), 0)
	})

	t.Run("Test EmbedQuery", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey(apiKey))
		require.NoError(t, err)
		resp, err := ef.EmbedQuery(context.Background(), "What is the meaning of life?")
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Greater(t, resp.Len(), 0)
	})

	t.Run("Test image embedding from URL", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		skipIfImageURLUnavailable(t)
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey(apiKey))
		require.NoError(t, err)

		image := embeddings.NewImageInputFromURL(testImageURL)
		resp, err := ef.EmbedImage(context.Background(), image)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Greater(t, resp.Len(), 0)
		t.Logf("Image embedding length: %d", resp.Len())
	})

	t.Run("Test image embedding from file", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey(apiKey))
		require.NoError(t, err)

		// Use the test image file in the same directory
		image := embeddings.NewImageInputFromFile("img.png")
		resp, err := ef.EmbedImage(context.Background(), image)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Greater(t, resp.Len(), 0)
		t.Logf("Image embedding from file length: %d", resp.Len())
	})

	t.Run("Test EmbedImages batch", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		skipIfImageURLUnavailable(t)
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey(apiKey))
		require.NoError(t, err)

		images := []embeddings.ImageInput{
			embeddings.NewImageInputFromURL(testImageURL),
			embeddings.NewImageInputFromURL(testImageURL),
		}
		resp, err := ef.EmbedImages(context.Background(), images)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 2)
		require.Greater(t, resp[0].Len(), 0)
		require.Greater(t, resp[1].Len(), 0)
	})

	t.Run("Test missing API key", func(t *testing.T) {
		_, err := NewRoboflowEmbeddingFunction()
		require.Error(t, err)
		require.Contains(t, err.Error(), "'APIKey' failed on the 'required'")
	})

	t.Run("Test HTTP endpoint rejected without WithInsecure", func(t *testing.T) {
		_, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"), WithBaseURL("http://example.com"))
		require.Error(t, err)
		require.Contains(t, err.Error(), "base URL must use HTTPS")
	})

	t.Run("Test HTTP endpoint accepted with WithInsecure", func(t *testing.T) {
		_, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"), WithBaseURL("http://example.com"), WithInsecure())
		require.NoError(t, err)
	})

	t.Run("Test HTTPS endpoint accepted", func(t *testing.T) {
		_, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"), WithBaseURL("https://example.com"))
		require.NoError(t, err)
	})

	t.Run("Test GetConfig default", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		ef, err := NewRoboflowEmbeddingFunction(WithEnvAPIKey())
		require.NoError(t, err)
		cfg := ef.GetConfig()
		require.Equal(t, "ROBOFLOW_API_KEY", cfg["api_key_env_var"])
		require.Equal(t, DefaultBaseURL, cfg["api_url"])
	})

	t.Run("Test GetConfig with custom base URL", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"), WithBaseURL("https://custom.api.com"), WithInsecure())
		require.NoError(t, err)
		cfg := ef.GetConfig()
		require.Equal(t, "https://custom.api.com", cfg["api_url"])
		require.Equal(t, true, cfg["insecure"])
	})

	t.Run("Test default CLIP version", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"))
		require.NoError(t, err)
		cfg := ef.GetConfig()
		require.Equal(t, string(DefaultCLIPVersion), cfg["clip_version"])
	})

	t.Run("Test custom CLIP version", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"), WithCLIPVersion(CLIPVersionViTL14))
		require.NoError(t, err)
		cfg := ef.GetConfig()
		require.Equal(t, string(CLIPVersionViTL14), cfg["clip_version"])
	})

	t.Run("Test empty CLIP version rejected", func(t *testing.T) {
		_, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"), WithCLIPVersion(""))
		require.Error(t, err)
		require.Contains(t, err.Error(), "CLIP version cannot be empty")
	})

	t.Run("Test Name returns roboflow", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"))
		require.NoError(t, err)
		require.Equal(t, "roboflow", ef.Name())
	})

	t.Run("Test DefaultSpace returns COSINE", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"))
		require.NoError(t, err)
		require.Equal(t, embeddings.COSINE, ef.DefaultSpace())
	})

	t.Run("Test SupportedSpaces", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"))
		require.NoError(t, err)
		spaces := ef.SupportedSpaces()
		require.Contains(t, spaces, embeddings.COSINE)
		require.Contains(t, spaces, embeddings.L2)
		require.Contains(t, spaces, embeddings.IP)
	})

	t.Run("Test empty documents returns nil", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"))
		require.NoError(t, err)
		resp, err := ef.EmbedDocuments(context.Background(), []string{})
		require.NoError(t, err)
		require.Nil(t, resp)
	})

	t.Run("Test empty images returns nil", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"))
		require.NoError(t, err)
		resp, err := ef.EmbedImages(context.Background(), []embeddings.ImageInput{})
		require.NoError(t, err)
		require.Nil(t, resp)
	})
}

func TestImageInput(t *testing.T) {
	t.Run("Test Validate with base64", func(t *testing.T) {
		img := embeddings.NewImageInputFromBase64("abc123")
		err := img.Validate()
		require.NoError(t, err)
		require.Equal(t, embeddings.ImageInputTypeBase64, img.Type())
	})

	t.Run("Test Validate with URL", func(t *testing.T) {
		img := embeddings.NewImageInputFromURL("https://example.com/image.png")
		err := img.Validate()
		require.NoError(t, err)
		require.Equal(t, embeddings.ImageInputTypeURL, img.Type())
	})

	t.Run("Test Validate with file path", func(t *testing.T) {
		img := embeddings.NewImageInputFromFile("/path/to/image.png")
		err := img.Validate()
		require.NoError(t, err)
		require.Equal(t, embeddings.ImageInputTypeFilePath, img.Type())
	})

	t.Run("Test Validate with no input", func(t *testing.T) {
		img := embeddings.ImageInput{}
		err := img.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "must have exactly one")
	})

	t.Run("Test Validate with multiple inputs", func(t *testing.T) {
		img := embeddings.ImageInput{
			Base64: "abc123",
			URL:    "https://example.com/image.png",
		}
		err := img.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "got multiple")
	})

	t.Run("Test ToBase64 returns base64 directly", func(t *testing.T) {
		img := embeddings.NewImageInputFromBase64("abc123")
		result, err := img.ToBase64(context.Background())
		require.NoError(t, err)
		require.Equal(t, "abc123", result)
	})

	t.Run("Test ToBase64 with invalid input", func(t *testing.T) {
		img := embeddings.ImageInput{}
		_, err := img.ToBase64(context.Background())
		require.Error(t, err)
	})

	t.Run("Test ToBase64 with URL returns error", func(t *testing.T) {
		// URL inputs should be passed directly to the API, not converted to base64
		img := embeddings.NewImageInputFromURL("http://example.com/image.png")
		_, err := img.ToBase64(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "URL inputs should be passed directly")
	})

	t.Run("Test ToBase64 with unsupported file extension", func(t *testing.T) {
		img := embeddings.NewImageInputFromFile("/path/to/file.txt")
		_, err := img.ToBase64(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported image file extension")
	})

	t.Run("Test ToBase64 with valid extensions", func(t *testing.T) {
		validExtensions := []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".tif"}
		for _, ext := range validExtensions {
			img := embeddings.NewImageInputFromFile("/nonexistent/file" + ext)
			_, err := img.ToBase64(context.Background())
			// Should fail on file not found, not extension validation
			require.Error(t, err)
			require.NotContains(t, err.Error(), "unsupported image file extension")
		}
	})
}

func TestRoboflowFromConfig(t *testing.T) {
	apiKey := os.Getenv("ROBOFLOW_API_KEY")

	t.Run("Test config missing api_key_env_var", func(t *testing.T) {
		cfg := embeddings.EmbeddingFunctionConfig{
			"api_url": DefaultBaseURL,
		}
		_, err := NewRoboflowEmbeddingFunctionFromConfig(cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "api_key_env_var is required")
	})

	t.Run("Test config missing api_url", func(t *testing.T) {
		cfg := embeddings.EmbeddingFunctionConfig{
			"api_key_env_var": "ROBOFLOW_API_KEY",
		}
		_, err := NewRoboflowEmbeddingFunctionFromConfig(cfg)
		require.Error(t, err)
		require.Contains(t, err.Error(), "api_url is required")
	})

	t.Run("Test config with all options", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		cfg := embeddings.EmbeddingFunctionConfig{
			"api_key_env_var": "ROBOFLOW_API_KEY",
			"api_url":         "https://custom.api.com",
			"clip_version":    string(CLIPVersionViTL14),
			"insecure":        true,
		}
		ef, err := NewRoboflowEmbeddingFunctionFromConfig(cfg)
		require.NoError(t, err)
		require.Equal(t, "https://custom.api.com", ef.baseURL)
		require.Equal(t, CLIPVersionViTL14, ef.clipVersion)
		require.True(t, ef.insecure)
	})

	t.Run("Test config roundtrip", func(t *testing.T) {
		if apiKey == "" {
			t.Skip("ROBOFLOW_API_KEY not set")
		}
		ef1, err := NewRoboflowEmbeddingFunction(WithEnvAPIKey(), WithBaseURL("https://custom.api.com"), WithCLIPVersion(CLIPVersionViTL14))
		require.NoError(t, err)

		cfg := ef1.GetConfig()
		require.Equal(t, "ROBOFLOW_API_KEY", cfg["api_key_env_var"])
		require.Equal(t, "https://custom.api.com", cfg["api_url"])
		require.Equal(t, string(CLIPVersionViTL14), cfg["clip_version"])

		ef2, err := NewRoboflowEmbeddingFunctionFromConfig(cfg)
		require.NoError(t, err)
		require.Equal(t, ef1.baseURL, ef2.baseURL)
		require.Equal(t, ef1.clipVersion, ef2.clipVersion)
	})
}

func TestMultimodalInterface(t *testing.T) {
	t.Run("Test implements MultimodalEmbeddingFunction", func(t *testing.T) {
		ef, err := NewRoboflowEmbeddingFunction(WithAPIKey("test-key"))
		require.NoError(t, err)

		var _ embeddings.MultimodalEmbeddingFunction = ef
		var _ embeddings.EmbeddingFunction = ef
	})
}
