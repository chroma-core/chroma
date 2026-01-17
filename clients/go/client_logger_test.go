package chroma

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/chroma-core/chroma/clients/go/pkg/logger"
)

func TestClientWithLogger(t *testing.T) {
	// Create a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"nanosecond heartbeat": 1234567890}`))
	}))
	defer server.Close()

	t.Run("Client with custom logger", func(t *testing.T) {
		// Create a test logger that writes to a buffer
		var buf bytes.Buffer
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
		encoder := zapcore.NewJSONEncoder(config.EncoderConfig)
		writer := zapcore.AddSync(&buf)
		core := zapcore.NewCore(encoder, writer, config.Level)
		zapLogger := zap.New(core)
		testLogger := logger.NewZapLogger(zapLogger)

		// Create client with custom logger
		client, err := NewHTTPClient(
			WithBaseURL(server.URL),
			WithLogger(testLogger),
		)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Make a request
		ctx := context.Background()
		err = client.Heartbeat(ctx)
		require.NoError(t, err)

		// Check that logging occurred
		assert.Contains(t, buf.String(), "HTTP Request")
		assert.Contains(t, buf.String(), "HTTP Response")
	})

	t.Run("Client with debug and automatic logger", func(t *testing.T) {
		// Create client with debug enabled (should create a development logger automatically)
		client, err := NewHTTPClient(
			WithBaseURL(server.URL),
			WithDebug(),
		)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Check that the logger is set
		apiClient, ok := client.(*APIClientV2)
		require.True(t, ok)
		assert.NotNil(t, apiClient.logger)
		assert.True(t, apiClient.logger.IsDebugEnabled())
	})

	t.Run("Client without logger defaults to noop", func(t *testing.T) {
		// Create client without logger or debug
		client, err := NewHTTPClient(
			WithBaseURL(server.URL),
		)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Check that a noop logger is set
		apiClient, ok := client.(*APIClientV2)
		require.True(t, ok)
		assert.NotNil(t, apiClient.logger)
		assert.False(t, apiClient.logger.IsDebugEnabled())
	})

	t.Run("WithLogger validation", func(t *testing.T) {
		// Test that WithLogger rejects nil
		_, err := NewHTTPClient(
			WithBaseURL(server.URL),
			WithLogger(nil),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "logger cannot be nil")
	})
}

func TestCloudClientWithLogger(t *testing.T) {
	// Set up environment for cloud client
	t.Setenv("CHROMA_TENANT", "test-tenant")
	t.Setenv("CHROMA_DATABASE", "test-database")
	t.Setenv("CHROMA_API_KEY", "test-key")

	t.Run("Cloud client with custom logger", func(t *testing.T) {
		// Create a test logger that writes to a buffer
		var buf bytes.Buffer
		config := zap.NewProductionConfig()
		config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
		encoder := zapcore.NewJSONEncoder(config.EncoderConfig)
		writer := zapcore.AddSync(&buf)
		core := zapcore.NewCore(encoder, writer, config.Level)
		zapLogger := zap.New(core)
		testLogger := logger.NewZapLogger(zapLogger)

		// Create cloud client with custom logger
		client, err := NewCloudClient(
			WithLogger(testLogger),
			WithBaseURL("http://localhost:8000"), // Override cloud URL for testing
		)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Check that the logger is set
		assert.NotNil(t, client.logger)
	})

	t.Run("Cloud client with debug", func(t *testing.T) {
		// Create cloud client with debug
		client, err := NewCloudClient(
			WithDebug(),
			WithBaseURL("http://localhost:8000"), // Override cloud URL for testing
		)
		require.NoError(t, err)
		require.NotNil(t, client)

		// Check that the logger is set and debug is enabled
		assert.NotNil(t, client.logger)
		assert.True(t, client.logger.IsDebugEnabled())
	})
}
