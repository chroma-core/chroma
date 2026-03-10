package logger

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNoopLogger(t *testing.T) {
	logger := NewNoopLogger()

	// These should all be no-ops and not panic
	logger.Debug("debug message")
	logger.Info("info message")
	logger.Warn("warn message")
	logger.Error("error message")

	logger.DebugWithContext(context.Background(), "debug with context")
	logger.InfoWithContext(context.Background(), "info with context")
	logger.WarnWithContext(context.Background(), "warn with context")
	logger.ErrorWithContext(context.Background(), "error with context")

	// With should return the same logger
	withLogger := logger.With(String("key", "value"))
	assert.Equal(t, logger, withLogger)

	// IsDebugEnabled should always return false
	assert.False(t, logger.IsDebugEnabled())
}

func TestZapLogger(t *testing.T) {
	// Create a test logger that writes to a buffer
	var buf bytes.Buffer
	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"stdout"}
	config.ErrorOutputPaths = []string{"stderr"}
	config.Encoding = "json"
	config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)

	// Use a custom encoder to write to our buffer
	encoder := zapcore.NewJSONEncoder(config.EncoderConfig)
	writer := zapcore.AddSync(&buf)
	core := zapcore.NewCore(encoder, writer, config.Level)
	zapLogger := zap.New(core)

	logger := NewZapLogger(zapLogger)

	t.Run("Basic logging", func(t *testing.T) {
		buf.Reset()
		logger.Debug("debug message", String("key", "value"))
		assert.Contains(t, buf.String(), "debug message")
		assert.Contains(t, buf.String(), "key")
		assert.Contains(t, buf.String(), "value")

		buf.Reset()
		logger.Info("info message", Int("count", 42))
		assert.Contains(t, buf.String(), "info message")
		assert.Contains(t, buf.String(), "count")
		assert.Contains(t, buf.String(), "42")

		buf.Reset()
		logger.Warn("warn message", Bool("flag", true))
		assert.Contains(t, buf.String(), "warn message")
		assert.Contains(t, buf.String(), "flag")
		assert.Contains(t, buf.String(), "true")

		buf.Reset()
		logger.Error("error message", ErrorField("error", assert.AnError))
		assert.Contains(t, buf.String(), "error message")
		assert.Contains(t, buf.String(), "error")
		assert.Contains(t, buf.String(), assert.AnError.Error())
	})

	t.Run("Context logging", func(t *testing.T) {
		ctx := context.Background()

		buf.Reset()
		logger.DebugWithContext(ctx, "debug with context", String("key", "value"))
		assert.Contains(t, buf.String(), "debug with context")

		buf.Reset()
		logger.InfoWithContext(ctx, "info with context", String("key", "value"))
		assert.Contains(t, buf.String(), "info with context")

		buf.Reset()
		logger.WarnWithContext(ctx, "warn with context", String("key", "value"))
		assert.Contains(t, buf.String(), "warn with context")

		buf.Reset()
		logger.ErrorWithContext(ctx, "error with context", String("key", "value"))
		assert.Contains(t, buf.String(), "error with context")
	})

	t.Run("With fields", func(t *testing.T) {
		buf.Reset()
		withLogger := logger.With(String("request_id", "123"))
		withLogger.Info("message with fields")
		assert.Contains(t, buf.String(), "message with fields")
		assert.Contains(t, buf.String(), "request_id")
		assert.Contains(t, buf.String(), "123")

		// Original logger should not have the fields
		buf.Reset()
		logger.Info("message without fields")
		assert.NotContains(t, buf.String(), "request_id")
	})

	t.Run("IsDebugEnabled", func(t *testing.T) {
		assert.True(t, logger.IsDebugEnabled())

		// Create a logger with info level
		infoConfig := zap.NewProductionConfig()
		infoConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
		infoCore := zapcore.NewCore(encoder, writer, infoConfig.Level)
		infoZapLogger := zap.New(infoCore)
		infoLogger := NewZapLogger(infoZapLogger)
		assert.False(t, infoLogger.IsDebugEnabled())
	})

	t.Run("Error field with empty key", func(t *testing.T) {
		buf.Reset()
		// Test error field with empty key (should use "_error" as default)
		logger.Error("error with empty key", Field{Key: "", Value: assert.AnError})
		output := buf.String()
		assert.Contains(t, output, "_error")
		assert.Contains(t, output, assert.AnError.Error())

		buf.Reset()
		// Test ErrorField with empty key
		logger.Error("error with ErrorField empty key", ErrorField("", assert.AnError))
		output = buf.String()
		assert.Contains(t, output, "_error")
		assert.Contains(t, output, assert.AnError.Error())

		buf.Reset()
		// Test normal error field with key
		logger.Error("error with key", ErrorField("custom_error", assert.AnError))
		output = buf.String()
		assert.Contains(t, output, "custom_error")
		assert.Contains(t, output, assert.AnError.Error())

		buf.Reset()
		// Test that nil errors don't panic - they get handled by the default case
		var nilErr error = nil
		assert.NotPanics(t, func() {
			logger.Error("nil error test", Field{Key: "nil_error", Value: nilErr})
		})

		// Test with concrete error to ensure normal case works
		buf.Reset()
		err := assert.AnError
		logger.Error("concrete error test", Field{Key: "concrete_error", Value: err})
		output = buf.String()
		assert.Contains(t, output, "concrete_error")
		assert.Contains(t, output, assert.AnError.Error())
	})
}

func TestFieldHelpers(t *testing.T) {
	t.Run("String field", func(t *testing.T) {
		field := String("key", "value")
		assert.Equal(t, "key", field.Key)
		assert.Equal(t, "value", field.Value)
	})

	t.Run("Int field", func(t *testing.T) {
		field := Int("key", 42)
		assert.Equal(t, "key", field.Key)
		assert.Equal(t, 42, field.Value)
	})

	t.Run("Bool field", func(t *testing.T) {
		field := Bool("key", true)
		assert.Equal(t, "key", field.Key)
		assert.Equal(t, true, field.Value)
	})

	t.Run("Error field", func(t *testing.T) {
		field := ErrorField("key", assert.AnError)
		assert.Equal(t, "key", field.Key)
		assert.Equal(t, assert.AnError, field.Value)
	})

	t.Run("Any field", func(t *testing.T) {
		complexValue := map[string]interface{}{
			"nested": "value",
		}
		field := Any("key", complexValue)
		assert.Equal(t, "key", field.Key)
		assert.Equal(t, complexValue, field.Value)
	})
}

func TestDefaultLoggers(t *testing.T) {
	t.Run("NewDefaultZapLogger", func(t *testing.T) {
		logger, err := NewDefaultZapLogger()
		require.NoError(t, err)
		assert.NotNil(t, logger)
		// Production logger has info level by default
		assert.False(t, logger.IsDebugEnabled())
	})

	t.Run("NewDevelopmentZapLogger", func(t *testing.T) {
		logger, err := NewDevelopmentZapLogger()
		require.NoError(t, err)
		assert.NotNil(t, logger)
		// Development logger has debug level by default
		assert.True(t, logger.IsDebugEnabled())
	})
}

func TestConvertFields(t *testing.T) {
	// This tests the internal convertFields function indirectly
	var buf bytes.Buffer
	config := zap.NewProductionConfig()
	encoder := zapcore.NewJSONEncoder(config.EncoderConfig)
	writer := zapcore.AddSync(&buf)
	core := zapcore.NewCore(encoder, writer, zapcore.DebugLevel)
	zapLogger := zap.New(core)
	logger := NewZapLogger(zapLogger)

	fields := []Field{
		String("string", "value"),
		Int("int", 42),
		Bool("bool", true),
		Any("float", 3.14),
		ErrorField("error", assert.AnError),
		Any("map", map[string]string{"key": "value"}),
		Any("int32", int32(32)),
		Any("int64", int64(64)),
		Any("uint", uint(10)),
		Any("uint32", uint32(32)),
		Any("uint64", uint64(64)),
		Any("float32", float32(3.14)),
		Any("float64", float64(3.14)),
	}

	buf.Reset()
	logger.Debug("test with various fields", fields...)
	output := buf.String()

	assert.Contains(t, output, "string")
	assert.Contains(t, output, "value")
	assert.Contains(t, output, "int")
	assert.Contains(t, output, "42")
	assert.Contains(t, output, "bool")
	assert.Contains(t, output, "true")
	assert.Contains(t, output, "float")
	assert.Contains(t, output, "error")
}
