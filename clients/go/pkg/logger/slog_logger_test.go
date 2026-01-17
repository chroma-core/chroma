package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSlogLogger tests the SlogLogger implementation
func TestSlogLogger(t *testing.T) {
	t.Run("NewSlogLogger", func(t *testing.T) {
		buf := &bytes.Buffer{}
		handler := slog.NewJSONHandler(buf, nil)
		logger := slog.New(handler)
		slogLogger := NewSlogLogger(logger)

		assert.NotNil(t, slogLogger)
		assert.NotNil(t, slogLogger.logger)
	})

	t.Run("NewSlogLoggerWithHandler", func(t *testing.T) {
		buf := &bytes.Buffer{}
		handler := slog.NewJSONHandler(buf, nil)
		slogLogger := NewSlogLoggerWithHandler(handler)

		assert.NotNil(t, slogLogger)
		assert.NotNil(t, slogLogger.logger)
	})

	t.Run("NewDefaultSlogLogger", func(t *testing.T) {
		logger, err := NewDefaultSlogLogger()
		assert.NoError(t, err)
		assert.NotNil(t, logger)
		assert.NotNil(t, logger.logger)
	})

	t.Run("NewTextSlogLogger", func(t *testing.T) {
		logger, err := NewTextSlogLogger()
		assert.NoError(t, err)
		assert.NotNil(t, logger)
		assert.NotNil(t, logger.logger)
	})
}

func TestSlogLoggerLevels(t *testing.T) {
	tests := []struct {
		name    string
		logFunc func(*SlogLogger, string, ...Field)
		level   string
		message string
	}{
		{
			name: "Debug",
			logFunc: func(l *SlogLogger, msg string, fields ...Field) {
				l.Debug(msg, fields...)
			},
			level:   "DEBUG",
			message: "debug message",
		},
		{
			name: "Info",
			logFunc: func(l *SlogLogger, msg string, fields ...Field) {
				l.Info(msg, fields...)
			},
			level:   "INFO",
			message: "info message",
		},
		{
			name: "Warn",
			logFunc: func(l *SlogLogger, msg string, fields ...Field) {
				l.Warn(msg, fields...)
			},
			level:   "WARN",
			message: "warn message",
		},
		{
			name: "Error",
			logFunc: func(l *SlogLogger, msg string, fields ...Field) {
				l.Error(msg, fields...)
			},
			level:   "ERROR",
			message: "error message",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			opts := &slog.HandlerOptions{
				Level: slog.LevelDebug, // Enable all levels
			}
			handler := slog.NewJSONHandler(buf, opts)
			logger := NewSlogLoggerWithHandler(handler)

			tt.logFunc(logger, tt.message)

			var logEntry map[string]interface{}
			err := json.Unmarshal(buf.Bytes(), &logEntry)
			require.NoError(t, err)

			assert.Equal(t, tt.level, logEntry["level"])
			assert.Equal(t, tt.message, logEntry["msg"])
		})
	}
}

func TestSlogLoggerWithFields(t *testing.T) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	logger := NewSlogLoggerWithHandler(handler)

	logger.Info("test message",
		String("string_field", "value"),
		Int("int_field", 42),
		Bool("bool_field", true),
		ErrorField("error_field", errors.New("test error")),
		Any("any_field", map[string]string{"key": "value"}),
	)

	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	require.NoError(t, err)

	assert.Equal(t, "test message", logEntry["msg"])
	assert.Equal(t, "value", logEntry["string_field"])
	assert.Equal(t, float64(42), logEntry["int_field"])
	assert.Equal(t, true, logEntry["bool_field"])
	assert.Equal(t, "test error", logEntry["error_field"])
	assert.NotNil(t, logEntry["any_field"])
}

func TestSlogLoggerContextMethods(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		logFunc func(*SlogLogger, context.Context, string, ...Field)
		level   string
		message string
	}{
		{
			name: "DebugWithContext",
			logFunc: func(l *SlogLogger, ctx context.Context, msg string, fields ...Field) {
				l.DebugWithContext(ctx, msg, fields...)
			},
			level:   "DEBUG",
			message: "debug with context",
		},
		{
			name: "InfoWithContext",
			logFunc: func(l *SlogLogger, ctx context.Context, msg string, fields ...Field) {
				l.InfoWithContext(ctx, msg, fields...)
			},
			level:   "INFO",
			message: "info with context",
		},
		{
			name: "WarnWithContext",
			logFunc: func(l *SlogLogger, ctx context.Context, msg string, fields ...Field) {
				l.WarnWithContext(ctx, msg, fields...)
			},
			level:   "WARN",
			message: "warn with context",
		},
		{
			name: "ErrorWithContext",
			logFunc: func(l *SlogLogger, ctx context.Context, msg string, fields ...Field) {
				l.ErrorWithContext(ctx, msg, fields...)
			},
			level:   "ERROR",
			message: "error with context",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			opts := &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}
			handler := slog.NewJSONHandler(buf, opts)
			logger := NewSlogLoggerWithHandler(handler)

			tt.logFunc(logger, ctx, tt.message, String("test_field", "test_value"))

			var logEntry map[string]interface{}
			err := json.Unmarshal(buf.Bytes(), &logEntry)
			require.NoError(t, err)

			assert.Equal(t, tt.level, logEntry["level"])
			assert.Equal(t, tt.message, logEntry["msg"])
			assert.Equal(t, "test_value", logEntry["test_field"])
		})
	}
}

func TestSlogLoggerWith(t *testing.T) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	logger := NewSlogLoggerWithHandler(handler)

	// Create a logger with persistent fields
	childLogger := logger.With(
		String("app", "test-app"),
		Int("version", 1),
	)

	childLogger.Info("message from child logger")

	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	require.NoError(t, err)

	assert.Equal(t, "message from child logger", logEntry["msg"])
	assert.Equal(t, "test-app", logEntry["app"])
	assert.Equal(t, float64(1), logEntry["version"])
}

func TestSlogLoggerIsDebugEnabled(t *testing.T) {
	t.Run("DebugEnabled", func(t *testing.T) {
		buf := &bytes.Buffer{}
		opts := &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}
		handler := slog.NewJSONHandler(buf, opts)
		logger := NewSlogLoggerWithHandler(handler)

		assert.True(t, logger.IsDebugEnabled())
	})

	t.Run("DebugDisabled", func(t *testing.T) {
		buf := &bytes.Buffer{}
		opts := &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}
		handler := slog.NewJSONHandler(buf, opts)
		logger := NewSlogLoggerWithHandler(handler)

		assert.False(t, logger.IsDebugEnabled())
	})
}

func TestSlogLoggerSync(t *testing.T) {
	logger, err := NewDefaultSlogLogger()
	require.NoError(t, err)

	// Sync should not return an error for slog
	err = logger.Sync()
	assert.NoError(t, err)
}

func TestConvertFieldsToAttrs(t *testing.T) {
	tests := []struct {
		name     string
		field    Field
		validate func(t *testing.T, buf *bytes.Buffer)
	}{
		{
			name:  "String field",
			field: String("key", "value"),
			validate: func(t *testing.T, buf *bytes.Buffer) {
				assert.Contains(t, buf.String(), `"key":"value"`)
			},
		},
		{
			name:  "Int field",
			field: Int("count", 42),
			validate: func(t *testing.T, buf *bytes.Buffer) {
				assert.Contains(t, buf.String(), `"count":42`)
			},
		},
		{
			name:  "Bool field",
			field: Bool("enabled", true),
			validate: func(t *testing.T, buf *bytes.Buffer) {
				assert.Contains(t, buf.String(), `"enabled":true`)
			},
		},
		{
			name:  "Error field",
			field: ErrorField("err", errors.New("test error")),
			validate: func(t *testing.T, buf *bytes.Buffer) {
				assert.Contains(t, buf.String(), `"err":"test error"`)
			},
		},
		{
			name:  "Any field",
			field: Any("data", map[string]int{"value": 10}),
			validate: func(t *testing.T, buf *bytes.Buffer) {
				assert.Contains(t, buf.String(), `"data":{"value":10}`)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			handler := slog.NewJSONHandler(buf, nil)
			logger := NewSlogLoggerWithHandler(handler)

			logger.Info("test", tt.field)
			tt.validate(t, buf)
		})
	}
}

func TestSlogLoggerTextHandler(t *testing.T) {
	buf := &bytes.Buffer{}
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	handler := slog.NewTextHandler(buf, opts)
	logger := NewSlogLoggerWithHandler(handler)

	logger.Info("text handler test",
		String("key", "value"),
		Int("number", 123),
	)

	output := buf.String()
	assert.Contains(t, output, "text handler test")
	assert.Contains(t, output, "key=value")
	assert.Contains(t, output, "number=123")
}

func TestSlogLoggerWithVariousFieldTypes(t *testing.T) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	logger := NewSlogLoggerWithHandler(handler)

	logger.Info("various types",
		Field{Key: "int32", Value: int32(32)},
		Field{Key: "int64", Value: int64(64)},
		Field{Key: "uint", Value: uint(100)},
		Field{Key: "uint32", Value: uint32(32)},
		Field{Key: "uint64", Value: uint64(64)},
		Field{Key: "float32", Value: float32(3.14)},
		Field{Key: "float64", Value: float64(2.718)},
	)

	var logEntry map[string]interface{}
	err := json.Unmarshal(buf.Bytes(), &logEntry)
	require.NoError(t, err)

	assert.Equal(t, float64(32), logEntry["int32"])
	assert.Equal(t, float64(64), logEntry["int64"])
	assert.Equal(t, float64(100), logEntry["uint"])
	assert.Equal(t, float64(32), logEntry["uint32"])
	assert.Equal(t, float64(64), logEntry["uint64"])
	assert.InDelta(t, 3.14, logEntry["float32"], 0.001)
	assert.InDelta(t, 2.718, logEntry["float64"], 0.001)
}

func TestSlogLoggerErrorFieldWithoutKey(t *testing.T) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	logger := NewSlogLoggerWithHandler(handler)

	// Test error field with empty key (should use "_error" as default)
	logger.Error("error log", Field{Key: "", Value: errors.New("test error")})

	output := buf.String()
	// The error field with empty key should use "_error" as the key
	assert.Contains(t, output, `"_error":"test error"`)
	assert.Contains(t, output, `"msg":"error log"`)

	// Test that error field with explicit key works
	buf.Reset()
	logger.Error("error log with key", ErrorField("error_field", errors.New("test error")))
	output = buf.String()
	assert.Contains(t, output, `"error_field":"test error"`)

	// Test that ErrorField with empty key also uses "_error"
	buf.Reset()
	logger.Error("error with empty ErrorField", ErrorField("", errors.New("another error")))
	output = buf.String()
	assert.Contains(t, output, `"_error":"another error"`)

	// Test that nil errors don't panic - they just get handled by the default case
	buf.Reset()
	var nilErr error = nil
	logger.Error("nil error test", Field{Key: "nil_error", Value: nilErr})
	output = buf.String()
	// Nil errors will be handled by default case, so just ensure no panic occurred
	assert.Contains(t, output, `"msg":"nil error test"`)

	// Test panic protection by testing with a concrete error type that could panic
	// Create an actual error that could be nil, but not a nil interface
	err := errors.New("test error")
	// This should work normally
	assert.NotPanics(t, func() {
		logger.Error("concrete error test", Field{Key: "concrete_error", Value: err})
	})
}

// BenchmarkSlogLogger benchmarks the SlogLogger performance
func BenchmarkSlogLogger(b *testing.B) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	logger := NewSlogLoggerWithHandler(handler)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.Info("benchmark message",
			String("key", "value"),
			Int("iteration", i),
			Bool("success", true),
		)
	}
}

// TestSlogLoggerIntegration tests the integration with the Logger interface
func TestSlogLoggerIntegration(t *testing.T) {
	// Test that SlogLogger properly implements the Logger interface
	var _ Logger = (*SlogLogger)(nil)

	// Create a logger and test it through the interface
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	var logger Logger = NewSlogLoggerWithHandler(handler)

	// Test all interface methods
	logger.Debug("debug")
	logger.Info("info")
	logger.Warn("warn")
	logger.Error("error")

	ctx := context.Background()
	logger.DebugWithContext(ctx, "debug with context")
	logger.InfoWithContext(ctx, "info with context")
	logger.WarnWithContext(ctx, "warn with context")
	logger.ErrorWithContext(ctx, "error with context")

	childLogger := logger.With(String("child", "true"))
	assert.NotNil(t, childLogger)

	assert.NotPanics(t, func() {
		_ = logger.IsDebugEnabled()
		_ = logger.Sync()
	})
}

// TestSlogLoggerWithMultipleCalls tests multiple sequential calls
func TestSlogLoggerWithMultipleCalls(t *testing.T) {
	buf := &bytes.Buffer{}
	handler := slog.NewJSONHandler(buf, nil)
	logger := NewSlogLoggerWithHandler(handler)

	logger.Info("first message")
	logger.Info("second message")
	logger.Info("third message")

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	assert.Len(t, lines, 3)

	for i, line := range lines {
		var logEntry map[string]interface{}
		err := json.Unmarshal([]byte(line), &logEntry)
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("%s message", []string{"first", "second", "third"}[i]), logEntry["msg"])
	}
}
