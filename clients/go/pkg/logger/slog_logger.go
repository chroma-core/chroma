// Package logger provides logging implementations for the chroma-go library.
// This file implements the Logger interface using the standard library's slog package.
//
// Panic Prevention:
// This implementation follows the codebase panic prevention guidelines:
// - No use of Must* functions that could panic
// - All type conversions are safe and explicit
// - No risky operations like unchecked array access or nil pointer dereferences
// - Type switches include default cases to handle unexpected types safely
//
// As a library component, this code is designed to never panic in production use.
package logger

import (
	"context"
	"log/slog"
	"os"
)

// SlogLogger is a Logger implementation using the standard library's slog package
type SlogLogger struct {
	logger *slog.Logger
}

// NewSlogLogger creates a new SlogLogger with the provided slog.Logger
func NewSlogLogger(logger *slog.Logger) *SlogLogger {
	return &SlogLogger{
		logger: logger,
	}
}

// NewSlogLoggerWithHandler creates a new SlogLogger with the provided handler
func NewSlogLoggerWithHandler(handler slog.Handler) *SlogLogger {
	return &SlogLogger{
		logger: slog.New(handler),
	}
}

// NewDefaultSlogLogger creates a new SlogLogger with JSON handler and production configuration
func NewDefaultSlogLogger() (*SlogLogger, error) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewJSONHandler(os.Stdout, opts)
	return &SlogLogger{
		logger: slog.New(handler),
	}, nil
}

// NewTextSlogLogger creates a new SlogLogger with text handler for human-readable output
func NewTextSlogLogger() (*SlogLogger, error) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	return &SlogLogger{
		logger: slog.New(handler),
	}, nil
}

// NewInfoSlogLogger creates a new SlogLogger with info level (no debug output)
func NewInfoSlogLogger() (*SlogLogger, error) {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	return &SlogLogger{
		logger: slog.New(handler),
	}, nil
}

// Debug logs a message at debug level
func (s *SlogLogger) Debug(msg string, fields ...Field) {
	s.logger.Debug(msg, convertFieldsToAttrs(fields)...)
}

// Info logs a message at info level
func (s *SlogLogger) Info(msg string, fields ...Field) {
	s.logger.Info(msg, convertFieldsToAttrs(fields)...)
}

// Warn logs a message at warn level
func (s *SlogLogger) Warn(msg string, fields ...Field) {
	s.logger.Warn(msg, convertFieldsToAttrs(fields)...)
}

// Error logs a message at error level
func (s *SlogLogger) Error(msg string, fields ...Field) {
	s.logger.Error(msg, convertFieldsToAttrs(fields)...)
}

// DebugWithContext logs a message at debug level with context
func (s *SlogLogger) DebugWithContext(ctx context.Context, msg string, fields ...Field) {
	ctxAttrs := extractContextAttrs(ctx)
	allAttrs := append(convertFieldsToAttrs(fields), ctxAttrs...)
	s.logger.DebugContext(ctx, msg, allAttrs...)
}

// InfoWithContext logs a message at info level with context
func (s *SlogLogger) InfoWithContext(ctx context.Context, msg string, fields ...Field) {
	ctxAttrs := extractContextAttrs(ctx)
	allAttrs := append(convertFieldsToAttrs(fields), ctxAttrs...)
	s.logger.InfoContext(ctx, msg, allAttrs...)
}

// WarnWithContext logs a message at warn level with context
func (s *SlogLogger) WarnWithContext(ctx context.Context, msg string, fields ...Field) {
	ctxAttrs := extractContextAttrs(ctx)
	allAttrs := append(convertFieldsToAttrs(fields), ctxAttrs...)
	s.logger.WarnContext(ctx, msg, allAttrs...)
}

// ErrorWithContext logs a message at error level with context
func (s *SlogLogger) ErrorWithContext(ctx context.Context, msg string, fields ...Field) {
	ctxAttrs := extractContextAttrs(ctx)
	allAttrs := append(convertFieldsToAttrs(fields), ctxAttrs...)
	s.logger.ErrorContext(ctx, msg, allAttrs...)
}

// With returns a new logger with the given fields
func (s *SlogLogger) With(fields ...Field) Logger {
	return &SlogLogger{
		logger: s.logger.With(convertFieldsToAttrs(fields)...),
	}
}

// IsDebugEnabled returns true if debug level is enabled
func (s *SlogLogger) IsDebugEnabled() bool {
	return s.logger.Enabled(context.Background(), slog.LevelDebug)
}

// Sync flushes any buffered log entries
// slog doesn't require explicit sync, but we implement it for interface compatibility
func (s *SlogLogger) Sync() error {
	// slog handlers typically don't buffer, so this is a no-op
	// If using a custom handler that does buffer, it should handle syncing internally
	return nil
}

// convertFieldsToAttrs converts our Field type to slog.Attr
// This function is designed to be panic-safe through careful type switching.
// All type conversions are explicit and safe, avoiding any panic-prone operations.
func convertFieldsToAttrs(fields []Field) []any {
	attrs := make([]any, 0, len(fields))
	for _, f := range fields {
		switch v := f.Value.(type) {
		case string:
			attrs = append(attrs, slog.String(f.Key, v))
		case int:
			attrs = append(attrs, slog.Int(f.Key, v))
		case int32:
			attrs = append(attrs, slog.Int(f.Key, int(v)))
		case int64:
			attrs = append(attrs, slog.Int64(f.Key, v))
		case uint:
			attrs = append(attrs, slog.Uint64(f.Key, uint64(v)))
		case uint32:
			attrs = append(attrs, slog.Uint64(f.Key, uint64(v)))
		case uint64:
			attrs = append(attrs, slog.Uint64(f.Key, v))
		case bool:
			attrs = append(attrs, slog.Bool(f.Key, v))
		case float32:
			attrs = append(attrs, slog.Float64(f.Key, float64(v)))
		case float64:
			attrs = append(attrs, slog.Float64(f.Key, v))
		case error:
			// Handle nil errors safely to prevent panics
			if v == nil {
				// Use the provided key or "_error" for consistency
				key := f.Key
				if key == "" {
					key = "_error"
				}
				// slog doesn't have a Nil method, so we use Any with nil value
				attrs = append(attrs, slog.Any(key, nil))
			} else {
				// Special handling for non-nil error fields:
				// - If the field key is provided, use it as-is
				// - If the field key is empty, use "_error" as a safe default
				//   The underscore prefix indicates it's an auto-generated key
				// - This ensures error information is never lost
				key := f.Key
				if key == "" {
					key = "_error" // Safe default that won't conflict with user "error" fields
				}
				attrs = append(attrs, slog.String(key, v.Error()))
			}
		default:
			// For any other type, use slog.Any which safely handles all types
			attrs = append(attrs, slog.Any(f.Key, v))
		}
	}
	return attrs
}

// extractContextAttrs extracts attributes from context.
// This function is designed to be panic-safe - it will never panic regardless of context values.
//
// Common patterns for extending this function:
//
//  1. Extract trace ID (OpenTelemetry style):
//     if span := trace.SpanFromContext(ctx); span.SpanContext().IsValid() {
//     attrs = append(attrs, slog.String("trace_id", span.SpanContext().TraceID().String()))
//     attrs = append(attrs, slog.String("span_id", span.SpanContext().SpanID().String()))
//     }
//
//  2. Extract custom request ID:
//     if reqID := ctx.Value(requestIDKey); reqID != nil {
//     switch v := reqID.(type) {
//     case string:
//     attrs = append(attrs, slog.String("request_id", v))
//     case fmt.Stringer:
//     attrs = append(attrs, slog.String("request_id", v.String()))
//     }
//     }
//
//  3. Extract user information:
//     if user := ctx.Value(userKey); user != nil {
//     if u, ok := user.(User); ok && u.ID != "" {
//     attrs = append(attrs, slog.String("user_id", u.ID))
//     }
//     }
//
//  4. Extract correlation ID for distributed tracing:
//     if corrID := ctx.Value(correlationIDKey); corrID != nil {
//     if cid, ok := corrID.(string); ok {
//     attrs = append(attrs, slog.String("correlation_id", cid))
//     }
//     }
//
// Note: Always use type assertions or type switches to safely extract values.
// Consider defining typed context keys to avoid string collisions.
func extractContextAttrs(ctx context.Context) []any {
	if ctx == nil {
		return []any{}
	}

	// Currently returns empty slice but can be extended using patterns above
	return []any{}
}
