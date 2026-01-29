package logger

import (
	"context"
)

// Logger is the interface that wraps basic logging methods.
// It provides a common interface for different logging implementations.
type Logger interface {
	// Standard logging methods
	Debug(msg string, fields ...Field)
	Info(msg string, fields ...Field)
	Warn(msg string, fields ...Field)
	Error(msg string, fields ...Field)

	// Context-aware logging methods
	DebugWithContext(ctx context.Context, msg string, fields ...Field)
	InfoWithContext(ctx context.Context, msg string, fields ...Field)
	WarnWithContext(ctx context.Context, msg string, fields ...Field)
	ErrorWithContext(ctx context.Context, msg string, fields ...Field)

	// With returns a new logger with the given fields
	With(fields ...Field) Logger

	// Enabled returns true if the given level is enabled
	IsDebugEnabled() bool

	// Sync flushes any buffered log entries. Should be called before program exit.
	Sync() error
}

// Field represents a key-value pair for structured logging
type Field struct {
	Key   string
	Value interface{}
}

// String creates a string field
func String(key string, value string) Field {
	return Field{Key: key, Value: value}
}

// Int creates an int field
func Int(key string, value int) Field {
	return Field{Key: key, Value: value}
}

// Bool creates a bool field
func Bool(key string, value bool) Field {
	return Field{Key: key, Value: value}
}

// ErrorField creates an error field.
// If key is empty, the field will use "_error" as the default key when logged.
func ErrorField(key string, err error) Field {
	if key == "" {
		// For consistency with slog_logger, empty keys will become "_error"
		// This is handled in the logger implementation, but we document it here
		return Field{Key: "", Value: err}
	}
	return Field{Key: key, Value: err}
}

// Any creates a field with any value
func Any(key string, value interface{}) Field {
	return Field{Key: key, Value: value}
}
