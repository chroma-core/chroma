package logger

import "context"

// NoopLogger is a logger that doesn't log anything
type NoopLogger struct{}

// NewNoopLogger creates a new NoopLogger
func NewNoopLogger() *NoopLogger {
	return &NoopLogger{}
}

// Debug does nothing
func (n *NoopLogger) Debug(msg string, fields ...Field) {}

// Info does nothing
func (n *NoopLogger) Info(msg string, fields ...Field) {}

// Warn does nothing
func (n *NoopLogger) Warn(msg string, fields ...Field) {}

// Error does nothing
func (n *NoopLogger) Error(msg string, fields ...Field) {}

// DebugWithContext does nothing
func (n *NoopLogger) DebugWithContext(ctx context.Context, msg string, fields ...Field) {}

// InfoWithContext does nothing
func (n *NoopLogger) InfoWithContext(ctx context.Context, msg string, fields ...Field) {}

// WarnWithContext does nothing
func (n *NoopLogger) WarnWithContext(ctx context.Context, msg string, fields ...Field) {}

// ErrorWithContext does nothing
func (n *NoopLogger) ErrorWithContext(ctx context.Context, msg string, fields ...Field) {}

// With returns the same NoopLogger
func (n *NoopLogger) With(fields ...Field) Logger {
	return n
}

// IsDebugEnabled always returns false
func (n *NoopLogger) IsDebugEnabled() bool {
	return false
}

// Sync does nothing for NoopLogger
func (n *NoopLogger) Sync() error {
	return nil
}
