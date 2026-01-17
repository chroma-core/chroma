package logger

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// ZapLogger is a Logger implementation using uber-go/zap
type ZapLogger struct {
	logger *zap.Logger
}

// NewZapLogger creates a new ZapLogger with the provided zap.Logger
func NewZapLogger(logger *zap.Logger) *ZapLogger {
	return &ZapLogger{
		logger: logger,
	}
}

// NewDefaultZapLogger creates a new ZapLogger with default configuration
func NewDefaultZapLogger() (*ZapLogger, error) {
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}
	return &ZapLogger{logger: logger}, nil
}

// NewDevelopmentZapLogger creates a new ZapLogger with development configuration
func NewDevelopmentZapLogger() (*ZapLogger, error) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}
	return &ZapLogger{logger: logger}, nil
}

// Debug logs a message at debug level
func (z *ZapLogger) Debug(msg string, fields ...Field) {
	z.logger.Debug(msg, convertFields(fields)...)
}

// Info logs a message at info level
func (z *ZapLogger) Info(msg string, fields ...Field) {
	z.logger.Info(msg, convertFields(fields)...)
}

// Warn logs a message at warn level
func (z *ZapLogger) Warn(msg string, fields ...Field) {
	z.logger.Warn(msg, convertFields(fields)...)
}

// Error logs a message at error level
func (z *ZapLogger) Error(msg string, fields ...Field) {
	z.logger.Error(msg, convertFields(fields)...)
}

// DebugWithContext logs a message at debug level with context
func (z *ZapLogger) DebugWithContext(ctx context.Context, msg string, fields ...Field) {
	// Add context fields if needed (e.g., trace ID, request ID)
	ctxFields := extractContextFields(ctx)
	allFields := append(convertFields(fields), ctxFields...)
	z.logger.Debug(msg, allFields...)
}

// InfoWithContext logs a message at info level with context
func (z *ZapLogger) InfoWithContext(ctx context.Context, msg string, fields ...Field) {
	ctxFields := extractContextFields(ctx)
	allFields := append(convertFields(fields), ctxFields...)
	z.logger.Info(msg, allFields...)
}

// WarnWithContext logs a message at warn level with context
func (z *ZapLogger) WarnWithContext(ctx context.Context, msg string, fields ...Field) {
	ctxFields := extractContextFields(ctx)
	allFields := append(convertFields(fields), ctxFields...)
	z.logger.Warn(msg, allFields...)
}

// ErrorWithContext logs a message at error level with context
func (z *ZapLogger) ErrorWithContext(ctx context.Context, msg string, fields ...Field) {
	ctxFields := extractContextFields(ctx)
	allFields := append(convertFields(fields), ctxFields...)
	z.logger.Error(msg, allFields...)
}

// With returns a new logger with the given fields
func (z *ZapLogger) With(fields ...Field) Logger {
	return &ZapLogger{
		logger: z.logger.With(convertFields(fields)...),
	}
}

// IsDebugEnabled returns true if debug level is enabled
func (z *ZapLogger) IsDebugEnabled() bool {
	return z.logger.Core().Enabled(zapcore.DebugLevel)
}

// Sync flushes any buffered log entries
func (z *ZapLogger) Sync() error {
	return z.logger.Sync()
}

// convertFields converts our Field type to zap.Field
func convertFields(fields []Field) []zap.Field {
	zapFields := make([]zap.Field, len(fields))
	for i, f := range fields {
		switch v := f.Value.(type) {
		case string:
			zapFields[i] = zap.String(f.Key, v)
		case int:
			zapFields[i] = zap.Int(f.Key, v)
		case int32:
			zapFields[i] = zap.Int32(f.Key, v)
		case int64:
			zapFields[i] = zap.Int64(f.Key, v)
		case uint:
			zapFields[i] = zap.Uint(f.Key, v)
		case uint32:
			zapFields[i] = zap.Uint32(f.Key, v)
		case uint64:
			zapFields[i] = zap.Uint64(f.Key, v)
		case bool:
			zapFields[i] = zap.Bool(f.Key, v)
		case float32:
			zapFields[i] = zap.Float32(f.Key, v)
		case float64:
			zapFields[i] = zap.Float64(f.Key, v)
		case error:
			// Handle nil errors safely to prevent panics
			if v == nil {
				key := f.Key
				if key == "" {
					key = "_error"
				}
				// Use zap.Any to represent nil error
				zapFields[i] = zap.Any(key, nil)
			} else {
				// For consistency with slog_logger:
				// - If key is provided, use it
				// - If key is empty, use "_error" as default
				key := f.Key
				if key == "" {
					key = "_error"
				}
				zapFields[i] = zap.NamedError(key, v)
			}
		default:
			zapFields[i] = zap.Any(f.Key, v)
		}
	}
	return zapFields
}

// extractContextFields extracts fields from context
// This can be extended to extract trace IDs, request IDs, etc.
func extractContextFields(ctx context.Context) []zap.Field {
	if ctx == nil {
		return []zap.Field{}
	}

	fields := []zap.Field{}

	// Example: Safely extract trace ID if present
	// if traceID := ctx.Value("trace-id"); traceID != nil {
	//     if tid, ok := traceID.(string); ok {
	//         fields = append(fields, zap.String("trace_id", tid))
	//     } else {
	//         // Handle non-string trace ID safely
	//         fields = append(fields, zap.Any("trace_id", traceID))
	//     }
	// }

	// Example: Safely extract request ID
	// if reqID := ctx.Value("request-id"); reqID != nil {
	//     switch v := reqID.(type) {
	//     case string:
	//         fields = append(fields, zap.String("request_id", v))
	//     case fmt.Stringer:
	//         fields = append(fields, zap.String("request_id", v.String()))
	//     default:
	//         fields = append(fields, zap.Any("request_id", v))
	//     }
	// }

	return fields
}
