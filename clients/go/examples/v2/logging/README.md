# Logging Examples

This directory contains examples demonstrating the logging capabilities of the Chroma Go V2 client.

## Running the Examples

First, ensure you have a Chroma instance running:

```bash
# Using Docker
docker run -p 8000:8000 chromadb/chroma:latest

# Or using the Makefile from the project root
make server
```

Then run the examples:

```bash
go run main.go
```

## Examples Included

1. **Production Logger**: Uses zap's production configuration with JSON output
2. **Development Logger**: Uses zap's development configuration with pretty printing
3. **Custom Logger Configuration**: Shows how to configure a custom zap logger
4. **~~WithDebug() Automatic Logger~~**: **DEPRECATED** - Use WithLogger with debug-level logger instead
5. **NoopLogger**: Shows silent operation with no logging
6. **Custom Fields**: Demonstrates adding persistent fields to all log messages

## Key Features Demonstrated

- **Structured Logging**: All logs use structured fields for better analysis
- **Context Support**: Logger supports context for distributed tracing
- **Log Levels**: Different log levels (Debug, Info, Warn, Error)
- **Field Types**: Various field types (String, Int, Bool, Error, Any)
- **Logger Composition**: Using `With()` to create loggers with persistent fields

## Configuration Options

### Production Logger
```go
zapLogger, _ := zap.NewProduction()
logger := chromalogger.NewZapLogger(zapLogger)
```

### Development Logger
```go
logger, _ := chromalogger.NewDevelopmentZapLogger()
```

### Custom Configuration
```go
config := zap.NewProductionConfig()
config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
config.Encoding = "json"
zapLogger, _ := config.Build()
logger := chromalogger.NewZapLogger(zapLogger)
```

### Debug Mode (DEPRECATED)

**⚠️ Note:** `WithDebug()` is deprecated. Use `WithLogger()` instead:

```go
// DEPRECATED - Don't use this
// client, _ := chroma.NewHTTPClient(
//     chroma.WithDebug(),
// )

// RECOMMENDED - Use this instead
logger, _ := chromalogger.NewDevelopmentZapLogger()
client, _ := chroma.NewHTTPClient(
    chroma.WithLogger(logger), // Use logger with debug level
)
```

## Environment Variables

- `CHROMA_URL`: The URL of your Chroma instance (default: `http://localhost:8000`)

## Output

The examples will produce different types of log output:

- **JSON format** (production): Machine-readable structured logs
- **Console format** (development): Human-readable colored output
- **Silent** (noop): No output at all

## Use Cases

- **Development**: Use development logger with `WithLogger()` for debugging (WithDebug is deprecated)
- **Production**: Use production logger with appropriate log level
- **Testing**: Use NoopLogger to disable logging during tests
- **Monitoring**: Use structured fields for log aggregation and analysis