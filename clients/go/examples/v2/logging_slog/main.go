package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"

	chroma "github.com/chroma-core/chroma/clients/go"
	chromalogger "github.com/chroma-core/chroma/clients/go/pkg/logger"
)

func main() {
	// Example 1: Using slog with default JSON handler
	fmt.Println("=== Example 1: Default slog JSON Logger ===")
	defaultJSONExample()

	// Example 2: Using slog with text handler
	fmt.Println("\n=== Example 2: slog Text Logger ===")
	textLoggerExample()

	// Example 3: Using custom slog configuration
	fmt.Println("\n=== Example 3: Custom slog Configuration ===")
	customSlogExample()

	// Example 4: Using slog with custom handler options
	fmt.Println("\n=== Example 4: slog with Custom Handler Options ===")
	customHandlerOptionsExample()

	// Example 5: Using slog with persistent fields
	fmt.Println("\n=== Example 5: slog with Persistent Fields ===")
	persistentFieldsExample()

	// Example 6: Using slog with context
	fmt.Println("\n=== Example 6: slog with Context ===")
	contextExample()
}

func defaultJSONExample() {
	// Create a default slog logger (JSON output)
	logger, err := chromalogger.NewDefaultSlogLogger()
	if err != nil {
		log.Printf("Failed to create logger: %v\n", err)
		return
	}

	// Create client with logger
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		logger.Error("Failed to create client", chromalogger.ErrorField("error", err))
		return
	}
	defer client.Close()

	// Use the client
	ctx := context.Background()
	collections, err := client.ListCollections(ctx)
	if err != nil {
		logger.Error("Failed to list collections", chromalogger.ErrorField("error", err))
		return
	}

	logger.Info("Listed collections",
		chromalogger.Int("count", len(collections)),
		chromalogger.String("format", "json"),
	)
}

func textLoggerExample() {
	// Create a text logger for human-readable output
	logger, err := chromalogger.NewTextSlogLogger()
	if err != nil {
		log.Printf("Failed to create logger: %v\n", err)
		return
	}

	// Create client with text logger
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		logger.Error("Failed to create client", chromalogger.ErrorField("error", err))
		return
	}
	defer client.Close()

	ctx := context.Background()

	// Create a collection with logging
	logger.Debug("Creating collection", chromalogger.String("name", "slog-example-collection"))

	collection, err := client.GetOrCreateCollection(
		ctx,
		"slog-example-collection",
		chroma.WithCollectionMetadataCreate(
			chroma.NewMetadata(chroma.NewStringAttribute("description", "slog example collection")),
		),
	)
	if err != nil {
		logger.Error("Failed to create collection", chromalogger.ErrorField("error", err))
		return
	}

	logger.Info("Collection created successfully",
		chromalogger.String("id", collection.ID()),
		chromalogger.String("name", collection.Name()),
	)

	// Clean up
	err = client.DeleteCollection(ctx, "slog-example-collection")
	if err != nil {
		logger.Error("Failed to delete collection", chromalogger.ErrorField("error", err))
	}
}

func customSlogExample() {
	// Create a custom slog logger with specific configuration
	opts := &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true, // Add source file information
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Custom attribute replacement
			if a.Key == slog.TimeKey {
				// Format time differently
				return slog.Attr{Key: "timestamp", Value: a.Value}
			}
			return a
		},
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	slogInstance := slog.New(handler)
	logger := chromalogger.NewSlogLogger(slogInstance)

	// Create client
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		logger.Error("Failed to create client", chromalogger.ErrorField("error", err))
		return
	}
	defer client.Close()

	ctx := context.Background()
	err = client.Heartbeat(ctx)
	if err != nil {
		logger.Error("Heartbeat failed", chromalogger.ErrorField("error", err))
		return
	}

	logger.Info("Heartbeat successful with custom logger")
}

func customHandlerOptionsExample() {
	// Create a logger with custom handler
	// This example shows how to create a handler that filters sensitive information
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Example: Redact sensitive information
			if a.Key == "api_key" {
				return slog.String(a.Key, "***REDACTED***")
			}
			// Example: Shorten long error messages
			if a.Key == "error" && len(a.Value.String()) > 100 {
				return slog.String(a.Key, a.Value.String()[:100]+"...")
			}
			return a
		},
	}

	handler := slog.NewJSONHandler(os.Stdout, opts)
	logger := chromalogger.NewSlogLoggerWithHandler(handler)

	// Test the redaction
	logger.Info("Configuration loaded",
		chromalogger.String("api_key", "secret-key-12345"), // Will be redacted
		chromalogger.String("endpoint", "http://localhost:8000"),
	)

	// Create client
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		logger.Error("Failed to create client", chromalogger.ErrorField("error", err))
		return
	}
	defer client.Close()

	ctx := context.Background()
	version, err := client.GetVersion(ctx)
	if err != nil {
		logger.Error("Failed to get version", chromalogger.ErrorField("error", err))
		return
	}

	logger.Info("Chroma version retrieved", chromalogger.String("version", version))
}

func persistentFieldsExample() {
	// Create a base logger
	baseHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	baseLogger := chromalogger.NewSlogLoggerWithHandler(baseHandler)

	// Add persistent fields that will be included in all log messages
	logger := baseLogger.With(
		chromalogger.String("service", "chroma-slog-example"),
		chromalogger.String("version", "1.0.0"),
		chromalogger.String("environment", "development"),
		chromalogger.String("host", "localhost"),
	)

	// Create client with the logger that has persistent fields
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		logger.Error("Failed to create client", chromalogger.ErrorField("error", err))
		return
	}
	defer client.Close()

	ctx := context.Background()

	// All logs will include the persistent fields
	collections, err := client.ListCollections(ctx)
	if err != nil {
		logger.Error("Failed to list collections",
			chromalogger.ErrorField("error", err),
			chromalogger.String("operation", "list-collections"),
		)
		return
	}

	logger.Info("Operation completed",
		chromalogger.String("operation", "list-collections"),
		chromalogger.Int("result_count", len(collections)),
		chromalogger.Bool("success", true),
	)
}

func contextExample() {
	// Create a logger
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})
	logger := chromalogger.NewSlogLoggerWithHandler(handler)

	// Create client
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		logger.Error("Failed to create client", chromalogger.ErrorField("error", err))
		return
	}
	defer client.Close()

	// Create a context with values that could be extracted in logging
	ctx := context.Background()
	// In a real application, you might have trace IDs, request IDs, etc.
	// ctx = context.WithValue(ctx, "trace-id", "abc123")
	// ctx = context.WithValue(ctx, "request-id", "req-456")

	// Use context-aware logging
	logger.InfoWithContext(ctx, "Starting operation",
		chromalogger.String("operation", "collection-management"),
	)

	// Perform operations
	collection, err := client.GetOrCreateCollection(
		ctx,
		"context-example",
		chroma.WithCollectionMetadataCreate(
			chroma.NewMetadata(chroma.NewStringAttribute("created_with", "context")),
		),
	)
	if err != nil {
		logger.ErrorWithContext(ctx, "Failed to create collection",
			chromalogger.ErrorField("error", err),
		)
		return
	}

	logger.DebugWithContext(ctx, "Collection details",
		chromalogger.String("id", collection.ID()),
		chromalogger.String("name", collection.Name()),
	)

	// Clean up
	err = client.DeleteCollection(ctx, "context-example")
	if err != nil {
		logger.ErrorWithContext(ctx, "Failed to delete collection",
			chromalogger.ErrorField("error", err),
		)
	} else {
		logger.InfoWithContext(ctx, "Collection deleted successfully")
	}
}

func getChromaURL() string {
	url := os.Getenv("CHROMA_URL")
	if url == "" {
		return "http://localhost:8000"
	}
	return url
}
