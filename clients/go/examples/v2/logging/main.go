package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	chroma "github.com/chroma-core/chroma/clients/go"
	chromalogger "github.com/chroma-core/chroma/clients/go/pkg/logger"
)

func main() {
	// Example 1: Using a production logger
	fmt.Println("=== Example 1: Production Logger ===")
	productionExample()

	// Example 2: Using a development logger
	fmt.Println("\n=== Example 2: Development Logger ===")
	developmentExample()

	// Example 3: Using custom logger configuration
	fmt.Println("\n=== Example 3: Custom Logger Configuration ===")
	customLoggerExample()

	// Example 4: Debug logging with WithLogger (WithDebug is deprecated)
	fmt.Println("\n=== Example 4: Debug Logging with WithLogger ===")
	debugLoggingExample()

	// Example 5: Using NoopLogger for silent operation
	fmt.Println("\n=== Example 5: NoopLogger (Silent) ===")
	noopLoggerExample()

	// Example 6: Logger with custom fields
	fmt.Println("\n=== Example 6: Logger with Custom Fields ===")
	customFieldsExample()
}

func productionExample() {
	// Create a production logger
	zapLogger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}

	// Wrap in Chroma logger
	logger := chromalogger.NewZapLogger(zapLogger)

	// Create client with logger
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		_ = zapLogger.Sync()
		log.Fatal(err)
	}
	defer func() {
		_ = zapLogger.Sync()
	}()
	defer client.Close()

	// Use the client
	ctx := context.Background()
	collections, err := client.ListCollections(ctx)
	if err != nil {
		logger.Error("Failed to list collections", chromalogger.ErrorField("error", err))
		return
	}

	logger.Info("Listed collections", chromalogger.Int("count", len(collections)))
}

func developmentExample() {
	// Create a development logger with pretty printing
	logger, err := chromalogger.NewDevelopmentZapLogger()
	if err != nil {
		log.Fatal(err)
	}

	// Create client with logger
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Create a collection with logging
	logger.Debug("Creating collection", chromalogger.String("name", "example-collection"))

	collection, err := client.GetOrCreateCollection(
		ctx,
		"example-collection",
		chroma.WithCollectionMetadataCreate(
			chroma.NewMetadata(chroma.NewStringAttribute("description", "example collection description")),
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
	err = client.DeleteCollection(ctx, "example-collection")
	if err != nil {
		logger.Error("Failed to delete collection", chromalogger.ErrorField("error", err))
	}
}

func customLoggerExample() {
	// Configure a custom zap logger
	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	config.Encoding = "json"
	config.EncoderConfig.TimeKey = "timestamp"
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.OutputPaths = []string{"stdout"}

	zapLogger, err := config.Build()
	if err != nil {
		log.Fatal(err)
	}

	// Wrap in Chroma logger
	logger := chromalogger.NewZapLogger(zapLogger)

	// Create client
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger),
	)
	if err != nil {
		_ = zapLogger.Sync()
		log.Fatal(err)
	}
	defer func() {
		_ = zapLogger.Sync()
	}()
	defer client.Close()

	ctx := context.Background()
	err = client.Heartbeat(ctx)
	if err != nil {
		logger.Error("Heartbeat failed", chromalogger.ErrorField("error", err))
		return
	}

	logger.Info("Heartbeat successful")
}

func debugLoggingExample() {
	// Note: WithDebug() is deprecated - use WithLogger with a debug-level logger instead
	// The old way (DEPRECATED):
	// client, err := chroma.NewHTTPClient(
	//     chroma.WithBaseURL(getChromaURL()),
	//     chroma.WithDebug(), // DEPRECATED
	// )

	// The recommended way:
	logger, err := chromalogger.NewDevelopmentZapLogger()
	if err != nil {
		log.Fatal(err)
	}
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger), // Use logger with debug level enabled
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// All HTTP requests and responses will be logged
	version, err := client.GetVersion(ctx)
	if err != nil {
		fmt.Printf("Failed to get version: %v\n", err)
		return
	}

	fmt.Printf("Chroma version: %s\n", version)
}

func noopLoggerExample() {
	// Create a noop logger for silent operation
	logger := chromalogger.NewNoopLogger()

	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(logger), // No logs will be produced
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Operations will work but produce no logs
	err = client.Heartbeat(ctx)
	if err != nil {
		fmt.Printf("Heartbeat failed: %v\n", err)
		return
	}

	fmt.Println("NoopLogger: Operations completed silently")
}

func customFieldsExample() {
	// Create a logger
	zapLogger, _ := zap.NewDevelopment()
	logger := chromalogger.NewZapLogger(zapLogger)

	// Add persistent fields to the logger
	requestLogger := logger.With(
		chromalogger.String("service", "chroma-example"),
		chromalogger.String("version", "1.0.0"),
		chromalogger.String("environment", "development"),
	)

	// Create client with the logger that has custom fields
	client, err := chroma.NewHTTPClient(
		chroma.WithBaseURL(getChromaURL()),
		chroma.WithLogger(requestLogger),
	)
	if err != nil {
		_ = zapLogger.Sync()
		log.Fatal(err)
	}
	defer func() {
		_ = zapLogger.Sync()
	}()
	defer client.Close()

	ctx := context.Background()

	// All logs from this client will include the custom fields
	collections, err := client.ListCollections(ctx)
	if err != nil {
		requestLogger.Error("Failed to list collections",
			chromalogger.ErrorField("error", err),
			chromalogger.String("operation", "list-collections"),
		)
		return
	}

	requestLogger.Info("Operation completed",
		chromalogger.String("operation", "list-collections"),
		chromalogger.Int("result_count", len(collections)),
		chromalogger.Bool("success", true),
	)
}

func getChromaURL() string {
	url := os.Getenv("CHROMA_URL")
	if url == "" {
		return "http://localhost:8000"
	}
	return url
}
