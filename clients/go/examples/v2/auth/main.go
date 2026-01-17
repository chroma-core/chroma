package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	v2 "github.com/chroma-core/chroma/clients/go"
)

func main() {
	var example string
	flag.StringVar(&example, "example", "", "Which authentication example to run: basic, bearer, x-chroma-token, custom-headers, cloud")
	flag.Parse()

	if example == "" {
		fmt.Println("Authentication Examples")
		fmt.Println("=======================")
		fmt.Println("\nUsage: go run main.go -example=<example-name>")
		fmt.Println("\nAvailable examples:")
		fmt.Println("  basic          - Basic authentication with username/password")
		fmt.Println("  bearer         - Bearer token authentication")
		fmt.Println("  x-chroma-token - X-Chroma-Token header authentication")
		fmt.Println("  custom-headers - Custom headers authentication")
		fmt.Println("  cloud          - Chroma Cloud authentication")
		fmt.Println("\nExample: go run main.go -example=basic")
		os.Exit(0)
	}

	switch example {
	case "basic":
		runBasicAuth()
	case "bearer":
		runBearerToken()
	case "x-chroma-token":
		runXChromaToken()
	case "custom-headers":
		runCustomHeaders()
	case "cloud":
		runCloudAuth()
	default:
		log.Fatalf("Unknown example: %s. Run without arguments to see available examples.", example)
	}
}

func runBasicAuth() {
	fmt.Println("Running Basic Authentication Example")
	fmt.Println("=====================================")

	// Get credentials from environment variables
	username := os.Getenv("CHROMA_AUTH_USERNAME")
	password := os.Getenv("CHROMA_AUTH_PASSWORD")
	baseURL := os.Getenv("CHROMA_URL")

	// Validate required environment variables
	if username == "" || password == "" {
		log.Fatal("Error: CHROMA_AUTH_USERNAME and CHROMA_AUTH_PASSWORD environment variables are required\n" +
			"Please set them:\n" +
			"  export CHROMA_AUTH_USERNAME=admin\n" +
			"  export CHROMA_AUTH_PASSWORD=your-password")
	}

	// Use default URL if not specified
	if baseURL == "" {
		baseURL = "http://localhost:8000"
		fmt.Printf("Using default URL: %s (set CHROMA_URL to override)\n", baseURL)
	}

	// Create client with basic authentication
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithAuth(v2.NewBasicAuthCredentialsProvider(username, password)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test the connection
	if err := client.Heartbeat(context.Background()); err != nil {
		log.Fatal("Failed to connect:", err)
	}

	fmt.Println("✓ Successfully connected with basic authentication")

	// Create a collection
	collection, err := client.CreateCollection(
		context.Background(),
		"test_collection_basic",
	)
	if err != nil {
		log.Fatal("Failed to create collection:", err)
	}

	defer collection.Delete(context.Background())

	fmt.Printf("✓ Created collection: %s\n", collection.Name())
}

func runBearerToken() {
	fmt.Println("Running Bearer Token Authentication Example")
	fmt.Println("============================================")

	// Get token from environment
	token := os.Getenv("CHROMA_AUTH_TOKEN")
	baseURL := os.Getenv("CHROMA_URL")

	// Validate required environment variables
	if token == "" {
		log.Fatal("Error: CHROMA_AUTH_TOKEN environment variable is required\n" +
			"Please set it:\n" +
			"  export CHROMA_AUTH_TOKEN=your-bearer-token")
	}

	// Use default URL if not specified
	if baseURL == "" {
		baseURL = "http://localhost:8000"
		fmt.Printf("Using default URL: %s (set CHROMA_URL to override)\n", baseURL)
	}

	// Create client with Bearer token authentication
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithAuth(v2.NewTokenAuthCredentialsProvider(token, v2.AuthorizationTokenHeader)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test the connection
	if err := client.Heartbeat(context.Background()); err != nil {
		log.Fatal("Failed to connect:", err)
	}

	fmt.Println("✓ Successfully connected with Bearer token authentication")

	// List collections
	collections, err := client.ListCollections(context.Background())
	if err != nil {
		log.Fatal("Failed to list collections:", err)
	}

	fmt.Printf("✓ Found %d collections\n", len(collections))
	for _, col := range collections {
		fmt.Printf("  - %s\n", col.Name())
	}
	defer func() {
		for _, col := range collections {
			_ = col.Close()
		}
	}()
}

func runXChromaToken() {
	fmt.Println("Running X-Chroma-Token Authentication Example")
	fmt.Println("==============================================")

	// Get token from environment
	token := os.Getenv("CHROMA_AUTH_TOKEN")
	baseURL := os.Getenv("CHROMA_URL")

	// Validate required environment variables
	if token == "" {
		log.Fatal("Error: CHROMA_AUTH_TOKEN environment variable is required\n" +
			"Please set it:\n" +
			"  export CHROMA_AUTH_TOKEN=your-chroma-token")
	}

	// Use default URL if not specified
	if baseURL == "" {
		baseURL = "http://localhost:8000"
		fmt.Printf("Using default URL: %s (set CHROMA_URL to override)\n", baseURL)
	}

	// Create client with X-Chroma-Token authentication
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithAuth(v2.NewTokenAuthCredentialsProvider(token, v2.XChromaTokenHeader)),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test the connection
	if err := client.Heartbeat(context.Background()); err != nil {
		log.Fatal("Failed to connect:", err)
	}

	fmt.Println("✓ Successfully connected with X-Chroma-Token authentication")

	// Get server version
	version, err := client.GetVersion(context.Background())
	if err != nil {
		log.Fatal("Failed to get version:", err)
	}

	fmt.Printf("✓ Server version: %s\n", version)

	// Count collections
	count, err := client.CountCollections(context.Background())
	if err != nil {
		log.Fatal("Failed to count collections:", err)
	}

	fmt.Printf("✓ Total collections: %d\n", count)
}

func runCustomHeaders() {
	fmt.Println("Running Custom Headers Authentication Example")
	fmt.Println("==============================================")

	// Get authentication credentials from environment
	authToken := os.Getenv("AUTH_TOKEN")
	apiKey := os.Getenv("API_KEY")
	baseURL := os.Getenv("CHROMA_URL")

	// Validate at least one authentication method is provided
	if authToken == "" && apiKey == "" {
		log.Fatal("Error: At least one authentication method is required\n" +
			"Please set either AUTH_TOKEN or API_KEY:\n" +
			"  export AUTH_TOKEN=your-bearer-token\n" +
			"  export API_KEY=your-api-key")
	}

	// Use default URL if not specified
	if baseURL == "" {
		baseURL = "http://localhost:8000"
		fmt.Printf("Using default URL: %s (set CHROMA_URL to override)\n", baseURL)
	}

	// Build custom headers
	headers := make(map[string]string)
	if authToken != "" {
		headers["Authorization"] = "Bearer " + authToken
		fmt.Println("✓ Using Bearer token authentication")
	}
	if apiKey != "" {
		headers["X-API-Key"] = apiKey
		fmt.Println("✓ Using API key authentication")
	}
	// Add additional custom headers
	headers["X-Request-ID"] = "req-001"
	headers["X-Custom-Header"] = "custom-value"

	// Create client with custom headers
	client, err := v2.NewHTTPClient(
		v2.WithBaseURL(baseURL),
		v2.WithDefaultHeaders(headers),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test the connection
	if err := client.Heartbeat(context.Background()); err != nil {
		log.Fatal("Failed to connect:", err)
	}

	fmt.Println("✓ Successfully connected with custom headers")

	// Create or get collection
	collection, err := client.GetOrCreateCollection(
		context.Background(),
		"custom_headers_collection",
	)
	if err != nil {
		log.Fatal("Failed to get/create collection:", err)
	}
	defer collection.Close()

	fmt.Printf("✓ Using collection: %s (ID: %s)\n", collection.Name(), collection.ID())
}

func runCloudAuth() {
	fmt.Println("Running Chroma Cloud Authentication Example")
	fmt.Println("============================================")

	// Get cloud configuration from environment variables
	apiKey := os.Getenv("CHROMA_CLOUD_API_KEY")
	tenant := os.Getenv("CHROMA_CLOUD_TENANT")
	database := os.Getenv("CHROMA_CLOUD_DATABASE")

	// Validate required environment variables
	if apiKey == "" {
		log.Fatal("Error: CHROMA_CLOUD_API_KEY environment variable is required\n" +
			"Please set it:\n" +
			"  export CHROMA_CLOUD_API_KEY=your-api-key\n" +
			"Get your API key from: https://app.trychroma.com")
	}

	// Use defaults if not specified
	if tenant == "" {
		tenant = "default-tenant"
		fmt.Printf("Using default tenant: %s (set CHROMA_CLOUD_TENANT to override)\n", tenant)
	}
	if database == "" {
		database = "default-database"
		fmt.Printf("Using default database: %s (set CHROMA_CLOUD_DATABASE to override)\n", database)
	}

	// Create Chroma Cloud client
	client, err := v2.NewCloudClient(
		v2.WithCloudAPIKey(apiKey),
		v2.WithDatabaseAndTenant(database, tenant),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	// Test the connection
	if err := client.Heartbeat(context.Background()); err != nil {
		log.Fatal("Failed to connect to Chroma Cloud:", err)
	}

	fmt.Println("✓ Successfully connected to Chroma Cloud")
	fmt.Printf("  Tenant: %s\n", tenant)
	fmt.Printf("  Database: %s\n", database)

	// List collections in the cloud
	collections, err := client.ListCollections(context.Background())
	if err != nil {
		log.Fatal("Failed to list collections:", err)
	}

	defer func() {
		for _, col := range collections {
			_ = col.Close()
		}
	}()

	fmt.Printf("\n✓ Found %d collections:\n", len(collections))
	for _, col := range collections {
		fmt.Printf("  - %s (ID: %s)\n", col.Name(), col.ID())
	}

	// Create a new collection in the cloud
	collectionName := "cloud_example_collection"
	collection, err := client.GetOrCreateCollection(
		context.Background(),
		collectionName,
	)
	if err != nil {
		log.Fatal("Failed to create collection:", err)
	}
	defer collection.Close()

	fmt.Printf("\n✓ Using collection: %s\n", collection.Name())
}
