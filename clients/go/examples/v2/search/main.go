// Package main demonstrates the Chroma Cloud Search API.
//
// This example shows:
// - Basic KNN search with text queries
// - Metadata filtering with search
// - Weighted search combinations
// - Reciprocal Rank Fusion (RRF)
// - Pagination and field selection
//
// Requirements:
// - Chroma Cloud account with API key
// - Environment variables: CHROMA_CLOUD_API_KEY, CHROMA_CLOUD_TENANT, CHROMA_CLOUD_DATABASE
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	chroma "github.com/chroma-core/chroma/clients/go"
	"github.com/chroma-core/chroma/clients/go/pkg/embeddings"
)

func main() {
	// Get credentials from environment
	apiKey := os.Getenv("CHROMA_CLOUD_API_KEY")
	tenant := os.Getenv("CHROMA_CLOUD_TENANT")
	database := os.Getenv("CHROMA_CLOUD_DATABASE")

	if apiKey == "" || tenant == "" || database == "" {
		log.Fatal("Set CHROMA_CLOUD_API_KEY, CHROMA_CLOUD_TENANT, CHROMA_CLOUD_DATABASE")
	}

	// Create cloud client
	client, err := chroma.NewHTTPClient(
		chroma.WithCloudAPIKey(apiKey),
		chroma.WithDatabaseAndTenant(tenant, database),
	)
	if err != nil {
		log.Fatalf("Error creating client: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Use the consistent hash embedding function for demonstration
	// In production, use OpenAI, Cohere, or another embedding provider
	ef := embeddings.NewConsistentHashEmbeddingFunction()

	// Create or get collection
	col, err := client.GetOrCreateCollection(ctx, "search-demo",
		chroma.WithEmbeddingFunctionCreate(ef),
		chroma.WithCollectionMetadataCreate(
			chroma.NewMetadata(
				chroma.NewStringAttribute("description", "Search API demo collection"),
			),
		),
	)
	if err != nil {
		_ = client.Close()
		log.Printf("Error creating collection: %v", err)
		return
	}

	// Add sample documents
	err = setupSampleData(ctx, col)
	if err != nil {
		log.Printf("Error setting up sample data: %v", err)
		return
	}

	// Run examples
	basicSearch(ctx, col)
	searchWithFilter(ctx, col)
	weightedSearch(ctx, col)
	rrfSearch(ctx, col)
	paginatedSearch(ctx, col)

	// Cleanup
	err = client.DeleteCollection(ctx, "search-demo")
	if err != nil {
		log.Printf("Error deleting collection: %v", err)
	}
}

func setupSampleData(ctx context.Context, col chroma.Collection) error {
	return col.Add(ctx,
		chroma.WithIDs(
			"doc1", "doc2", "doc3", "doc4", "doc5",
			"doc6", "doc7", "doc8", "doc9", "doc10",
		),
		chroma.WithTexts(
			"Machine learning is a subset of artificial intelligence",
			"Deep learning uses neural networks with many layers",
			"Natural language processing enables computers to understand text",
			"Computer vision allows machines to interpret images",
			"Reinforcement learning trains agents through rewards",
			"Transfer learning reuses models trained on other tasks",
			"Supervised learning requires labeled training data",
			"Unsupervised learning finds patterns without labels",
			"Neural networks are inspired by biological neurons",
			"Gradient descent optimizes model parameters iteratively",
		),
		chroma.WithMetadatas(
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "ml"), chroma.NewIntAttribute("year", 2023)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "dl"), chroma.NewIntAttribute("year", 2024)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "nlp"), chroma.NewIntAttribute("year", 2023)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "cv"), chroma.NewIntAttribute("year", 2022)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "rl"), chroma.NewIntAttribute("year", 2024)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "ml"), chroma.NewIntAttribute("year", 2023)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "ml"), chroma.NewIntAttribute("year", 2022)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "ml"), chroma.NewIntAttribute("year", 2024)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "dl"), chroma.NewIntAttribute("year", 2023)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("category", "ml"), chroma.NewIntAttribute("year", 2022)),
		),
	)
}

// basicSearch demonstrates simple KNN search with text query
func basicSearch(ctx context.Context, col chroma.Collection) {
	fmt.Println("\n=== Basic Search ===")

	result, err := col.Search(ctx,
		chroma.NewSearchRequest(
			chroma.WithKnnRank(
				chroma.KnnQueryText("neural networks and deep learning"),
				chroma.WithKnnLimit(50),
			),
			chroma.NewPage(chroma.Limit(5)),
			chroma.WithSelect(chroma.KDocument, chroma.KScore),
		),
	)
	if err != nil {
		log.Printf("Search error: %v", err)
		return
	}

	printResults(result)
}

// searchWithFilter demonstrates combining search with metadata filters
func searchWithFilter(ctx context.Context, col chroma.Collection) {
	fmt.Println("\n=== Search with Filter ===")
	fmt.Println("Query: 'machine learning', Filter: category='ml' AND year>=2023")

	result, err := col.Search(ctx,
		chroma.NewSearchRequest(
			chroma.WithKnnRank(
				chroma.KnnQueryText("machine learning"),
				chroma.WithKnnLimit(50),
			),
			chroma.WithFilter(
				chroma.And(
					chroma.EqString(chroma.K("category"), "ml"),
					chroma.GteInt(chroma.K("year"), 2023),
				),
			),
			chroma.NewPage(chroma.Limit(5)),
			chroma.WithSelect(chroma.KDocument, chroma.KScore, chroma.K("category"), chroma.K("year")),
		),
	)
	if err != nil {
		log.Printf("Search error: %v", err)
		return
	}

	printResults(result)
}

// weightedSearch demonstrates combining multiple search queries with weights
func weightedSearch(ctx context.Context, col chroma.Collection) {
	fmt.Println("\n=== Weighted Search ===")
	fmt.Println("Combining 'neural networks' (70%) + 'training data' (30%)")

	knn1, err := chroma.NewKnnRank(
		chroma.KnnQueryText("neural networks"),
		chroma.WithKnnLimit(50),
		chroma.WithKnnDefault(1000.0), // Include docs not in top-K
	)
	if err != nil {
		log.Printf("Error creating knn1: %v", err)
		return
	}

	knn2, err := chroma.NewKnnRank(
		chroma.KnnQueryText("training data"),
		chroma.WithKnnLimit(50),
		chroma.WithKnnDefault(1000.0),
	)
	if err != nil {
		log.Printf("Error creating knn2: %v", err)
		return
	}

	// Weighted combination: 70% knn1 + 30% knn2
	combined := knn1.Multiply(chroma.FloatOperand(0.7)).Add(
		knn2.Multiply(chroma.FloatOperand(0.3)),
	)

	result, err := col.Search(ctx,
		chroma.NewSearchRequest(
			chroma.WithRank(combined),
			chroma.NewPage(chroma.Limit(5)),
			chroma.WithSelect(chroma.KDocument, chroma.KScore),
		),
	)
	if err != nil {
		log.Printf("Search error: %v", err)
		return
	}

	printResults(result)
}

// rrfSearch demonstrates Reciprocal Rank Fusion
func rrfSearch(ctx context.Context, col chroma.Collection) {
	fmt.Println("\n=== RRF Search ===")
	fmt.Println("Combining rankings from 'AI' and 'learning' queries")

	knn1, err := chroma.NewKnnRank(
		chroma.KnnQueryText("artificial intelligence"),
		chroma.WithKnnReturnRank(), // Required for RRF
		chroma.WithKnnLimit(50),
	)
	if err != nil {
		log.Printf("Error creating knn1: %v", err)
		return
	}

	knn2, err := chroma.NewKnnRank(
		chroma.KnnQueryText("learning algorithms"),
		chroma.WithKnnReturnRank(),
		chroma.WithKnnLimit(50),
	)
	if err != nil {
		log.Printf("Error creating knn2: %v", err)
		return
	}

	rrf, err := chroma.NewRrfRank(
		chroma.WithRffRanks(
			knn1.WithWeight(0.6),
			knn2.WithWeight(0.4),
		),
		chroma.WithRffK(60),
	)
	if err != nil {
		log.Printf("Error creating RRF: %v", err)
		return
	}

	result, err := col.Search(ctx,
		chroma.NewSearchRequest(
			chroma.WithRank(rrf),
			chroma.NewPage(chroma.Limit(5)),
			chroma.WithSelect(chroma.KDocument, chroma.KScore),
		),
	)
	if err != nil {
		log.Printf("Search error: %v", err)
		return
	}

	printResults(result)
}

// paginatedSearch demonstrates pagination through results
func paginatedSearch(ctx context.Context, col chroma.Collection) {
	fmt.Println("\n=== Paginated Search ===")

	pageSize := 3
	for page := 0; page < 3; page++ {
		fmt.Printf("\n--- Page %d ---\n", page+1)

		result, err := col.Search(ctx,
			chroma.NewSearchRequest(
				chroma.WithKnnRank(
					chroma.KnnQueryText("learning"),
					chroma.WithKnnLimit(50),
				),
				chroma.NewPage(chroma.Limit(pageSize), chroma.Offset(page*pageSize)),
				chroma.WithSelect(chroma.KDocument, chroma.KScore),
			),
		)
		if err != nil {
			log.Printf("Search error: %v", err)
			return
		}

		printResults(result)
	}
}

func printResults(result chroma.SearchResult) {
	sr, ok := result.(*chroma.SearchResultImpl)
	if !ok || len(sr.IDs) == 0 || len(sr.IDs[0]) == 0 {
		fmt.Println("No results found")
		return
	}

	for i, id := range sr.IDs[0] {
		fmt.Printf("  [%d] ID: %s", i+1, id)
		if len(sr.Scores) > 0 && len(sr.Scores[0]) > i {
			fmt.Printf(", Score: %.4f", sr.Scores[0][i])
		}
		if len(sr.Documents) > 0 && len(sr.Documents[0]) > i {
			doc := sr.Documents[0][i]
			if len(doc) > 60 {
				doc = doc[:60] + "..."
			}
			fmt.Printf("\n      Doc: %s", doc)
		}
		fmt.Println()
	}
}
