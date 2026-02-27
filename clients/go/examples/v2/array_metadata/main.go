package main

import (
	"context"
	"fmt"
	"log"

	chroma "github.com/chroma-core/chroma/clients/go"
)

func main() {
	client, err := chroma.NewHTTPClient()
	if err != nil {
		log.Fatalf("Error creating client: %s\n", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("Error closing client: %s\n", err)
		}
	}()

	ctx := context.Background()

	// Create a collection
	col, err := client.GetOrCreateCollection(ctx, "array_metadata_demo")
	if err != nil {
		log.Fatalf("Error creating collection: %s\n", err)
	}

	// Add documents with array metadata (requires Chroma >= 1.5.0)
	err = col.Add(ctx,
		chroma.WithIDs("doc1", "doc2", "doc3"),
		chroma.WithTexts(
			"Introduction to machine learning",
			"Advanced calculus for engineers",
			"Physics of quantum computing",
		),
		chroma.WithMetadatas(
			chroma.NewDocumentMetadata(
				chroma.NewStringArrayAttribute("tags", []string{"ml", "ai", "beginner"}),
				chroma.NewIntArrayAttribute("scores", []int64{95, 87, 92}),
				chroma.NewFloatArrayAttribute("ratings", []float64{4.5, 4.8}),
				chroma.NewBoolArrayAttribute("flags", []bool{true, false}),
			),
			chroma.NewDocumentMetadata(
				chroma.NewStringArrayAttribute("tags", []string{"math", "engineering"}),
				chroma.NewIntArrayAttribute("scores", []int64{88, 91}),
			),
			chroma.NewDocumentMetadata(
				chroma.NewStringArrayAttribute("tags", []string{"physics", "quantum", "ai"}),
				chroma.NewIntArrayAttribute("scores", []int64{96, 94, 99}),
			),
		),
	)
	if err != nil {
		log.Fatalf("Error adding documents: %s\n", err)
	}

	fmt.Println("Added 3 documents with array metadata")

	// Query: find documents where tags contain "ai"
	qr, err := col.Query(ctx,
		chroma.WithQueryTexts("artificial intelligence"),
		chroma.WithWhere(chroma.MetadataContainsString(chroma.K("tags"), "ai")),
		chroma.WithInclude(chroma.IncludeDocuments, chroma.IncludeMetadatas),
	)
	if err != nil {
		log.Fatalf("Error querying: %s\n", err)
	}
	fmt.Printf("Documents with tag 'ai': %v\n", qr.GetDocumentsGroups()[0])

	// Query: find documents where tags do NOT contain "math"
	qr2, err := col.Query(ctx,
		chroma.WithQueryTexts("science"),
		chroma.WithWhere(chroma.MetadataNotContainsString(chroma.K("tags"), "math")),
		chroma.WithInclude(chroma.IncludeDocuments, chroma.IncludeMetadatas),
	)
	if err != nil {
		log.Fatalf("Error querying: %s\n", err)
	}
	fmt.Printf("Documents without tag 'math': %v\n", qr2.GetDocumentsGroups()[0])

	// Combine array contains with other filters
	qr3, err := col.Query(ctx,
		chroma.WithQueryTexts("computing"),
		chroma.WithWhere(
			chroma.And(
				chroma.MetadataContainsString(chroma.K("tags"), "ai"),
				chroma.MetadataContainsInt(chroma.K("scores"), 99),
			),
		),
		chroma.WithInclude(chroma.IncludeDocuments),
	)
	if err != nil {
		log.Fatalf("Error querying: %s\n", err)
	}
	fmt.Printf("Documents with tag 'ai' and score 99: %v\n", qr3.GetDocumentsGroups()[0])

	// Clean up
	err = col.Delete(ctx, chroma.WithIDs("doc1", "doc2", "doc3"))
	if err != nil {
		log.Fatalf("Error deleting: %s\n", err)
	}
	fmt.Println("Cleanup complete")
}
