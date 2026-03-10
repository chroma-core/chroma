package main

import (
	"context"
	"fmt"
	"log"
	"os"

	chroma "github.com/chroma-core/chroma/clients/go"
	openai "github.com/chroma-core/chroma/clients/go/pkg/embeddings/openai"
)

func main() {
	// Create a new Chroma client
	// Note: WithDebug() is deprecated - use WithLogger with debug level for logging
	client, err := chroma.NewHTTPClient()
	if err != nil {
		log.Fatalf("Error creating client: %s \n", err)
		return
	}
	// Close the client to release any resources such as local embedding functions
	defer func() {
		err = client.Close()
		if err != nil {
			log.Fatalf("Error closing client: %s \n", err)
		}
	}()

	ef, err := openai.NewOpenAIEmbeddingFunction(os.Getenv("OPENAI_API_KEY"), openai.WithModel(openai.TextEmbedding3Small))
	if err != nil {
		log.Fatalf("Error creating embedding function: %s \n", err)
		return
	}

	// Create a new collection with options. We don't provide an embedding function here, so the default embedding function will be used
	col, err := client.GetOrCreateCollection(context.Background(), "openai-embedding-function-test",
		chroma.WithCollectionMetadataCreate(
			chroma.NewMetadata(
				chroma.NewStringAttribute("str", "hello2"),
				chroma.NewIntAttribute("int", 1),
				chroma.NewFloatAttribute("float", 1.1),
			),
		),
		chroma.WithEmbeddingFunctionCreate(ef),
	)
	if err != nil {
		log.Fatalf("Error creating collection: %s \n", err)
		return
	}

	err = col.Add(context.Background(),
		//chroma.WithIDGenerator(chroma.NewULIDGenerator()),
		chroma.WithIDs("1", "2"),
		chroma.WithTexts("hello world", "goodbye world"),
		chroma.WithMetadatas(
			chroma.NewDocumentMetadata(chroma.NewIntAttribute("int", 1)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("str1", "hello2")),
		))
	if err != nil {
		log.Fatalf("Error adding collection: %s \n", err)
	}

	count, err := col.Count(context.Background())
	if err != nil {
		log.Fatalf("Error counting collection: %s \n", err)
		return
	}
	fmt.Printf("Count collection: %d\n", count)

	qr, err := col.Query(context.Background(), chroma.WithQueryTexts("say hello"))
	if err != nil {
		log.Fatalf("Error querying collection: %s \n", err)
		return
	}
	fmt.Printf("Query result: %v\n", qr.GetDocumentsGroups()[0][0])

	err = col.Delete(context.Background(), chroma.WithIDs("1", "2"))
	if err != nil {
		log.Fatalf("Error deleting collection: %s \n", err)
		return
	}
}
