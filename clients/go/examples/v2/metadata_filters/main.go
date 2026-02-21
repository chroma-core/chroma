package main

import (
	"context"
	"fmt"
	"log"

	chroma "github.com/chroma-core/chroma/clients/go"
)

func main() {
	// Create a new Chroma client
	// Note: WithDebug() is deprecated - use WithLogger with debug level for logging
	client, err := chroma.NewHTTPClient()
	if err != nil {
		log.Printf("Error creating client: %s \n", err)
		return
	}
	// Close the client to release any resources such as local embedding functions
	defer func() {
		err = client.Close()
		if err != nil {
			log.Printf("Error closing client: %s \n", err)
			return
		}
	}()

	// Create a new collection with options. We don't provide an embedding function here, so the default embedding function will be used
	col, err := client.GetOrCreateCollection(context.Background(), "col1",
		chroma.WithCollectionMetadataCreate(
			chroma.NewMetadata(
				chroma.NewStringAttribute("str", "hello2"),
				chroma.NewIntAttribute("int", 1),
				chroma.NewFloatAttribute("float", 1.1),
			),
		),
	)
	if err != nil {
		_ = client.Close() // Ensure the client is closed before exiting
		log.Printf("Error creating collection: %s \n", err)
		return
	}

	err = col.Add(context.Background(),
		chroma.WithIDs("1", "2"),
		chroma.WithTexts("hello world", "goodbye world"),
		chroma.WithMetadatas(
			chroma.NewDocumentMetadata(
				chroma.NewIntAttribute("int", 1),
				chroma.NewStringArrayAttribute("tags", []string{"greeting", "english"}),
			),
			chroma.NewDocumentMetadata(
				chroma.NewStringAttribute("str1", "hello2"),
				chroma.NewStringArrayAttribute("tags", []string{"farewell", "english"}),
			),
		))
	if err != nil {
		log.Printf("Error adding collection: %s \n", err)
		return
	}

	count, err := col.Count(context.Background())
	if err != nil {
		log.Printf("Error counting collection: %s \n", err)
		return
	}
	fmt.Printf("Count collection: %d\n", count)
	// Use K() to clearly mark field names in filters
	IntFilter := chroma.EqInt(chroma.K("int"), 1)
	StringFilter := chroma.EqString(chroma.K("str1"), "hello2")
	qr, err := col.Query(context.Background(),
		chroma.WithQueryTexts("say hello"),
		chroma.WithInclude(chroma.IncludeDocuments, chroma.IncludeMetadatas),
		// Example with a single filter:
		// chroma.WithWhere(StringFilter)

		// Example with multiple combined filters:
		chroma.WithWhere(
			chroma.Or(StringFilter, IntFilter),
		),
	)
	if err != nil {
		log.Printf("Error querying collection: %s \n", err)
		return
	}
	fmt.Printf("Query result expected: 'hello world', actual: '%v'\n", qr.GetDocumentsGroups()[0][0]) // goodbye world is also returned because of the OR filter

	// Example with array contains filter (Chroma >= 1.5.0)
	arrayFilter := chroma.MetadataContainsString(chroma.K("tags"), "greeting")
	qr2, err := col.Query(context.Background(),
		chroma.WithQueryTexts("say hello"),
		chroma.WithInclude(chroma.IncludeDocuments, chroma.IncludeMetadatas),
		chroma.WithWhere(arrayFilter),
	)
	if err != nil {
		log.Printf("Error querying collection with array filter: %s \n", err)
		return
	}
	fmt.Printf("Array filter result expected: 'hello world', actual: '%v'\n", qr2.GetDocumentsGroups()[0][0])

	err = col.Delete(context.Background(), chroma.WithIDs("1", "2"))
	if err != nil {
		log.Printf("Error deleting collection: %s \n", err)
		return
	}
}
