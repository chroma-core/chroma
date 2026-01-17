package main

import (
	"context"
	"fmt"
	chroma "github.com/chroma-core/chroma/clients/go"
	"log"
	"math/rand"
)

func main() {
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
	r := rand.Int()
	tenant, err := client.CreateTenant(context.Background(), chroma.NewTenant(fmt.Sprintf("tenant-%d", r)))
	if err != nil {
		log.Fatalf("Error creating tenant: %s \n", err)
		return
	}
	fmt.Printf("Created tenant %v\n", tenant)
	db1, err := client.CreateDatabase(context.Background(), tenant.Database("db2"))
	if err != nil {
		log.Fatalf("Error creating database: %s \n", err)
		return
	}
	col, err := client.GetOrCreateCollection(context.Background(), "col1",
		chroma.WithDatabaseCreate(db1), chroma.WithCollectionMetadataCreate(
			chroma.NewMetadata(
				chroma.NewStringAttribute("str", "hello"),
				chroma.NewIntAttribute("int", 1),
				chroma.NewFloatAttribute("float", 1.1),
			),
		),
	)
	if err != nil {
		log.Fatalf("Error creating collection: %s \n", err)
		return
	}
	fmt.Printf("Created collection %v+\n", col)

	err = col.Add(context.Background(),
		//chroma.WithIDGenerator(chroma.NewULIDGenerator()),
		chroma.WithIDs("1", "2"),
		chroma.WithTexts("hello world", "goodbye world"),
		chroma.WithMetadatas(
			chroma.NewDocumentMetadata(chroma.NewIntAttribute("int", 1)),
			chroma.NewDocumentMetadata(chroma.NewStringAttribute("str", "hello")),
		))
	if err != nil {
		log.Fatalf("Error adding collection: %s \n", err)
	}

	colCount, err := client.CountCollections(context.Background(), chroma.WithDatabaseCount(db1))
	if err != nil {
		log.Fatalf("Error counting collections: %s \n", err)
		return
	}
	fmt.Printf("Count collections in %s : %d\n", db1.String(), colCount)
	cols, err := client.ListCollections(context.Background(), chroma.WithDatabaseList(db1))
	if err != nil {
		log.Fatalf("Error listing collections: %s \n", err)
		return
	}
	fmt.Printf("List collections in %s : %d\n", db1.String(), len(cols))

	qr, err := col.Query(context.Background(), chroma.WithQueryTexts("say hello"))
	if err != nil {
		log.Fatalf("Error querying collection: %s \n", err)
		return
	}
	fmt.Printf("Query result: %v\n", qr.GetDocumentsGroups()[0][0])
	err = col.Delete(context.Background(), chroma.WithIDsDelete("1", "2"))
	if err != nil {
		log.Fatalf("Error deleting collection: %s \n", err)
		return
	}
	fmt.Printf("Deleted items from collection %s\n", col.Name())

	err = client.DeleteCollection(context.Background(), "col1", chroma.WithDatabaseDelete(db1))
	if err != nil {
		log.Fatalf("Error deleting collection: %s \n", err)
		return
	}
	err = client.DeleteDatabase(context.Background(), db1)
	if err != nil {
		log.Fatalf("Error deleting database: %s \n", err)
		return
	}
	fmt.Printf("Deleted database %s\n", db1)
}
