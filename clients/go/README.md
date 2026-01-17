## ChromaDB Go Client

Official Go client for [Chroma](https://www.trychroma.com/) - the open-source embedding database.

### Installation

```bash
go get github.com/chroma-core/chroma/clients/go
```

### Quick Start

```go
package main

import (
	"context"
	"log"

	chroma "github.com/chroma-core/chroma/clients/go"
)

func main() {
	client, err := chroma.NewHTTPClient()
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	col, err := client.GetOrCreateCollection(context.Background(), "my-collection")
	if err != nil {
		log.Fatal(err)
	}

	err = col.Add(context.Background(),
		chroma.WithIDs("doc1", "doc2"),
		chroma.WithTexts("hello world", "goodbye world"),
	)
	if err != nil {
		log.Fatal(err)
	}

	results, err := col.Query(context.Background(),
		chroma.WithQueryTexts("hello"),
		chroma.WithNResults(1),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Results: %v", results.GetDocumentsGroups())
}
```

