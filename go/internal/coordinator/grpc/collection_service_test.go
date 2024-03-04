package grpc

import (
	"context"
	"github.com/chroma-core/chroma/go/internal/grpcutils"
	"testing"

	"github.com/chroma-core/chroma/go/internal/common"
	"github.com/chroma-core/chroma/go/internal/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/internal/proto/coordinatorpb"
	"pgregory.net/rapid"
)

// CreateCollection
// Collection created successfully are visible to ListCollections
// Collection created should have the right metadata, the metadata should be a flat map, with keys as strings and values as strings, ints, or floats
// Collection created should have the right name
// Collection created should have the right ID
// Collection created should have the right topic
// Collection created should have the right timestamp
func testCollection(t *rapid.T) {
	db := dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		AssignmentPolicy:          "simple",
		SystemCatalogProvider:     "memory",
		NotificationStoreProvider: "memory",
		NotifierProvider:          "memory",
		Testing:                   true}, grpcutils.Default, db)
	if err != nil {
		t.Fatalf("error creating server: %v", err)
	}
	var state []*coordinatorpb.Collection
	var collectionsWithErrors []*coordinatorpb.Collection

	t.Repeat(map[string]func(*rapid.T){
		"create_collection": func(t *rapid.T) {
			stringValue := generateStringMetadataValue(t)
			intValue := generateInt64MetadataValue(t)
			floatValue := generateFloat64MetadataValue(t)
			getOrCreate := false

			createCollectionRequest := rapid.Custom[*coordinatorpb.CreateCollectionRequest](func(t *rapid.T) *coordinatorpb.CreateCollectionRequest {
				return &coordinatorpb.CreateCollectionRequest{
					Id:   rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "collection_id"),
					Name: rapid.String().Draw(t, "collection_name"),
					Metadata: &coordinatorpb.UpdateMetadata{
						Metadata: map[string]*coordinatorpb.UpdateMetadataValue{
							"string_value": stringValue,
							"int_value":    intValue,
							"float_value":  floatValue,
						},
					},
					GetOrCreate: &getOrCreate,
				}
			}).Draw(t, "create_collection_request")

			ctx := context.Background()
			res, err := s.CreateCollection(ctx, createCollectionRequest)
			if err != nil {
				if err == common.ErrCollectionNameEmpty && createCollectionRequest.Name == "" {
					t.Logf("expected error for empty collection name")
					collectionsWithErrors = append(collectionsWithErrors, res.Collection)
				} else if err == common.ErrCollectionTopicEmpty {
					t.Logf("expected error for empty collection topic")
					collectionsWithErrors = append(collectionsWithErrors, res.Collection)
					// TODO: check the topic name not empty
				} else {
					t.Fatalf("error creating collection: %v", err)
					collectionsWithErrors = append(collectionsWithErrors, res.Collection)
				}
			}

			getCollectionsRequest := coordinatorpb.GetCollectionsRequest{
				Id: &createCollectionRequest.Id,
			}
			if err == nil {
				// verify the correctness
				GetCollectionsResponse, err := s.GetCollections(ctx, &getCollectionsRequest)
				if err != nil {
					t.Fatalf("error getting collections: %v", err)
				}
				collectionList := GetCollectionsResponse.GetCollections()
				if len(collectionList) != 1 {
					t.Fatalf("More than 1 collection with the same collection id")
				}
				for _, collection := range collectionList {
					if collection.Id != createCollectionRequest.Id {
						t.Fatalf("collection id is the right value")
					}
				}
				state = append(state, res.Collection)
			}
		},
		"get_collections": func(t *rapid.T) {
		},
	})
}

func generateStringMetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_StringValue{
			StringValue: rapid.String().Draw(t, "string_value"),
		},
	}
}

func generateInt64MetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_IntValue{
			IntValue: rapid.Int64().Draw(t, "int_value"),
		},
	}
}

func generateFloat64MetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
			FloatValue: rapid.Float64().Draw(t, "float_value"),
		},
	}
}

func TestCollection(t *testing.T) {
	// rapid.Check(t, testCollection)
}
