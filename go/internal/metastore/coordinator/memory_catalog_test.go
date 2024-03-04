package coordinator

import (
	"context"
	"testing"

	"github.com/chroma-core/chroma/go/internal/model"
	"github.com/chroma-core/chroma/go/internal/notification"
	"github.com/chroma-core/chroma/go/internal/types"
)

const (
	defaultTenant   = "default_tenant"
	defaultDatabase = "default_database"
)

func TestMemoryCatalog(t *testing.T) {
	ctx := context.Background()
	store := notification.NewMemoryNotificationStore()
	mc := NewMemoryCatalogWithNotification(store)

	// Test CreateCollection
	coll := &model.CreateCollection{
		ID:   types.NewUniqueID(),
		Name: "test-collection-name",
		// Topic: "test-collection-topic",
		Metadata: &model.CollectionMetadata[model.CollectionMetadataValueType]{
			Metadata: map[string]model.CollectionMetadataValueType{
				"test-metadata-key": &model.CollectionMetadataValueStringType{Value: "test-metadata-value"},
			},
		},
		TenantID:     defaultTenant,
		DatabaseName: defaultDatabase,
	}
	collection, err := mc.CreateCollection(ctx, coll, types.Timestamp(0))
	if err != nil {
		t.Fatalf("unexpected error creating collection: %v", err)
	}
	// Test GetCollections
	collections, err := mc.GetCollections(ctx, coll.ID, &coll.Name, nil, defaultTenant, defaultDatabase)
	if err != nil {
		t.Fatalf("unexpected error getting collections: %v", err)
	}
	if len(collections) != 1 {
		t.Fatalf("expected 1 collection, got %d", len(collections))
	}
	if collections[0] != collection {
		t.Fatalf("expected collection %+v, got %+v", coll, collections[0])
	}

	// Test DeleteCollection
	deleteCollection := &model.DeleteCollection{
		ID:           coll.ID,
		DatabaseName: defaultDatabase,
		TenantID:     defaultTenant,
	}
	if err := mc.DeleteCollection(ctx, deleteCollection); err != nil {
		t.Fatalf("unexpected error deleting collection: %v", err)
	}

	// Test CreateSegment
	testTopic := "test-segment-topic"
	createSegment := &model.CreateSegment{
		ID:           types.NewUniqueID(),
		Type:         "test-segment-type",
		Scope:        "test-segment-scope",
		Topic:        &testTopic,
		CollectionID: coll.ID,
		Metadata: &model.SegmentMetadata[model.SegmentMetadataValueType]{
			Metadata: map[string]model.SegmentMetadataValueType{
				"test-metadata-key": &model.SegmentMetadataValueStringType{Value: "test-metadata-value"},
			},
		},
	}
	segment, err := mc.CreateSegment(ctx, createSegment, types.Timestamp(0))
	if err != nil {
		t.Fatalf("unexpected error creating segment: %v", err)
	}
	if len(mc.segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(mc.segments))
	}

	if mc.segments[createSegment.ID] != segment {
		t.Fatalf("expected segment with ID %q, got %+v", createSegment.ID, mc.segments[createSegment.ID])
	}

	// Test GetSegments
	segments, err := mc.GetSegments(ctx, createSegment.ID, &createSegment.Type, &createSegment.Scope, createSegment.Topic, coll.ID, types.Timestamp(0))
	if err != nil {
		t.Fatalf("unexpected error getting segments: %v", err)
	}
	if len(segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(segments))
	}
	if segments[0] != segment {
		t.Fatalf("expected segment %+v, got %+v", createSegment, segments[0])
	}

	// Test CreateCollection
	coll = &model.CreateCollection{
		ID:   types.NewUniqueID(),
		Name: "test-collection-name",
		// Topic: "test-collection-topic",
		Metadata: &model.CollectionMetadata[model.CollectionMetadataValueType]{
			Metadata: map[string]model.CollectionMetadataValueType{
				"test-metadata-key": &model.CollectionMetadataValueStringType{Value: "test-metadata-value"},
			},
		},
		TenantID:     defaultTenant,
		DatabaseName: defaultDatabase,
	}
	collection, err = mc.CreateCollection(ctx, coll, types.Timestamp(0))
	if err != nil {
		t.Fatalf("unexpected error creating collection: %v", err)
	}

	// Test GetCollections
	collections, err = mc.GetCollections(ctx, coll.ID, &coll.Name, nil, defaultTenant, defaultDatabase)
	if err != nil {
		t.Fatalf("unexpected error getting collections: %v", err)
	}
	if len(collections) != 1 {
		t.Fatalf("expected 1 collection, got %d", len(collections))
	}
	if collections[0] != collection {
		t.Fatalf("expected collection %+v, got %+v", coll, collections[0])
	}

	// Test DeleteCollection
	deleteCollection = &model.DeleteCollection{
		ID:           coll.ID,
		DatabaseName: defaultDatabase,
		TenantID:     defaultTenant,
	}
	if err := mc.DeleteCollection(ctx, deleteCollection); err != nil {
		t.Fatalf("unexpected error deleting collection: %v", err)
	}
}
