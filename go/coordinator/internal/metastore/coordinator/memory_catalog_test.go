package coordinator

import (
	"context"
	"testing"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

func TestMemoryCatalog(t *testing.T) {
	ctx := context.Background()
	mc := NewMemoryCatalog()

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
	}
	collection, err := mc.CreateCollection(ctx, coll, types.Timestamp(0))
	if err != nil {
		t.Fatalf("unexpected error creating collection: %v", err)
	}
	if len(mc.Collections) != 1 {
		t.Fatalf("expected 1 collection, got %d", len(mc.Collections))
	}

	if mc.Collections[coll.ID] != collection {
		t.Fatalf("expected collection with ID %q, got %+v", coll.ID, mc.Collections[coll.ID])
	}

	// Test GetCollections
	collections, err := mc.GetCollections(ctx, coll.ID, &coll.Name, nil)
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
	if err := mc.DeleteCollection(ctx, coll.ID); err != nil {
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
	if len(mc.Segments) != 1 {
		t.Fatalf("expected 1 segment, got %d", len(mc.Segments))
	}

	if mc.Segments[createSegment.ID] != segment {
		t.Fatalf("expected segment with ID %q, got %+v", createSegment.ID, mc.Segments[createSegment.ID])
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
	}
	collection, err = mc.CreateCollection(ctx, coll, types.Timestamp(0))
	if err != nil {
		t.Fatalf("unexpected error creating collection: %v", err)
	}
	if len(mc.Collections) != 1 {
		t.Fatalf("expected 1 collection, got %d", len(mc.Collections))
	}
	if mc.Collections[coll.ID] != collection {
		t.Fatalf("expected collection with ID %q, got %+v", coll.ID, mc.Collections[coll.ID])
	}

	// Test GetCollections
	collections, err = mc.GetCollections(ctx, coll.ID, &coll.Name, nil)
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
	if err := mc.DeleteCollection(ctx, coll.ID); err != nil {
		t.Fatalf("unexpected error deleting collection: %v", err)
	}
}
