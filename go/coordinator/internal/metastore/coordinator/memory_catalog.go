package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/metastore"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// MemoryCatalog is a reference implementation of Catalog interface to ensure the
// application logic is correctly implemented.
type MemoryCatalog struct {
	Collections map[types.UniqueID]*model.Collection
	Segments    map[types.UniqueID]*model.Segment
}

var _ metastore.Catalog = (*MemoryCatalog)(nil)

func NewMemoryCatalog() *MemoryCatalog {
	return &MemoryCatalog{
		Collections: make(map[types.UniqueID]*model.Collection),
		Segments:    make(map[types.UniqueID]*model.Segment),
	}
}

func (mc *MemoryCatalog) ResetState(ctx context.Context) error {
	mc.Collections = make(map[types.UniqueID]*model.Collection)
	mc.Segments = make(map[types.UniqueID]*model.Segment)
	return nil
}

func (mc *MemoryCatalog) CreateCollection(ctx context.Context, createCollection *model.CreateCollection, ts types.Timestamp) (*model.Collection, error) {
	if _, ok := mc.Collections[createCollection.ID]; ok {
		return nil, common.ErrCollectionUniqueConstraintViolation
	}

	collection := &model.Collection{
		ID:        createCollection.ID,
		Name:      createCollection.Name,
		Topic:     createCollection.Topic,
		Dimension: createCollection.Dimension,
		Metadata:  createCollection.Metadata,
	}
	mc.Collections[collection.ID] = collection
	return collection, nil
}

func (mc *MemoryCatalog) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error) {
	collections := make([]*model.Collection, 0, len(mc.Collections))
	for _, collection := range mc.Collections {
		if model.FilterCollection(collection, collectionID, collectionName, collectionTopic) {
			collections = append(collections, collection)
		}
	}
	log.Debug("collections", zap.Any("collections", collections))
	return collections, nil
}

func (mc *MemoryCatalog) DeleteCollection(ctx context.Context, collectionID types.UniqueID) error {
	if _, ok := mc.Collections[collectionID]; !ok {
		return common.ErrCollectionDeleteNonExistingCollection
	}
	delete(mc.Collections, collectionID)
	return nil
}

func (mc *MemoryCatalog) UpdateCollection(ctx context.Context, coll *model.UpdateCollection, ts types.Timestamp) (*model.Collection, error) {
	oldCollection := mc.Collections[coll.ID]
	topic := coll.Topic
	if topic != nil {
		oldCollection.Topic = *topic
	}
	name := coll.Name
	if name != nil {
		oldCollection.Name = *name
	}
	if coll.Dimension != nil {
		oldCollection.Dimension = coll.Dimension
	}

	// Case 1: if resetMetadata is true, then delete all metadata for the collection
	// Case 2: if resetMetadata is true and metadata is not nil -> THIS SHOULD NEVER HAPPEN
	// Case 3: if resetMetadata is false, and the metadata is not nil - set the metadata to the value in metadata
	// Case 4: if resetMetadata is false and metadata is nil, then leave the metadata as is
	resetMetadata := coll.ResetMetadata
	if resetMetadata {
		oldCollection.Metadata = nil
	} else {
		if coll.Metadata != nil {
			oldCollection.Metadata = coll.Metadata
		}
	}
	mc.Collections[coll.ID] = oldCollection
	// Better to return a copy of the collection to avoid being modified by others.
	log.Debug("collection metadata", zap.Any("metadata", oldCollection.Metadata))
	return oldCollection, nil
}

func (mc *MemoryCatalog) CreateSegment(ctx context.Context, createSegment *model.CreateSegment, ts types.Timestamp) (*model.Segment, error) {
	if _, ok := mc.Segments[createSegment.ID]; ok {
		return nil, common.ErrSegmentUniqueConstraintViolation
	}

	segment := &model.Segment{
		ID:           createSegment.ID,
		Topic:        createSegment.Topic,
		Type:         createSegment.Type,
		Scope:        createSegment.Scope,
		CollectionID: createSegment.CollectionID,
		Metadata:     createSegment.Metadata,
	}
	mc.Segments[createSegment.ID] = segment
	log.Debug("segment created", zap.Any("segment", segment))
	return segment, nil
}

func (mc *MemoryCatalog) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID, ts types.Timestamp) ([]*model.Segment, error) {
	segments := make([]*model.Segment, 0, len(mc.Segments))
	for _, segment := range mc.Segments {
		if model.FilterSegments(segment, segmentID, segmentType, scope, topic, collectionID) {
			segments = append(segments, segment)
		}
	}
	return segments, nil
}

func (mc *MemoryCatalog) DeleteSegment(ctx context.Context, segmentID types.UniqueID) error {
	if _, ok := mc.Segments[segmentID]; !ok {
		return common.ErrSegmentDeleteNonExistingSegment
	}

	delete(mc.Segments, segmentID)
	return nil
}

func (mc *MemoryCatalog) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment, ts types.Timestamp) (*model.Segment, error) {
	// Case 1: if ResetTopic is true and topic is nil, then set the topic to nil
	// Case 2: if ResetTopic is true and topic is not nil -> THIS SHOULD NEVER HAPPEN
	// Case 3: if ResetTopic is false and topic is not nil - set the topic to the value in topic
	// Case 4: if ResetTopic is false and topic is nil, then leave the topic as is
	oldSegment := mc.Segments[updateSegment.ID]
	topic := updateSegment.Topic
	if updateSegment.ResetTopic {
		if topic == nil {
			oldSegment.Topic = nil
		}
	} else {
		if topic != nil {
			oldSegment.Topic = topic
		}
	}
	collection := updateSegment.Collection
	if updateSegment.ResetCollection {
		if collection == nil {
			oldSegment.CollectionID = types.NilUniqueID()
		}
	} else {
		if collection != nil {
			parsedCollectionID, err := types.ToUniqueID(collection)
			if err != nil {
				return nil, err
			}
			oldSegment.CollectionID = parsedCollectionID
		}
	}
	resetMetadata := updateSegment.ResetMetadata
	if resetMetadata {
		oldSegment.Metadata = nil
	} else {
		if updateSegment.Metadata != nil {
			for key, value := range updateSegment.Metadata.Metadata {
				if value == nil {
					updateSegment.Metadata.Remove(key)
				} else {
					updateSegment.Metadata.Set(key, value)
				}
			}
		}
	}
	mc.Segments[updateSegment.ID] = oldSegment
	return oldSegment, nil
}
