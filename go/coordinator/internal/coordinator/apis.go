package coordinator

import (
	"context"
	"errors"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

// ICoordinator is an interface that defines the methods for interacting with the
// Chroma Coordinator. It is designed in a way that can be run standalone without
// spinning off the GRPC service.
type ICoordinator interface {
	common.Component
	ResetState(ctx context.Context) error
	CreateCollection(ctx context.Context, collection *model.CreateCollection) (*model.Collection, error)
	GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error)
	DeleteCollection(ctx context.Context, collectionID types.UniqueID) error
	UpdateCollection(ctx context.Context, collection *model.UpdateCollection) (*model.Collection, error)
	CreateSegment(ctx context.Context, segment *model.CreateSegment) error
	GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*model.Segment, error)
	DeleteSegment(ctx context.Context, segmentID types.UniqueID) error
	UpdateSegment(ctx context.Context, segment *model.UpdateSegment) (*model.Segment, error)
}

func (s *Coordinator) ResetState(ctx context.Context) error {
	return s.meta.ResetState(ctx)
}

func (s *Coordinator) CreateCollection(ctx context.Context, createCollection *model.CreateCollection) (*model.Collection, error) {
	collectionTopic := s.assignCollection(createCollection.ID)
	createCollection.Topic = collectionTopic

	collection, err := s.meta.AddCollection(ctx, createCollection)
	if err != nil {
		return nil, err
	}
	return collection, nil
}

func (s *Coordinator) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error) {
	return s.meta.GetCollections(ctx, collectionID, collectionName, collectionTopic)
}

func (s *Coordinator) DeleteCollection(ctx context.Context, collectionID types.UniqueID) error {
	return s.meta.DeleteCollection(ctx, collectionID)
}

func (s *Coordinator) UpdateCollection(ctx context.Context, collection *model.UpdateCollection) (*model.Collection, error) {
	return s.meta.UpdateCollection(ctx, collection)
}

func (s *Coordinator) CreateSegment(ctx context.Context, segment *model.CreateSegment) error {
	if err := verifyCreateSegment(segment); err != nil {
		return err
	}
	err := s.meta.AddSegment(ctx, segment)
	if err != nil {
		return err
	}
	return nil
}

func (s *Coordinator) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*model.Segment, error) {
	return s.meta.GetSegments(ctx, segmentID, segmentType, scope, topic, collectionID)
}

func (s *Coordinator) DeleteSegment(ctx context.Context, segmentID types.UniqueID) error {
	return s.meta.DeleteSegment(ctx, segmentID)
}

func (s *Coordinator) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment) (*model.Segment, error) {
	segment, err := s.meta.UpdateSegment(ctx, updateSegment)
	if err != nil {
		return nil, err
	}
	return segment, nil

}

func verifyCreateCollection(collection *model.CreateCollection) error {
	if collection.ID.String() == "" {
		return errors.New("collection ID cannot be empty")
	}
	if err := verifyCollectionMetadata(collection.Metadata); err != nil {
		return err
	}
	return nil
}

func verifyCollectionMetadata(metadata *model.CollectionMetadata[model.CollectionMetadataValueType]) error {
	if metadata == nil {
		return nil
	}
	for _, value := range metadata.Metadata {
		switch (value).(type) {
		case *model.CollectionMetadataValueStringType:
		case *model.CollectionMetadataValueInt64Type:
		case *model.CollectionMetadataValueFloat64Type:
		default:
			return common.ErrUnknownCollectionMetadataType
		}
	}
	return nil
}

func verifyUpdateCollection(collection *model.UpdateCollection) error {
	if collection.ID.String() == "" {
		return errors.New("collection ID cannot be empty")
	}
	if err := verifyCollectionMetadata(collection.Metadata); err != nil {
		return err
	}
	return nil
}

func verifyCreateSegment(segment *model.CreateSegment) error {
	if err := verifySegmentMetadata(segment.Metadata); err != nil {
		return err
	}
	return nil
}

func VerifyUpdateSegment(segment *model.UpdateSegment) error {
	if err := verifySegmentMetadata(segment.Metadata); err != nil {
		return err
	}
	return nil
}

func verifySegmentMetadata(metadata *model.SegmentMetadata[model.SegmentMetadataValueType]) error {
	if metadata == nil {
		return nil
	}
	for _, value := range metadata.Metadata {
		switch (value).(type) {
		case *model.SegmentMetadataValueStringType:
		case *model.SegmentMetadataValueInt64Type:
		case *model.SegmentMetadataValueFloat64Type:
		default:
			return common.ErrUnknownSegmentMetadataType
		}
	}
	return nil
}
