package coordinator

import (
	"context"
	"errors"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// ICoordinator is an interface that defines the methods for interacting with the
// Chroma Coordinator. It is designed in a way that can be run standalone without
// spinning off the GRPC service.
type ICoordinator interface {
	common.Component
	ResetState(ctx context.Context) error
	CreateCollection(ctx context.Context, createCollection *model.CreateCollection) (*model.Collection, error)
	GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string, tenantID string, dataName string) ([]*model.Collection, error)
	DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error
	UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection) (*model.Collection, error)
	CreateSegment(ctx context.Context, createSegment *model.CreateSegment) error
	GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*model.Segment, error)
	DeleteSegment(ctx context.Context, segmentID types.UniqueID) error
	UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment) (*model.Segment, error)
	CreateDatabase(ctx context.Context, createDatabase *model.CreateDatabase) (*model.Database, error)
	GetDatabase(ctx context.Context, getDatabase *model.GetDatabase) (*model.Database, error)
	CreateTenant(ctx context.Context, createTenant *model.CreateTenant) (*model.Tenant, error)
	GetTenant(ctx context.Context, getTenant *model.GetTenant) (*model.Tenant, error)
}

func (s *Coordinator) ResetState(ctx context.Context) error {
	return s.meta.ResetState(ctx)
}

func (s *Coordinator) CreateDatabase(ctx context.Context, createDatabase *model.CreateDatabase) (*model.Database, error) {
	database, err := s.meta.CreateDatabase(ctx, createDatabase)
	if err != nil {
		return nil, err
	}
	return database, nil
}

func (s *Coordinator) GetDatabase(ctx context.Context, getDatabase *model.GetDatabase) (*model.Database, error) {
	database, err := s.meta.GetDatabase(ctx, getDatabase)
	if err != nil {
		return nil, err
	}
	return database, nil
}

func (s *Coordinator) CreateTenant(ctx context.Context, createTenant *model.CreateTenant) (*model.Tenant, error) {
	tenant, err := s.meta.CreateTenant(ctx, createTenant)
	if err != nil {
		return nil, err
	}
	return tenant, nil
}

func (s *Coordinator) GetTenant(ctx context.Context, getTenant *model.GetTenant) (*model.Tenant, error) {
	tenant, err := s.meta.GetTenant(ctx, getTenant)
	if err != nil {
		return nil, err
	}
	return tenant, nil
}

func (s *Coordinator) CreateCollection(ctx context.Context, createCollection *model.CreateCollection) (*model.Collection, error) {
	collectionTopic, err := s.assignCollection(createCollection.ID)
	if err != nil {
		return nil, err
	}
	createCollection.Topic = collectionTopic
	log.Info("apis create collection", zap.Any("collection", createCollection))
	collection, err := s.meta.AddCollection(ctx, createCollection)
	if err != nil {
		return nil, err
	}
	return collection, nil
}

func (s *Coordinator) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string, tenantID string, databaseName string) ([]*model.Collection, error) {
	return s.meta.GetCollections(ctx, collectionID, collectionName, collectionTopic, tenantID, databaseName)
}

func (s *Coordinator) DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	return s.meta.DeleteCollection(ctx, deleteCollection)
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
