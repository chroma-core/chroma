package coordinator

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
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
	SetTenantLastCompactionTime(ctx context.Context, tenantID string, lastCompactionTime int64) error
	GetTenantsLastCompactionTime(ctx context.Context, tenantIDs []string) ([]*dbmodel.Tenant, error)
	FlushCollectionCompaction(ctx context.Context, flushCollectionCompaction *model.FlushCollectionCompaction) (*model.FlushCollectionInfo, error)
}

func (s *Coordinator) ResetState(ctx context.Context) error {
	return s.catalog.ResetState(ctx)
}

func (s *Coordinator) CreateDatabase(ctx context.Context, createDatabase *model.CreateDatabase) (*model.Database, error) {
	database, err := s.catalog.CreateDatabase(ctx, createDatabase, createDatabase.Ts)
	if err != nil {
		return nil, err
	}
	return database, nil
}

func (s *Coordinator) GetDatabase(ctx context.Context, getDatabase *model.GetDatabase) (*model.Database, error) {
	database, err := s.catalog.GetDatabases(ctx, getDatabase, getDatabase.Ts)
	if err != nil {
		return nil, err
	}
	return database, nil
}

func (s *Coordinator) CreateTenant(ctx context.Context, createTenant *model.CreateTenant) (*model.Tenant, error) {
	tenant, err := s.catalog.CreateTenant(ctx, createTenant, createTenant.Ts)
	if err != nil {
		return nil, err
	}
	return tenant, nil
}

func (s *Coordinator) GetTenant(ctx context.Context, getTenant *model.GetTenant) (*model.Tenant, error) {
	tenant, err := s.catalog.GetTenants(ctx, getTenant, getTenant.Ts)
	if err != nil {
		return nil, err
	}
	return tenant, nil
}

func (s *Coordinator) CreateCollection(ctx context.Context, createCollection *model.CreateCollection) (*model.Collection, error) {
	log.Info("create collection", zap.Any("createCollection", createCollection))
	collectionTopic, err := s.assignCollection(createCollection.ID)
	if err != nil {
		return nil, err
	}
	createCollection.Topic = collectionTopic
	collection, err := s.catalog.CreateCollection(ctx, createCollection, createCollection.Ts)
	if err != nil {
		return nil, err
	}
	return collection, nil
}

func (s *Coordinator) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string, tenantID string, databaseName string) ([]*model.Collection, error) {
	return s.catalog.GetCollections(ctx, collectionID, collectionName, collectionTopic, tenantID, databaseName)
}

func (s *Coordinator) DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	return s.catalog.DeleteCollection(ctx, deleteCollection)
}

func (s *Coordinator) UpdateCollection(ctx context.Context, collection *model.UpdateCollection) (*model.Collection, error) {
	return s.catalog.UpdateCollection(ctx, collection, collection.Ts)
}

func (s *Coordinator) CreateSegment(ctx context.Context, segment *model.CreateSegment) error {
	if err := verifyCreateSegment(segment); err != nil {
		return err
	}
	_, err := s.catalog.CreateSegment(ctx, segment, segment.Ts)
	if err != nil {
		return err
	}
	return nil
}

func (s *Coordinator) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*model.Segment, error) {
	return s.catalog.GetSegments(ctx, segmentID, segmentType, scope, topic, collectionID)
}

func (s *Coordinator) DeleteSegment(ctx context.Context, segmentID types.UniqueID) error {
	return s.catalog.DeleteSegment(ctx, segmentID)
}

func (s *Coordinator) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment) (*model.Segment, error) {
	segment, err := s.catalog.UpdateSegment(ctx, updateSegment, updateSegment.Ts)
	if err != nil {
		return nil, err
	}
	return segment, nil
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

func verifyCreateSegment(segment *model.CreateSegment) error {
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

func (s *Coordinator) SetTenantLastCompactionTime(ctx context.Context, tenantID string, lastCompactionTime int64) error {
	return s.catalog.SetTenantLastCompactionTime(ctx, tenantID, lastCompactionTime)
}

func (s *Coordinator) GetTenantsLastCompactionTime(ctx context.Context, tenantIDs []string) ([]*dbmodel.Tenant, error) {
	return s.catalog.GetTenantsLastCompactionTime(ctx, tenantIDs)
}

func (s *Coordinator) FlushCollectionCompaction(ctx context.Context, flushCollectionCompaction *model.FlushCollectionCompaction) (*model.FlushCollectionInfo, error) {
	return s.catalog.FlushCollectionCompaction(ctx, flushCollectionCompaction)
}
