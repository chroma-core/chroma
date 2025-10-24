package coordinator

import (
	"context"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/memberlist_manager"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	s3metastore "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/s3"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Coordinator is the top level component.
// Currently, it only has the system catalog related APIs and will be extended to
// support other functionalities such as membership managed and propagation.
type Coordinator struct {
	ctx         context.Context
	catalog     Catalog
	objectStore *s3metastore.S3MetaStore
	heapClient  HeapClient // Optional, can be nil if heap service is disabled
}

// CoordinatorConfig holds configuration for creating a Coordinator
type CoordinatorConfig struct {
	// Object store for metadata persistence
	ObjectStore *s3metastore.S3MetaStore

	// Enable version file functionality
	VersionFileEnabled bool

	// Heap service configuration (optional)
	HeapServiceEnabled          bool
	HeapServicePort             int
	HeapServiceAssignmentHasher string // Assignment policy hasher: "murmur3" (default)
	KubernetesNamespace         string
	LogServiceMemberlistName    string
}

func NewCoordinator(ctx context.Context, config CoordinatorConfig) (*Coordinator, error) {
	s := &Coordinator{
		ctx:         ctx,
		objectStore: config.ObjectStore,
	}

	// catalog
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	s.catalog = *NewTableCatalog(txnImpl, metaDomain, s.objectStore, config.VersionFileEnabled)

	// Initialize heap client if enabled
	if config.HeapServiceEnabled {
		memberlistStore, err := memberlist_manager.NewCRMemberlistStoreFromK8s(
			config.KubernetesNamespace,
			config.LogServiceMemberlistName,
		)
		if err != nil {
			log.Error("Failed to create memberlist store", zap.Error(err))
			return nil, err
		}

		hasher, err := GetHasherFromString(config.HeapServiceAssignmentHasher)
		if err != nil {
			log.Error("Failed to get hasher from config", zap.Error(err))
			return nil, err
		}

		s.heapClient = NewGrpcHeapClient(memberlistStore, config.HeapServicePort, hasher)
		log.Info("Heap service client initialized",
			zap.String("memberlist", config.LogServiceMemberlistName),
			zap.Int("port", config.HeapServicePort),
			zap.String("hasher", config.HeapServiceAssignmentHasher))
	}

	return s, nil
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

func (s *Coordinator) ListDatabases(ctx context.Context, listDatabases *model.ListDatabases) ([]*model.Database, error) {
	databases, err := s.catalog.ListDatabases(ctx, listDatabases, listDatabases.Ts)
	if err != nil {
		return nil, err
	}
	return databases, nil
}

func (s *Coordinator) DeleteDatabase(ctx context.Context, deleteDatabase *model.DeleteDatabase) error {
	return s.catalog.DeleteDatabase(ctx, deleteDatabase)
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

func (s *Coordinator) CreateCollectionAndSegments(ctx context.Context, createCollection *model.CreateCollection, createSegments []*model.Segment) (*model.Collection, bool, error) {
	collection, created, err := s.catalog.CreateCollectionAndSegments(ctx, createCollection, createSegments, createCollection.Ts)
	if err != nil {
		return nil, false, err
	}
	return collection, created, nil
}

func (s *Coordinator) CreateCollection(ctx context.Context, createCollection *model.CreateCollection) (*model.Collection, bool, error) {
	log.Info("create collection", zap.Any("createCollection", createCollection))
	collection, created, err := s.catalog.CreateCollection(ctx, createCollection, createCollection.Ts)
	if err != nil {
		return nil, false, err
	}
	return collection, created, nil
}

func (s *Coordinator) GetCollection(ctx context.Context, collectionID types.UniqueID, collectionName *string, tenantID string, databaseName string) (*model.Collection, error) {
	return s.catalog.GetCollection(ctx, collectionID, collectionName, tenantID, databaseName)
}

func (s *Coordinator) GetCollections(ctx context.Context, collectionIDs []types.UniqueID, collectionName *string, tenantID string, databaseName string, limit *int32, offset *int32, includeSoftDeleted bool) ([]*model.Collection, error) {
	return s.catalog.GetCollections(ctx, collectionIDs, collectionName, tenantID, databaseName, limit, offset, includeSoftDeleted)
}

func (s *Coordinator) GetCollectionByResourceName(ctx context.Context, tenantResourceName string, databaseName string, collectionName string) (*model.Collection, error) {
	return s.catalog.GetCollectionByResourceName(ctx, tenantResourceName, databaseName, collectionName)
}

func (s *Coordinator) CountCollections(ctx context.Context, tenantID string, databaseName *string) (uint64, error) {
	return s.catalog.CountCollections(ctx, tenantID, databaseName)
}

func (s *Coordinator) GetCollectionSize(ctx context.Context, collectionID types.UniqueID) (uint64, error) {
	return s.catalog.GetCollectionSize(ctx, collectionID)
}

func (s *Coordinator) GetCollectionWithSegments(ctx context.Context, collectionID types.UniqueID) (*model.Collection, []*model.Segment, error) {
	return s.catalog.GetCollectionWithSegments(ctx, collectionID, false)
}

func (s *Coordinator) CheckCollection(ctx context.Context, collectionID types.UniqueID) (bool, int64, error) {
	return s.catalog.CheckCollection(ctx, collectionID)
}

func (s *Coordinator) GetSoftDeletedCollections(ctx context.Context, collectionID *string, tenantID string, databaseName string, limit int32) ([]*model.Collection, error) {
	return s.catalog.GetSoftDeletedCollections(ctx, collectionID, tenantID, databaseName, limit)
}

func (s *Coordinator) SoftDeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	return s.catalog.DeleteCollection(ctx, deleteCollection, true)
}

func (s *Coordinator) FinishCollectionDeletion(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	return s.catalog.DeleteCollection(ctx, deleteCollection, false)
}

func (s *Coordinator) UpdateCollection(ctx context.Context, collection *model.UpdateCollection) (*model.Collection, error) {
	return s.catalog.UpdateCollection(ctx, collection, collection.Ts)
}

func (s *Coordinator) ForkCollection(ctx context.Context, forkCollection *model.ForkCollection) (*model.Collection, []*model.Segment, error) {
	return s.catalog.ForkCollection(ctx, forkCollection)
}

func (s *Coordinator) CountForks(ctx context.Context, sourceCollectionID types.UniqueID) (uint64, error) {
	return s.catalog.CountForks(ctx, sourceCollectionID)
}

func (s *Coordinator) CreateSegment(ctx context.Context, segment *model.Segment) error {
	if err := verifyCreateSegment(segment); err != nil {
		return err
	}
	_, err := s.catalog.CreateSegment(ctx, segment, segment.Ts)
	if err != nil {
		return err
	}
	return nil
}

func (s *Coordinator) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, collectionID types.UniqueID) ([]*model.Segment, error) {
	return s.catalog.GetSegments(ctx, segmentID, segmentType, scope, collectionID)
}

// DeleteSegment is a no-op.
// Segments are deleted as part of atomic delete of collection.
// Keeping this API so that older clients continue to work, since older clients will issue DeleteSegment
// after a DeleteCollection.
func (s *Coordinator) DeleteSegment(ctx context.Context, segmentID types.UniqueID, collectionID types.UniqueID) error {
	return s.catalog.DeleteSegment(ctx, segmentID, collectionID)
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

func verifyCreateSegment(segment *model.Segment) error {
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

// Note: as of now, this is the only field settable on a tenant, so we have a narrowly scoped operation.
// If we enable more fields to be settable on a tenant, we should consider adding a more general UpdateTenant API.
func (s *Coordinator) SetTenantResourceName(ctx context.Context, tenantID string, resourceName string) error {
	return s.catalog.SetTenantResourceName(ctx, tenantID, resourceName)
}

func (s *Coordinator) FlushCollectionCompaction(ctx context.Context, flushCollectionCompaction *model.FlushCollectionCompaction) (*model.FlushCollectionInfo, error) {
	return s.catalog.FlushCollectionCompaction(ctx, flushCollectionCompaction)
}

func (s *Coordinator) FlushCollectionCompactionAndAttachedFunction(
	ctx context.Context,
	flushCollectionCompaction *model.FlushCollectionCompaction,
	attachedFunctionID uuid.UUID,
	runNonce uuid.UUID,
	completionOffset int64,
) (*model.FlushCollectionInfo, error) {
	return s.catalog.FlushCollectionCompactionAndAttachedFunction(ctx, flushCollectionCompaction, attachedFunctionID, runNonce, completionOffset)
}

func (s *Coordinator) ListCollectionsToGc(ctx context.Context, cutoffTimeSecs *uint64, limit *uint64, tenantID *string, minVersionsIfAlive *uint64) ([]*model.CollectionToGc, error) {
	return s.catalog.ListCollectionsToGc(ctx, cutoffTimeSecs, limit, tenantID, minVersionsIfAlive)
}

func (s *Coordinator) ListCollectionVersions(ctx context.Context, collectionID types.UniqueID, tenantID string, maxCount *int64, versionsBefore *int64, versionsAtOrAfter *int64, includeMarkedForDeletion bool) ([]*coordinatorpb.CollectionVersionInfo, error) {
	return s.catalog.ListCollectionVersions(ctx, collectionID, tenantID, maxCount, versionsBefore, versionsAtOrAfter, includeMarkedForDeletion)
}

func (s *Coordinator) MarkVersionForDeletion(ctx context.Context, req *coordinatorpb.MarkVersionForDeletionRequest) (*coordinatorpb.MarkVersionForDeletionResponse, error) {
	return s.catalog.MarkVersionForDeletion(ctx, req)
}

func (s *Coordinator) DeleteCollectionVersion(ctx context.Context, req *coordinatorpb.DeleteCollectionVersionRequest) (*coordinatorpb.DeleteCollectionVersionResponse, error) {
	return s.catalog.DeleteCollectionVersion(ctx, req)
}

func (s *Coordinator) BatchGetCollectionVersionFilePaths(ctx context.Context, req *coordinatorpb.BatchGetCollectionVersionFilePathsRequest) (*coordinatorpb.BatchGetCollectionVersionFilePathsResponse, error) {
	return s.catalog.BatchGetCollectionVersionFilePaths(ctx, req.CollectionIds)
}

func (s *Coordinator) BatchGetCollectionSoftDeleteStatus(ctx context.Context, req *coordinatorpb.BatchGetCollectionSoftDeleteStatusRequest) (*coordinatorpb.BatchGetCollectionSoftDeleteStatusResponse, error) {
	return s.catalog.BatchGetCollectionSoftDeleteStatus(ctx, req.CollectionIds)
}

func (s *Coordinator) FinishDatabaseDeletion(ctx context.Context, req *coordinatorpb.FinishDatabaseDeletionRequest) (*coordinatorpb.FinishDatabaseDeletionResponse, error) {
	numDeleted, err := s.catalog.FinishDatabaseDeletion(ctx, time.Unix(req.CutoffTime.Seconds, int64(req.CutoffTime.Nanos)))
	if err != nil {
		return nil, err
	}

	res := &coordinatorpb.FinishDatabaseDeletionResponse{
		NumDeleted: numDeleted,
	}
	return res, nil
}
