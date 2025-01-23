package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	s3metastore "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/s3"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/chroma-core/chroma/go/shared/otel"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

const (
	maxAttempts = 10
)

// The catalog backed by databases using GORM.
type Catalog struct {
	metaDomain         dbmodel.IMetaDomain
	txImpl             dbmodel.ITransaction
	s3Store            s3metastore.S3MetaStoreInterface
	versionFileEnabled bool
}

func NewTableCatalog(tx dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain, s3Store s3metastore.S3MetaStoreInterface, enableVersionFile bool) *Catalog {
	return &Catalog{
		txImpl:             tx,
		metaDomain:         metaDomain,
		s3Store:            s3Store,
		versionFileEnabled: enableVersionFile,
	}
}

func (tc *Catalog) ResetState(ctx context.Context) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		err := tc.metaDomain.CollectionMetadataDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reest collection metadata db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.CollectionDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset collection db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.SegmentMetadataDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset segment metadata db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.SegmentDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset segment db", zap.Error(err))
			return err
		}

		err = tc.metaDomain.DatabaseDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset database db", zap.Error(err))
			return err
		}

		// TODO: default database and tenant should be pre-defined object
		err = tc.metaDomain.DatabaseDb(txCtx).Insert(&dbmodel.Database{
			ID:       types.NilUniqueID().String(),
			Name:     common.DefaultDatabase,
			TenantID: common.DefaultTenant,
		})
		if err != nil {
			log.Error("error inserting default database", zap.Error(err))
			return err
		}

		err = tc.metaDomain.TenantDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset tenant db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.TenantDb(txCtx).Insert(&dbmodel.Tenant{
			ID:                 common.DefaultTenant,
			LastCompactionTime: time.Now().Unix(),
		})
		if err != nil {
			log.Error("error inserting default tenant", zap.Error(err))
			return err
		}

		return nil
	})
}

func (tc *Catalog) CreateDatabase(ctx context.Context, createDatabase *model.CreateDatabase, ts types.Timestamp) (*model.Database, error) {
	var result *model.Database

	// Check if database name is not empty
	if createDatabase.Name == "" {
		return nil, common.ErrDatabaseNameEmpty
	}

	// Check if tenant exists for the given tenant id
	tenants, err := tc.metaDomain.TenantDb(ctx).GetTenants(createDatabase.Tenant)
	if err != nil {
		log.Error("error getting tenants", zap.Error(err))
		return nil, err
	}
	if len(tenants) == 0 {
		log.Error("tenant not found", zap.Error(err))
		return nil, common.ErrTenantNotFound
	}

	err = tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		dbDatabase := &dbmodel.Database{
			ID:       createDatabase.ID,
			Name:     createDatabase.Name,
			TenantID: createDatabase.Tenant,
			Ts:       ts,
		}
		err := tc.metaDomain.DatabaseDb(txCtx).Insert(dbDatabase)
		if err != nil {
			log.Error("error inserting database", zap.Error(err))
			return err
		}
		databaseList, err := tc.metaDomain.DatabaseDb(txCtx).GetDatabases(createDatabase.Tenant, createDatabase.Name)
		if err != nil {
			log.Error("error getting database", zap.Error(err))
			return err
		}
		result = convertDatabaseToModel(databaseList[0])
		return nil
	})
	if err != nil {
		log.Error("error creating database", zap.Error(err))
		return nil, err
	}
	log.Info("database created", zap.Any("database", result))
	return result, nil
}

func (tc *Catalog) GetDatabases(ctx context.Context, getDatabase *model.GetDatabase, ts types.Timestamp) (*model.Database, error) {
	databases, err := tc.metaDomain.DatabaseDb(ctx).GetDatabases(getDatabase.Tenant, getDatabase.Name)
	if err != nil {
		return nil, err
	}
	if len(databases) == 0 {
		return nil, common.ErrDatabaseNotFound
	}
	result := make([]*model.Database, 0, len(databases))
	for _, database := range databases {
		result = append(result, convertDatabaseToModel(database))
	}
	return result[0], nil
}

func (tc *Catalog) ListDatabases(ctx context.Context, listDatabases *model.ListDatabases, ts types.Timestamp) ([]*model.Database, error) {
	databases, err := tc.metaDomain.DatabaseDb(ctx).ListDatabases(listDatabases.Limit, listDatabases.Offset, listDatabases.Tenant)
	if err != nil {
		return nil, err
	}
	result := make([]*model.Database, 0, len(databases))
	for _, database := range databases {
		result = append(result, convertDatabaseToModel(database))
	}
	return result, nil
}

func (tc *Catalog) DeleteDatabase(ctx context.Context, deleteDatabase *model.DeleteDatabase) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		databases, err := tc.metaDomain.DatabaseDb(txCtx).GetDatabases(deleteDatabase.Tenant, deleteDatabase.Name)
		if err != nil {
			return err
		}
		if len(databases) == 0 {
			return common.ErrDatabaseNotFound
		}
		err = tc.metaDomain.DatabaseDb(txCtx).Delete(databases[0].ID)
		if err != nil {
			return err
		}
		return nil
	})
}

func (tc *Catalog) GetAllDatabases(ctx context.Context, ts types.Timestamp) ([]*model.Database, error) {
	databases, err := tc.metaDomain.DatabaseDb(ctx).GetAllDatabases()
	if err != nil {
		log.Error("error getting all databases", zap.Error(err))
		return nil, err
	}
	result := make([]*model.Database, 0, len(databases))
	for _, database := range databases {
		result = append(result, convertDatabaseToModel(database))
	}
	return result, nil
}

func (tc *Catalog) CreateTenant(ctx context.Context, createTenant *model.CreateTenant, ts types.Timestamp) (*model.Tenant, error) {
	var result *model.Tenant

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// TODO: createTenant has ts, don't need to pass in
		dbTenant := &dbmodel.Tenant{
			ID:                 createTenant.Name,
			Ts:                 ts,
			LastCompactionTime: time.Now().Unix(),
		}
		err := tc.metaDomain.TenantDb(txCtx).Insert(dbTenant)
		if err != nil {
			return err
		}
		tenantList, err := tc.metaDomain.TenantDb(txCtx).GetTenants(createTenant.Name)
		if err != nil {
			return err
		}
		result = convertTenantToModel(tenantList[0])
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (tc *Catalog) GetTenants(ctx context.Context, getTenant *model.GetTenant, ts types.Timestamp) (*model.Tenant, error) {
	tenants, err := tc.metaDomain.TenantDb(ctx).GetTenants(getTenant.Name)
	if err != nil {
		log.Error("error getting tenants", zap.Error(err))
		return nil, err
	}
	if (len(tenants)) == 0 {
		log.Error("tenant not found", zap.Error(err))
		return nil, common.ErrTenantNotFound
	}
	result := make([]*model.Tenant, 0, len(tenants))
	for _, tenant := range tenants {
		result = append(result, convertTenantToModel(tenant))
	}
	return result[0], nil
}

func (tc *Catalog) GetAllTenants(ctx context.Context, ts types.Timestamp) ([]*model.Tenant, error) {
	tenants, err := tc.metaDomain.TenantDb(ctx).GetAllTenants()
	if err != nil {
		log.Error("error getting all tenants", zap.Error(err))
		return nil, err
	}
	result := make([]*model.Tenant, 0, len(tenants))
	for _, tenant := range tenants {
		result = append(result, convertTenantToModel(tenant))
	}
	return result, nil
}

func (tc *Catalog) createCollectionImpl(txCtx context.Context, createCollection *model.CreateCollection, versionFileName string, ts types.Timestamp) (*model.Collection, bool, error) {
	// insert collection
	databaseName := createCollection.DatabaseName
	tenantID := createCollection.TenantID
	databases, err := tc.metaDomain.DatabaseDb(txCtx).GetDatabases(tenantID, databaseName)
	if err != nil {
		log.Error("error getting database", zap.Error(err))
		return nil, false, err
	}
	if len(databases) == 0 {
		log.Error("database not found", zap.Error(err))
		return nil, false, common.ErrDatabaseNotFound
	}

	collectionName := createCollection.Name
	existing, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(nil, &collectionName, tenantID, databaseName, nil, nil)
	if err != nil {
		log.Error("error getting collection", zap.Error(err))
		return nil, false, err
	}
	if len(existing) != 0 {
		if createCollection.GetOrCreate {
			collection := convertCollectionToModel(existing)[0]
			return collection, false, nil
		} else {
			return nil, false, common.ErrCollectionUniqueConstraintViolation
		}
	}

	dbCollection := &dbmodel.Collection{
		ID:                   createCollection.ID.String(),
		Name:                 &createCollection.Name,
		ConfigurationJsonStr: &createCollection.ConfigurationJsonStr,
		Dimension:            createCollection.Dimension,
		DatabaseID:           databases[0].ID,
		Ts:                   ts,
		LogPosition:          0,
		VersionFileName:      versionFileName,
	}

	err = tc.metaDomain.CollectionDb(txCtx).Insert(dbCollection)
	if err != nil {
		log.Error("error inserting collection", zap.Error(err))
		return nil, false, err
	}
	// insert collection metadata
	metadata := createCollection.Metadata
	dbCollectionMetadataList := convertCollectionMetadataToDB(createCollection.ID.String(), metadata)
	if len(dbCollectionMetadataList) != 0 {
		err = tc.metaDomain.CollectionMetadataDb(txCtx).Insert(dbCollectionMetadataList)
		if err != nil {
			return nil, false, err
		}
	}
	// get collection
	collectionList, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(types.FromUniqueID(createCollection.ID), nil, tenantID, databaseName, nil, nil)
	if err != nil {
		log.Error("error getting collection", zap.Error(err))
		return nil, false, err
	}
	result := convertCollectionToModel(collectionList)[0]
	return result, true, nil

}

func (tc *Catalog) CreateCollection(ctx context.Context, createCollection *model.CreateCollection, ts types.Timestamp) (*model.Collection, bool, error) {
	var result *model.Collection
	created := false
	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		var err error
		result, created, err = tc.createCollectionImpl(txCtx, createCollection, "", ts)
		return err
	})
	if err != nil {
		log.Error("error creating collection", zap.Error(err))
		return nil, false, err
	}
	log.Info("collection created", zap.Any("collection", result))
	return result, created, nil
}

// Returns true if collection is deleted (either soft-deleted or hard-deleted)
// and false otherwise.
func (tc *Catalog) CheckCollection(ctx context.Context, collectionID types.UniqueID) (bool, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.CheckCollection")
		defer span.End()
	}

	collectionInfo, err := tc.metaDomain.CollectionDb(ctx).GetCollectionEntry(types.FromUniqueID(collectionID), nil)
	if err != nil {
		return false, err
	}
	// Collection is hard deleted.
	if collectionInfo == nil {
		return true, nil
	}
	// Collection is soft deleted.
	if collectionInfo.IsDeleted {
		return true, nil
	}
	// Collection is not deleted.
	return false, nil
}

func (tc *Catalog) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, tenantID string, databaseName string, limit *int32, offset *int32) ([]*model.Collection, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetCollections")
		defer span.End()
	}

	collectionAndMetadataList, err := tc.metaDomain.CollectionDb(ctx).GetCollections(types.FromUniqueID(collectionID), collectionName, tenantID, databaseName, limit, offset)
	if err != nil {
		return nil, err
	}
	collections := convertCollectionToModel(collectionAndMetadataList)
	return collections, nil
}

func (tc *Catalog) GetCollectionSize(ctx context.Context, collectionID types.UniqueID) (uint64, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetCollectionSize")
		defer span.End()
	}

	total_records_post_compaction, err := tc.metaDomain.CollectionDb(ctx).GetCollectionSize(collectionID.String())
	if err != nil {
		return 0, err
	}
	return total_records_post_compaction, nil
}

func (tc *Catalog) ListCollectionsToGc(ctx context.Context) ([]*model.CollectionToGc, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.ListCollectionsToGc")
		defer span.End()
	}

	collectionsToGc, err := tc.metaDomain.CollectionDb(ctx).ListCollectionsToGc()

	if err != nil {
		return nil, err
	}
	collections := convertCollectionToGcToModel(collectionsToGc)
	return collections, nil
}

func (tc *Catalog) GetCollectionWithSegments(ctx context.Context, collectionID types.UniqueID) (*model.Collection, []*model.Segment, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetCollections")
		defer span.End()
	}

	var collection *model.Collection
	var segments []*model.Segment

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		collections, e := tc.GetCollections(ctx, collectionID, nil, "", "", nil, nil)
		if e != nil {
			return e
		}
		if len(collections) == 0 {
			return common.ErrCollectionNotFound
		}
		if len(collections) > 1 {
			return common.ErrCollectionUniqueConstraintViolation
		}
		collection = collections[0]

		segments, e = tc.GetSegments(ctx, types.NilUniqueID(), nil, nil, collectionID)
		if e != nil {
			return e
		}

		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return collection, segments, nil
}

func (tc *Catalog) DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection, softDelete bool) error {
	if softDelete {
		return tc.softDeleteCollection(ctx, deleteCollection)
	}
	return tc.hardDeleteCollection(ctx, deleteCollection)
}

func (tc *Catalog) hardDeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	log.Info("hard deleting collection", zap.Any("deleteCollection", deleteCollection), zap.String("databaseName", deleteCollection.DatabaseName))
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		collectionID := deleteCollection.ID

		collectionEntry, err := tc.metaDomain.CollectionDb(txCtx).GetCollectionEntry(types.FromUniqueID(collectionID), &deleteCollection.DatabaseName)
		if err != nil {
			return err
		}
		if collectionEntry == nil {
			log.Info("collection not found during hard delete", zap.Any("deleteCollection", deleteCollection))
			return common.ErrCollectionDeleteNonExistingCollection
		}

		// Delete collection and collection metadata.
		collectionDeletedCount, err := tc.metaDomain.CollectionDb(txCtx).DeleteCollectionByID(collectionID.String())
		if err != nil {
			log.Error("error deleting collection during hard delete", zap.Error(err))
			return err
		}
		if collectionDeletedCount == 0 {
			log.Info("collection not found during hard delete", zap.Any("deleteCollection", deleteCollection))
			return common.ErrCollectionDeleteNonExistingCollection
		}
		// Delete collection metadata.
		collectionMetadataDeletedCount, err := tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(collectionID.String())
		if err != nil {
			log.Error("error deleting collection metadata during hard delete", zap.Error(err))
			return err
		}
		// Delete segments.
		segments, err := tc.metaDomain.SegmentDb(txCtx).GetSegmentsByCollectionID(collectionID.String())
		if err != nil {
			log.Error("error getting segments during hard delete", zap.Error(err))
			return err
		}
		for _, segment := range segments {
			err = tc.metaDomain.SegmentDb(txCtx).DeleteSegmentByID(segment.ID)
			if err != nil {
				log.Error("error deleting segment during hard delete", zap.Error(err))
				return err
			}
			err = tc.metaDomain.SegmentMetadataDb(txCtx).DeleteBySegmentID(segment.ID)
			if err != nil {
				log.Error("error deleting segment metadata during hard delete", zap.Error(err))
				return err
			}
		}

		log.Info("collection hard deleted", zap.Any("collection", collectionID),
			zap.Int("collectionDeletedCount", collectionDeletedCount),
			zap.Int("collectionMetadataDeletedCount", collectionMetadataDeletedCount))
		return nil
	})
}

func (tc *Catalog) softDeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	log.Info("Soft deleting collection", zap.Any("softDeleteCollection", deleteCollection))
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Check if collection exists
		collections, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(types.FromUniqueID(deleteCollection.ID), nil, deleteCollection.TenantID, deleteCollection.DatabaseName, nil, nil)
		if err != nil {
			return err
		}
		if len(collections) == 0 {
			return common.ErrCollectionDeleteNonExistingCollection
		}

		// Generate new name with timestamp and random number
		oldName := *collections[0].Collection.Name
		newName := fmt.Sprintf("_deleted_%s_%d_%d", oldName, time.Now().Unix(), rand.Intn(1000))

		dbCollection := &dbmodel.Collection{
			ID:        deleteCollection.ID.String(),
			Name:      &newName,
			IsDeleted: true,
			Ts:        deleteCollection.Ts,
			UpdatedAt: time.Now(),
		}
		err = tc.metaDomain.CollectionDb(txCtx).Update(dbCollection)
		if err != nil {
			log.Error("soft delete collection failed", zap.Error(err))
			return fmt.Errorf("collection delete failed due to update error: %w", err)
		}
		return nil
	})
}

func (tc *Catalog) GetSoftDeletedCollections(ctx context.Context, collectionID *string, tenantID string, databaseName string, limit int32) ([]*model.Collection, error) {
	collections, err := tc.metaDomain.CollectionDb(ctx).GetSoftDeletedCollections(collectionID, tenantID, databaseName, limit)
	if err != nil {
		return nil, err
	}
	// Convert to model.Collection
	collectionList := make([]*model.Collection, 0, len(collections))
	for _, dbCollection := range collections {
		collection := &model.Collection{
			ID:           types.MustParse(dbCollection.Collection.ID),
			Name:         *dbCollection.Collection.Name,
			DatabaseName: dbCollection.DatabaseName,
			TenantID:     dbCollection.TenantID,
			Ts:           types.Timestamp(dbCollection.Collection.Ts),
			UpdatedAt:    types.Timestamp(dbCollection.Collection.UpdatedAt.Unix()),
		}
		collectionList = append(collectionList, collection)
	}
	return collectionList, nil
}

func (tc *Catalog) UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection, ts types.Timestamp) (*model.Collection, error) {
	log.Info("updating collection", zap.String("collectionId", updateCollection.ID.String()))
	var result *model.Collection

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Check if collection exists
		collections, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(
			types.FromUniqueID(updateCollection.ID),
			nil,
			updateCollection.TenantID,
			updateCollection.DatabaseName,
			nil,
			nil,
		)
		if err != nil {
			return err
		}
		if len(collections) == 0 {
			return common.ErrCollectionNotFound
		}

		dbCollection := &dbmodel.Collection{
			ID:        updateCollection.ID.String(),
			Name:      updateCollection.Name,
			Dimension: updateCollection.Dimension,
			Ts:        ts,
		}
		err = tc.metaDomain.CollectionDb(txCtx).Update(dbCollection)
		if err != nil {
			return err
		}

		// Case 1: if ResetMetadata is true, then delete all metadata for the collection
		// Case 2: if ResetMetadata is true and metadata is not nil -> THIS SHOULD NEVER HAPPEN
		// Case 3: if ResetMetadata is false, and the metadata is not nil - set the metadata to the value in metadata
		// Case 4: if ResetMetadata is false and metadata is nil, then leave the metadata as is
		metadata := updateCollection.Metadata
		resetMetadata := updateCollection.ResetMetadata
		if resetMetadata {
			if metadata != nil { // Case 2
				return common.ErrInvalidMetadataUpdate
			} else { // Case 1
				_, err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(updateCollection.ID.String())
				if err != nil {
					return err
				}
			}
		} else {
			if metadata != nil { // Case 3
				_, err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(updateCollection.ID.String())
				if err != nil {
					return err
				}
				dbCollectionMetadataList := convertCollectionMetadataToDB(updateCollection.ID.String(), metadata)
				if len(dbCollectionMetadataList) != 0 {
					err = tc.metaDomain.CollectionMetadataDb(txCtx).Insert(dbCollectionMetadataList)
					if err != nil {
						return err
					}
				}
			}
		}
		databaseName := updateCollection.DatabaseName
		tenantID := updateCollection.TenantID
		collectionList, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(types.FromUniqueID(updateCollection.ID), nil, tenantID, databaseName, nil, nil)
		if err != nil {
			return err
		}
		if collectionList == nil || len(collectionList) == 0 {
			return common.ErrCollectionNotFound
		}
		result = convertCollectionToModel(collectionList)[0]
		return nil
	})
	if err != nil {
		return nil, err
	}
	log.Info("collection updated", zap.String("collectionID", result.ID.String()))
	return result, nil
}

func (tc *Catalog) CreateSegment(ctx context.Context, createSegment *model.CreateSegment, ts types.Timestamp) (*model.Segment, error) {
	var result *model.Segment

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		var err error
		result, err = tc.createSegmentImpl(txCtx, createSegment, ts)
		return err
	})
	if err != nil {
		log.Error("error creating segment", zap.Error(err))
		return nil, err
	}
	log.Info("segment created", zap.Any("segment", result))
	return result, nil
}

func (tc *Catalog) createSegmentImpl(txCtx context.Context, createSegment *model.CreateSegment, ts types.Timestamp) (*model.Segment, error) {
	var result *model.Segment

	// insert segment
	collectionString := createSegment.CollectionID.String()
	dbSegment := &dbmodel.Segment{
		ID:           createSegment.ID.String(),
		CollectionID: &collectionString,
		Type:         createSegment.Type,
		Scope:        createSegment.Scope,
		Ts:           ts,
	}
	err := tc.metaDomain.SegmentDb(txCtx).Insert(dbSegment)
	if err != nil {
		log.Error("error inserting segment", zap.Error(err))
		return nil, err
	}
	// insert segment metadata
	metadata := createSegment.Metadata
	if metadata != nil {
		dbSegmentMetadataList := convertSegmentMetadataToDB(createSegment.ID.String(), metadata)
		if len(dbSegmentMetadataList) != 0 {
			err = tc.metaDomain.SegmentMetadataDb(txCtx).Insert(dbSegmentMetadataList)
			if err != nil {
				log.Error("error inserting segment metadata", zap.Error(err))
				return nil, err
			}
		}
	}
	// get segment
	segmentList, err := tc.metaDomain.SegmentDb(txCtx).GetSegments(createSegment.ID, nil, nil, createSegment.CollectionID)
	if err != nil {
		log.Error("error getting segment", zap.Error(err))
		return nil, err
	}
	result = convertSegmentToModel(segmentList)[0]

	return result, nil
}

func (tc *Catalog) createFirstVersionFile(ctx context.Context, createCollection *model.CreateCollection, createSegments []*model.CreateSegment, ts types.Timestamp) (string, error) {
	collectionVersionFilePb := &coordinatorpb.CollectionVersionFile{
		CollectionInfoImmutable: &coordinatorpb.CollectionInfoImmutable{
			TenantId:               createCollection.TenantID,
			DatabaseId:             createCollection.DatabaseName,
			CollectionId:           createCollection.ID.String(),
			CollectionName:         createCollection.Name,
			CollectionCreationSecs: int64(ts),
		},
		VersionHistory: &coordinatorpb.CollectionVersionHistory{
			Versions: []*coordinatorpb.CollectionVersionInfo{
				{
					Version:       0,
					CreatedAtSecs: int64(ts),
				},
			},
		},
	}
	// Construct the version file name.
	versionFileName := "0"
	err := tc.s3Store.PutVersionFile(createCollection.TenantID, createCollection.ID.String(), versionFileName, collectionVersionFilePb)
	if err != nil {
		return "", err
	}
	return versionFileName, nil
}

func (tc *Catalog) CreateCollectionAndSegments(ctx context.Context, createCollection *model.CreateCollection, createSegments []*model.CreateSegment, ts types.Timestamp) (*model.Collection, bool, error) {
	var resultCollection *model.Collection
	created := false

	// Create the first Version file in S3.
	// If the transaction below fails, then there will be an orphan file in S3.
	// This orphan file will not affect new collection creations.
	// An alternative approach is to create this file after the transaction is committed.
	// and let FlushCollectionCompaction do any repair work if first version file is missing.
	versionFileName := ""
	var err error
	if tc.versionFileEnabled {
		versionFileName, err = tc.createFirstVersionFile(ctx, createCollection, createSegments, ts)
		if err != nil {
			return nil, false, err
		}
	}

	err = tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Create the collection using the refactored helper
		var err error
		resultCollection, created, err = tc.createCollectionImpl(txCtx, createCollection, versionFileName, ts)
		if err != nil {
			log.Error("error creating collection", zap.Error(err))
			return err
		}

		// If collection already exists, then do not create segments.
		// TODO: Should we check to see if segments does not exist? and create them?
		if !created {
			return nil
		}

		// Create the associated segments.
		for _, createSegment := range createSegments {
			createSegment.CollectionID = resultCollection.ID // Ensure the segment is linked to the newly created collection

			_, err := tc.createSegmentImpl(txCtx, createSegment, ts)
			if err != nil {
				log.Error("error creating segment", zap.Error(err))
				return err
			}
		}

		return nil
	})

	if err != nil {
		log.Error("error creating collection and segments", zap.Error(err))
		return nil, false, err
	}

	log.Info("collection and segments created", zap.Any("collection", resultCollection))
	return resultCollection, created, nil
}

func (tc *Catalog) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, collectionID types.UniqueID) ([]*model.Segment, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetSegments")
		defer span.End()
	}

	segmentAndMetadataList, err := tc.metaDomain.SegmentDb(ctx).GetSegments(segmentID, segmentType, scope, collectionID)
	if err != nil {
		return nil, err
	}
	segments := make([]*model.Segment, 0, len(segmentAndMetadataList))
	for _, segmentAndMetadata := range segmentAndMetadataList {
		segment := &model.Segment{
			ID:        types.MustParse(segmentAndMetadata.Segment.ID),
			Type:      segmentAndMetadata.Segment.Type,
			Scope:     segmentAndMetadata.Segment.Scope,
			Ts:        segmentAndMetadata.Segment.Ts,
			FilePaths: segmentAndMetadata.Segment.FilePaths,
		}

		if segmentAndMetadata.Segment.CollectionID != nil {
			segment.CollectionID = types.MustParse(*segmentAndMetadata.Segment.CollectionID)
		} else {
			segment.CollectionID = types.NilUniqueID()
		}
		segment.Metadata = convertSegmentMetadataToModel(segmentAndMetadata.SegmentMetadata)
		segments = append(segments, segment)
	}
	return segments, nil
}

// DeleteSegment is a no-op.
// Segments are deleted as part of atomic delete of collection.
// Keeping this API so that older clients continue to work.
func (tc *Catalog) DeleteSegment(ctx context.Context, segmentID types.UniqueID, collectionID types.UniqueID) error {
	return nil
}

func (tc *Catalog) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment, ts types.Timestamp) (*model.Segment, error) {
	if updateSegment.Collection == nil {
		return nil, common.ErrMissingCollectionID
	}

	parsedCollectionID, err := types.Parse(*updateSegment.Collection)
	if err != nil {
		return nil, err
	}

	var result *model.Segment

	err = tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		{
			results, err := tc.metaDomain.SegmentDb(txCtx).GetSegments(updateSegment.ID, nil, nil, parsedCollectionID)
			if err != nil {
				return err
			}
			if len(results) == 0 {
				return common.ErrSegmentUpdateNonExistingSegment
			}
			updateSegment.Collection = results[0].Segment.CollectionID
		}

		// update segment
		dbSegment := &dbmodel.UpdateSegment{
			ID:         updateSegment.ID.String(),
			Collection: updateSegment.Collection,
		}

		err := tc.metaDomain.SegmentDb(txCtx).Update(dbSegment)
		if err != nil {
			return err
		}

		// Case 1: if ResetMetadata is true, then delete all metadata for the collection
		// Case 2: if ResetMetadata is true and metadata is not nil -> THIS SHOULD NEVER HAPPEN
		// Case 3: if ResetMetadata is false, and the metadata is not nil - set the metadata to the value in metadata
		// Case 4: if ResetMetadata is false and metadata is nil, then leave the metadata as is
		metadata := updateSegment.Metadata
		resetMetadata := updateSegment.ResetMetadata
		if resetMetadata {
			if metadata != nil { // Case 2
				return common.ErrInvalidMetadataUpdate
			} else { // Case 1
				err := tc.metaDomain.SegmentMetadataDb(txCtx).DeleteBySegmentID(updateSegment.ID.String())
				if err != nil {
					return err
				}
			}
		} else {
			if metadata != nil { // Case 3
				err := tc.metaDomain.SegmentMetadataDb(txCtx).DeleteBySegmentIDAndKeys(updateSegment.ID.String(), metadata.Keys())
				if err != nil {
					log.Error("error deleting segment metadata", zap.Error(err))
					return err
				}
				newMetadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
				for _, key := range metadata.Keys() {
					if metadata.Get(key) == nil {
						metadata.Remove(key)
					} else {
						newMetadata.Set(key, metadata.Get(key))
					}
				}
				dbSegmentMetadataList := convertSegmentMetadataToDB(updateSegment.ID.String(), newMetadata)
				if len(dbSegmentMetadataList) != 0 {
					err = tc.metaDomain.SegmentMetadataDb(txCtx).Insert(dbSegmentMetadataList)
					if err != nil {
						return err
					}
				}
			}
		}

		// get segment
		segmentList, err := tc.metaDomain.SegmentDb(txCtx).GetSegments(updateSegment.ID, nil, nil, parsedCollectionID)
		if err != nil {
			log.Error("error getting segment", zap.Error(err))
			return err
		}
		result = convertSegmentToModel(segmentList)[0]
		return nil
	})
	if err != nil {
		log.Error("error updating segment", zap.Error(err))
		return nil, err
	}
	log.Debug("segment updated", zap.Any("segment", result))
	return result, nil
}

func (tc *Catalog) SetTenantLastCompactionTime(ctx context.Context, tenantID string, lastCompactionTime int64) error {
	return tc.metaDomain.TenantDb(ctx).UpdateTenantLastCompactionTime(tenantID, lastCompactionTime)
}

func (tc *Catalog) GetTenantsLastCompactionTime(ctx context.Context, tenantIDs []string) ([]*dbmodel.Tenant, error) {
	tenants, err := tc.metaDomain.TenantDb(ctx).GetTenantsLastCompactionTime(tenantIDs)
	return tenants, err
}

// ListCollectionVersions lists all versions of a collection that have not been marked for deletion.
func (tc *Catalog) ListCollectionVersions(ctx context.Context,
	collectionID types.UniqueID,
	tenantID string,
	maxCount *int64,
	versionsBefore int64,
	versionsAtOrAfter int64,
) ([]*coordinatorpb.CollectionVersionInfo, error) {
	return nil, nil
}

func (tc *Catalog) updateVersionFileInS3(ctx context.Context, existingVersionFilePb *coordinatorpb.CollectionVersionFile, flushCollectionCompaction *model.FlushCollectionCompaction, ts_secs int64) (string, error) {
	segmentCompactionInfos := make([]*coordinatorpb.FlushSegmentCompactionInfo, 0, len(flushCollectionCompaction.FlushSegmentCompactions))
	for _, compaction := range flushCollectionCompaction.FlushSegmentCompactions {
		// Convert map[string][]string to map[string]*coordinatorpb.FilePaths
		convertedPaths := make(map[string]*coordinatorpb.FilePaths)
		for k, v := range compaction.FilePaths {
			convertedPaths[k] = &coordinatorpb.FilePaths{Paths: v}
		}

		info := &coordinatorpb.FlushSegmentCompactionInfo{
			SegmentId: compaction.ID.String(),
			FilePaths: convertedPaths,
		}
		segmentCompactionInfos = append(segmentCompactionInfos, info)
	}

	existingVersionFilePb.GetVersionHistory().Versions = append(existingVersionFilePb.GetVersionHistory().Versions, &coordinatorpb.CollectionVersionInfo{
		Version:       int64(flushCollectionCompaction.CurrentCollectionVersion) + 1,
		CreatedAtSecs: ts_secs,
		SegmentInfo: &coordinatorpb.CollectionSegmentInfo{
			SegmentCompactionInfo: segmentCompactionInfos,
		},
		CollectionInfoMutable: &coordinatorpb.CollectionInfoMutable{
			CurrentLogPosition:       int64(flushCollectionCompaction.LogPosition),
			CurrentCollectionVersion: int64(flushCollectionCompaction.CurrentCollectionVersion),
			UpdatedAtSecs:            ts_secs,
		},
		VersionChangeReason: coordinatorpb.CollectionVersionInfo_VERSION_CHANGE_REASON_DATA_COMPACTION,
	})

	// Write the new version file to S3.
	// Format of version file name: <version>_<uuid>_flush
	// The version should be left padded with 0s upto 6 digits.
	newVersionFileName := fmt.Sprintf("%06d_%s_flush", flushCollectionCompaction.CurrentCollectionVersion+1, uuid.New().String())
	err := tc.s3Store.PutVersionFile(flushCollectionCompaction.TenantID, flushCollectionCompaction.ID.String(), newVersionFileName, existingVersionFilePb)
	if err != nil {
		return "", err
	}

	return newVersionFileName, nil
}

func (tc *Catalog) FlushCollectionCompaction(ctx context.Context, flushCollectionCompaction *model.FlushCollectionCompaction) (*model.FlushCollectionInfo, error) {
	if tc.versionFileEnabled {
		return tc.FlushCollectionCompactionForVersionedCollection(ctx, flushCollectionCompaction)
	}

	flushCollectionInfo := &model.FlushCollectionInfo{
		ID: flushCollectionCompaction.ID.String(),
	}

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Check if collection exists.
		collection, err := tc.metaDomain.CollectionDb(txCtx).GetCollectionEntry(types.FromUniqueID(flushCollectionCompaction.ID), nil)
		if err != nil {
			return err
		}
		if collection == nil {
			return common.ErrCollectionNotFound
		}
		if collection.IsDeleted {
			return common.ErrCollectionSoftDeleted
		}

		// register files to Segment metadata
		err = tc.metaDomain.SegmentDb(txCtx).RegisterFilePaths(flushCollectionCompaction.FlushSegmentCompactions)
		if err != nil {
			return err
		}

		// update collection log position and version
		collectionVersion, err := tc.metaDomain.CollectionDb(txCtx).UpdateLogPositionVersionAndTotalRecords(flushCollectionCompaction.ID.String(), flushCollectionCompaction.LogPosition, flushCollectionCompaction.CurrentCollectionVersion, flushCollectionCompaction.TotalRecordsPostCompaction)
		if err != nil {
			return err
		}
		flushCollectionInfo.CollectionVersion = collectionVersion

		// update tenant last compaction time
		// TODO: add a system configuration to disable
		// since this might cause resource contention if one tenant has a lot of collection compactions at the same time
		lastCompactionTime := time.Now().Unix()
		err = tc.metaDomain.TenantDb(txCtx).UpdateTenantLastCompactionTime(flushCollectionCompaction.TenantID, lastCompactionTime)
		if err != nil {
			return err
		}
		flushCollectionInfo.TenantLastCompactionTime = lastCompactionTime

		// return nil will commit the transaction
		return nil
	})
	if err != nil {
		return nil, err
	}
	return flushCollectionInfo, nil
}

func (tc *Catalog) validateVersionFile(versionFile *coordinatorpb.CollectionVersionFile, collectionID string, version int64) error {
	if versionFile.GetCollectionInfoImmutable().GetCollectionId() != collectionID {
		log.Error("collection id mismatch", zap.String("collection_id", collectionID), zap.String("version_file_collection_id", versionFile.GetCollectionInfoImmutable().GetCollectionId()))
		return errors.New("collection id mismatch")
	}
	if versionFile.GetVersionHistory().GetVersions()[0].GetVersion() != version {
		log.Error("version mismatch", zap.Int64("version", version), zap.Int64("version_file_version", versionFile.GetVersionHistory().GetVersions()[0].GetVersion()))
		return errors.New("version mismatch")
	}
	return nil
}

// Pre-Context for understanding this method:
//  1. Information about collection version history is maintained in the VersionFile in S3.
//  2. The VersionFileName is maintained in the Postgres table.
//  3. When updating CollectionEntry, a CAS operation against both version and version file name is performed.
//  4. Since Segment information is maintained in a separate table, a Transaction
//     is used to atomically update the CollectionEntry and Segment data.
//
// Algorithm:
// 1. Get the collection entry from the table.
// 2. Prepare the new version file.
// 3. Write the version file to S3.
// 4. Till the CAS operation succeeds, retry the operation (i.e. goto 1)
// 5. 		If version CAS fails - then fail the operation to the Compactor.
// 6. 		If version file name CAS fails - read updated file and write a new version file to S3.
func (tc *Catalog) FlushCollectionCompactionForVersionedCollection(ctx context.Context, flushCollectionCompaction *model.FlushCollectionCompaction) (*model.FlushCollectionInfo, error) {
	// The result that is sent back to the Compactor.
	flushCollectionInfo := &model.FlushCollectionInfo{
		ID: flushCollectionCompaction.ID.String(),
	}

	// Do the operation in a loop until the CollectionEntry is updated,
	// 		OR FAIL the operation if the version is stale
	//      OR other DB error.
	//
	// In common case, the loop will run only once.
	// The loop with run more than once only when GC competes to update the
	// VersionFileName. More precisely, when GC updates the VersionFile in S3
	// to mark certain versions and then tries to update the VersionFileName in
	// the table at the same time.
	numAttempts := 0
	for numAttempts < maxAttempts {
		numAttempts++
		// Get the current version info and the version file from the table.
		collectionEntry, err := tc.metaDomain.CollectionDb(ctx).GetCollectionEntry(types.FromUniqueID(flushCollectionCompaction.ID), nil)
		if err != nil {
			return nil, err
		}
		if collectionEntry == nil {
			return nil, common.ErrCollectionNotFound
		}
		if collectionEntry.IsDeleted {
			return nil, common.ErrCollectionSoftDeleted
		}

		versionAtCompactionStart := int64(flushCollectionCompaction.CurrentCollectionVersion)
		existingVersion := int64(collectionEntry.Version)

		// Do a check to see if the version is stale.
		if existingVersion > versionAtCompactionStart {
			// Compactor is trying to flush a version that is no longer valid, since
			// a different compaction instance has already incremented the version.
			log.Info("Compactor is trying to flush a stale version", zap.Int64("existing_version", existingVersion), zap.Int64("current_collection_version", versionAtCompactionStart))
			return nil, common.ErrCollectionVersionStale
		}

		if existingVersion < versionAtCompactionStart {
			// This condition should not happen. Or may be its possible due to Restore which is currently not implemented.
			// Logging error and returning.
			log.Error("Compactor is trying to flush a version that is less than the current version", zap.Int64("existing_version", existingVersion), zap.Int64("current_collection_version", versionAtCompactionStart))
			return nil, common.ErrCollectionVersionInvalid
		}

		existingVersionFileName := collectionEntry.VersionFileName
		// Read the VersionFile from S3MetaStore.
		existingVersionFilePb, err := tc.s3Store.GetVersionFile(flushCollectionCompaction.TenantID, flushCollectionCompaction.ID.String(), existingVersion, existingVersionFileName)
		if err != nil {
			return nil, err
		}

		// Do a simple validation of the version file.
		err = tc.validateVersionFile(existingVersionFilePb, collectionEntry.ID, existingVersion)
		if err != nil {
			log.Error("version file validation failed", zap.Error(err))
			return nil, err
		}

		// The update function takes the content of the existing version file,
		// and the set of segments that are part of the new version file.
		// NEW VersionFile is created in S3 at this step.
		newVersionFileName, err := tc.updateVersionFileInS3(ctx, existingVersionFilePb, flushCollectionCompaction, time.Now().Unix())
		if err != nil {
			return nil, err
		}

		txErr := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
			// NOTE: DO NOT move UpdateTenantLastCompactionTime & RegisterFilePaths to the end of the transaction.
			//		 Keep both these operations before the UpdateLogPositionAndVersionInfo.
			//       UpdateLogPositionAndVersionInfo acts as a CAS operation whose failure will roll back the transaction.
			//       If order is changed, we can still potentially loose an update to Collection entry by
			//       a concurrent transaction that updates Collection entry immediately after UpdateLogPositionAndVersionInfo completes.
			// The other approach is to use a "SELECT FOR UPDATE" to lock the Collection entry at the start of the transaction,
			// which is costlier than the current approach that does not lock the Collection entry.

			// register files to Segment metadata
			err = tc.metaDomain.SegmentDb(txCtx).RegisterFilePaths(flushCollectionCompaction.FlushSegmentCompactions)
			if err != nil {
				return err
			}
			// update tenant last compaction time
			// TODO: add a system configuration to disable
			// since this might cause resource contention if one tenant has a lot of collection compactions at the same time
			lastCompactionTime := time.Now().Unix()
			err = tc.metaDomain.TenantDb(txCtx).UpdateTenantLastCompactionTime(flushCollectionCompaction.TenantID, lastCompactionTime)
			if err != nil {
				return err
			}

			// At this point, a concurrent Transaction can still update/commit
			// the Collection entry.
			// Since this Tx is ReadCommitted, the result of other Tx will be
			// visible to the statement below. Hence the statement below will
			// use WHERE clause to ensure that its update will not go through
			// if the Collection entry is updated by another Tx.

			// Update collection log position and version
			rowsAffected, err := tc.metaDomain.CollectionDb(txCtx).UpdateLogPositionAndVersionInfo(
				flushCollectionCompaction.ID.String(),
				flushCollectionCompaction.LogPosition,
				flushCollectionCompaction.CurrentCollectionVersion,
				existingVersionFileName,
				flushCollectionCompaction.CurrentCollectionVersion+1,
				newVersionFileName,
			)
			if err != nil {
				return err
			}
			if rowsAffected == 0 {
				// CAS operation failed.
				// Error out the transaction, so that segment is not updated.
				return common.ErrCollectionEntryIsStale
			}

			// CAS operation succeeded. Update tenant compaction time and then
			// COMMIT the transaction.

			// Set the result values that will be returned to the Compactor.
			flushCollectionInfo.TenantLastCompactionTime = lastCompactionTime
			flushCollectionInfo.CollectionVersion = flushCollectionCompaction.CurrentCollectionVersion + 1

			// return nil will commit the transaction
			return nil
		}) // End of transaction

		if txErr == nil {
			// CAS operation succeeded.
			// Return the result to the Compactor.
			return flushCollectionInfo, nil
		}

		// There are only two possible reasons for this error:
		// 1. The entry was stale because either another Compactor or GarbageCollector updated the entry
		//    between the start of this operation, and before the Update was run.
		//    => Retry the operation.
		// 2. Some other DB error
		//    => Return error to Compactor.
		switch txErr {
		case common.ErrCollectionEntryIsStale:
			// CAS operation failed. i.e. no rows were updated.
			// Retry the CAS operation.
			// TODO: Convert this to log.Debug in future.
			// TODO: The version file that was just created can be deleted. Delete it.
			log.Info("version file name stale, retrying",
				zap.Int("attempt", numAttempts),
				zap.Int("max_attempts", maxAttempts),
				zap.String("existing_version_file_name", existingVersionFileName),
				zap.String("committed_version_file_name", newVersionFileName))
			continue

		default:
			// Return the error to Compactor.
			return nil, txErr
		}
	} // End of loop

	log.Error("Max attempts reached for version file update. Retry from compactor.",
		zap.Int("max_attempts", maxAttempts),
		zap.String("collection_id", flushCollectionCompaction.ID.String()),
		zap.Int64("log_position", flushCollectionCompaction.LogPosition),
		zap.Int32("current_collection_version", flushCollectionCompaction.CurrentCollectionVersion))
	return nil, fmt.Errorf("max attempts (%d) reached for version file update", maxAttempts)
}
