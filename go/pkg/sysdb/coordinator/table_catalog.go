package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	s3metastore "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/s3"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/chroma-core/chroma/go/shared/otel"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const (
	maxAttempts                         = 10
	maxAttemptsToMarkVersionForDeletion = 5
	maxAttemptsToDeleteVersionEntries   = 5
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
		err = tc.metaDomain.DatabaseDb(txCtx).SoftDelete(databases[0].ID)
		if err != nil {
			return err
		}

		collections, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(nil, nil, deleteDatabase.Tenant, deleteDatabase.Name, nil, nil, false)
		if err != nil {
			return err
		}

		for _, collection := range collections {
			collectionID, err := types.Parse(collection.Collection.ID)
			if err != nil {
				return err
			}

			err = tc.softDeleteCollection(txCtx, &model.DeleteCollection{
				ID:           collectionID,
				TenantID:     deleteDatabase.Tenant,
				DatabaseName: deleteDatabase.Name,
			})
			if err != nil {
				return err
			}
		}

		return nil
	})
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
		log.Error("database not found for database", zap.String("database_name", databaseName), zap.String("tenant_id", tenantID))
		return nil, false, common.ErrDatabaseNotFound
	}

	collectionName := createCollection.Name
	existing, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(nil, &collectionName, tenantID, databaseName, nil, nil, false)
	if err != nil {
		log.Error("error getting collection", zap.Error(err))
		return nil, false, err
	}
	if len(existing) != 0 {
		if createCollection.GetOrCreate {
			// In the happy path for get or create, the collection exists and under
			// read commited isolation, we know its not deleted at the time we
			// we started the transaction. So we return it
			collection := convertCollectionToModel(existing)[0]
			return collection, false, nil
		} else {
			return nil, false, common.ErrCollectionUniqueConstraintViolation
		}
	}

	dbCollection := &dbmodel.Collection{
		ID:                         createCollection.ID.String(),
		Name:                       &createCollection.Name,
		ConfigurationJsonStr:       &createCollection.ConfigurationJsonStr,
		SchemaStr:                  createCollection.SchemaStr,
		Dimension:                  createCollection.Dimension,
		DatabaseID:                 databases[0].ID,
		VersionFileName:            versionFileName,
		Tenant:                     createCollection.TenantID,
		Ts:                         ts,
		LogPosition:                createCollection.LogPosition,
		RootCollectionId:           createCollection.RootCollectionId,
		TotalRecordsPostCompaction: createCollection.TotalRecordsPostCompaction,
		SizeBytesPostCompaction:    createCollection.SizeBytesPostCompaction,
		LastCompactionTimeSecs:     createCollection.LastCompactionTimeSecs,
	}

	created := false
	// In get or create mode, ignore conflicts
	// NOTE(hammadb) 5/16/2025 - We could skip the above get() and always InsertOnConflictDoNothing
	// however when adding this change to handle a race condition I am biasing towards keeping the exisitng
	// performance of the happy get() path without profiling whether InsertOnConflictDoNothing is equivalent
	// If this proves to be a performance issue, we can change this to always use InsertOnConflictDoNothing
	// and remove the get() above
	if createCollection.GetOrCreate {
		created, err = tc.metaDomain.CollectionDb(txCtx).InsertOnConflictDoNothing(dbCollection)
	} else {
		/*
			Note a potential race here for three writers
			Thread 1 calls create_collection and inserts a row
			Thread 2 calls delete and hard delete kicks in, which deletes the row
			Thread 3 calls create_collection and creates a row
			Thread 1 now proceeds to insert metadata and inserts the metadata for the collection
			Thread 3 will now proceed to insert the metadata for the collection

			So we have info and metadata from two different writers

			We can avoid this with the timing assumption that hard delete runs far
			behind the timeout of these operations
		*/
		err = tc.metaDomain.CollectionDb(txCtx).Insert(dbCollection)
		if err == nil {
			created = true
		}
	}

	if err != nil {
		log.Error("error inserting collection", zap.Error(err))
		return nil, false, err
	}

	// Under read-commited isolation with get_or_create, its possible someone else created the collection, in which case
	// we don't want to insert the metadata again, and create an inconsistent state
	if created {
		// insert collection metadata
		metadata := createCollection.Metadata
		dbCollectionMetadataList := convertCollectionMetadataToDB(createCollection.ID.String(), metadata)
		if len(dbCollectionMetadataList) != 0 {
			err = tc.metaDomain.CollectionMetadataDb(txCtx).Insert(dbCollectionMetadataList)
			if err != nil {
				return nil, false, err
			}
		}
	}

	// Get the inserted collection (by name, to handle the case where some other request created the collection)
	collectionList, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(nil, &collectionName, tenantID, databaseName, nil, nil, false)
	// It is possible, under read-commited isolation that someone else deleted the collection
	// in between writing the collection and reading it back, in that case this will return empty, and we should throw an error
	if err != nil {
		log.Error("error getting collection", zap.Error(err))
		return nil, false, err
	}

	if len(collectionList) == 0 {
		// This can happen if the collection was deleted in between the insert and the get
		// we inform downstream of this contention, and let the client decide what to do based on their application logic
		log.Error("collection not found after insert, implying a concurrent delete")
		return nil, false, common.ErrConcurrentDeleteCollection
	}
	result := convertCollectionToModel(collectionList)[0]
	return result, created, nil
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
func (tc *Catalog) CheckCollection(ctx context.Context, collectionID types.UniqueID) (bool, int64, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.CheckCollection")
		defer span.End()
	}

	collectionInfo, err := tc.metaDomain.CollectionDb(ctx).GetCollectionWithoutMetadata(types.FromUniqueID(collectionID), nil, nil)
	if err != nil {
		return false, 0, err
	}
	// Collection is hard deleted.
	if collectionInfo == nil {
		return true, 0, nil
	}

	return collectionInfo.IsDeleted, collectionInfo.LogPosition, nil
}

func (tc *Catalog) GetCollection(ctx context.Context, collectionID types.UniqueID, collectionName *string, tenantID string, databaseName string) (*model.Collection, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetCollection")
		defer span.End()
	}

	// Get collection and metadata.
	// NOTE: Choosing to use GetCollectionEntries instead of GetCollections so that we can check if the collection is soft deleted.
	// Also, choosing to use a function that returns a list to avoid creating a new function.
	collectionAndMetadataList, err := tc.metaDomain.CollectionDb(ctx).GetCollectionEntries(types.FromUniqueID(collectionID), collectionName, tenantID, databaseName, nil, nil)
	if err != nil {
		return nil, err
	}
	// Collection not found.
	if len(collectionAndMetadataList) == 0 {
		return nil, common.ErrCollectionNotFound
	}
	// Check if the entry is soft deleted.
	collectionWithMetdata := collectionAndMetadataList[0].Collection
	if collectionWithMetdata.IsDeleted {
		return nil, common.ErrCollectionSoftDeleted
	}
	// Convert to model.
	collection := convertCollectionToModel(collectionAndMetadataList)
	// CollectionID is primary key, so there should be only one collection.
	return collection[0], nil
}

func (tc *Catalog) GetCollections(ctx context.Context, collectionIDs []types.UniqueID, collectionName *string, tenantID string, databaseName string, limit *int32, offset *int32, includeSoftDeleted bool) ([]*model.Collection, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetCollections")
		defer span.End()
	}

	ids := ([]string)(nil)
	if collectionIDs != nil {
		ids = make([]string, 0, len(collectionIDs))
		for _, id := range collectionIDs {
			ids = append(ids, id.String())
		}
	}

	collectionAndMetadataList, err := tc.metaDomain.CollectionDb(ctx).GetCollections(ids, collectionName, tenantID, databaseName, limit, offset, includeSoftDeleted)
	if err != nil {
		return nil, err
	}
	collections := convertCollectionToModel(collectionAndMetadataList)
	return collections, nil
}

func (tc *Catalog) GetCollectionByResourceName(ctx context.Context, tenantResourceName string, databaseName string, collectionName string) (*model.Collection, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetCollectionByResourceName")
		defer span.End()
	}

	collectionAndMetadata, err := tc.metaDomain.CollectionDb(ctx).GetCollectionByResourceName(tenantResourceName, databaseName, collectionName)
	if err != nil {
		return nil, err
	}
	if collectionAndMetadata == nil {
		return nil, common.ErrCollectionNotFound
	}
	return convertCollectionToModel([]*dbmodel.CollectionAndMetadata{collectionAndMetadata})[0], nil
}

func (tc *Catalog) CountCollections(ctx context.Context, tenantID string, databaseName *string) (uint64, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.CountCollections")
		defer span.End()
	}

	collection_count, err := tc.metaDomain.CollectionDb(ctx).CountCollections(tenantID, databaseName)
	if err != nil {
		return 0, err
	}
	return collection_count, nil
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

func (tc *Catalog) ListCollectionsToGc(ctx context.Context, cutoffTimeSecs *uint64, limit *uint64, tenantID *string, minVersionsIfAlive *uint64) ([]*model.CollectionToGc, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.ListCollectionsToGc")
		defer span.End()
	}

	collectionsToGc, err := tc.metaDomain.CollectionDb(ctx).ListCollectionsToGc(cutoffTimeSecs, limit, tenantID, minVersionsIfAlive)

	if err != nil {
		return nil, err
	}
	collections := convertCollectionToGcToModel(collectionsToGc)
	return collections, nil
}

func (tc *Catalog) GetCollectionWithSegments(ctx context.Context, collectionID types.UniqueID, returnSoftDeleted bool) (*model.Collection, []*model.Segment, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetCollections")
		defer span.End()
	}

	var collection *model.Collection
	var segments []*model.Segment

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		collection_entry, e := tc.GetCollection(txCtx, collectionID, nil, "", "")
		if e != nil {
			return e
		}
		if collection_entry == nil {
			return common.ErrCollectionNotFound
		}
		if collection_entry.IsDeleted && !returnSoftDeleted {
			return common.ErrCollectionNotFound
		}

		segments, e = tc.GetSegments(txCtx, types.NilUniqueID(), nil, nil, collectionID)
		if e != nil {
			return e
		}

		collection = collection_entry
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

		_, err := tc.metaDomain.CollectionDb(txCtx).LockCollection(collectionID.String())
		if err != nil {
			log.Error("error locking collection for hard delete", zap.Error(err), zap.Any("deleteCollection", deleteCollection))
			return err
		}

		collectionEntry, err := tc.metaDomain.CollectionDb(txCtx).GetCollectionWithoutMetadata(types.FromUniqueID(collectionID), &deleteCollection.DatabaseName, nil)
		if err != nil {
			return err
		}
		if collectionEntry == nil {
			log.Info("collection not found during hard delete", zap.Any("deleteCollection", deleteCollection))
			return common.ErrCollectionDeleteNonExistingCollection
		}

		if !collectionEntry.IsDeleted {
			return common.ErrCollectionWasNotSoftDeleted
		}

		if collectionEntry.RootCollectionId != nil {
			// We need to lock the root collection for the current transaction since we later modify it by changing the path to the lineage file
			// NOTE: the locking order (first a collection, then its root collection) must be EXACTLY THE SAME as the locking order used for forking to avoid deadlocks.
			_, err = tc.metaDomain.CollectionDb(txCtx).LockCollection(*collectionEntry.RootCollectionId)
			if err != nil {
				return err
			}

			// This was a forked collection, so we need to update the lineage file
			rootCollection, err := tc.metaDomain.CollectionDb(txCtx).GetCollectionWithoutMetadata(collectionEntry.RootCollectionId, nil, nil)
			if err != nil {
				return err
			}
			if rootCollection == nil {
				// This should not happen since LockCollection above will error if the collection does not exist
				return errors.New("root collection not found")
			}

			if rootCollection.LineageFileName == nil {
				return common.ErrMissingLineageFileName
			}

			lineageFile, err := tc.getLineageFile(txCtx, rootCollection.LineageFileName)
			if err != nil {
				return err
			}
			// Remove collection being deleted from the dependencies
			updatedDependencies := make([]*coordinatorpb.CollectionVersionDependency, 0)
			for _, dependency := range lineageFile.Dependencies {
				if dependency.SourceCollectionId == deleteCollection.ID.String() {
					return errors.New("cannot delete a collection that is still listed as a source of another collection")
				}

				if dependency.TargetCollectionId != deleteCollection.ID.String() {
					updatedDependencies = append(updatedDependencies, dependency)
				}
			}
			lineageFile.Dependencies = updatedDependencies

			newLineageFileId, err := uuid.NewV7()
			if err != nil {
				return err
			}

			newLineageFileFullName, err := tc.s3Store.PutLineageFile(ctx, collectionEntry.Tenant, collectionEntry.DatabaseID, rootCollection.ID, fmt.Sprintf("%s.binpb", newLineageFileId.String()), lineageFile)
			if err != nil {
				return err
			}

			tc.metaDomain.CollectionDb(txCtx).UpdateCollectionLineageFilePath(rootCollection.ID, rootCollection.LineageFileName, newLineageFileFullName)
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
		collections, err := tc.metaDomain.CollectionDb(txCtx).GetCollections([]string{deleteCollection.ID.String()}, nil, deleteCollection.TenantID, deleteCollection.DatabaseName, nil, nil, false)
		if err != nil {
			return err
		}
		if len(collections) == 0 {
			return common.ErrCollectionDeleteNonExistingCollection
		}

		// Generate new name with timestamp and random number
		oldName := *collections[0].Collection.Name
		newName := fmt.Sprintf("_deleted_%s_%s", oldName, deleteCollection.ID.String())

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
			ID:              types.MustParse(dbCollection.Collection.ID),
			Name:            *dbCollection.Collection.Name,
			DatabaseName:    dbCollection.DatabaseName,
			TenantID:        dbCollection.TenantID,
			Ts:              types.Timestamp(dbCollection.Collection.Ts),
			UpdatedAt:       types.Timestamp(dbCollection.Collection.UpdatedAt.Unix()),
			LineageFileName: dbCollection.Collection.LineageFileName,
		}
		collectionList = append(collectionList, collection)
	}
	return collectionList, nil
}

// updateCollectionConfigurationAndSchema handles parsing and updating collection configuration and schema
func (tc *Catalog) updateCollectionConfigurationAndSchema(
	existingConfigJsonStr *string,
	existingSchemaStr *string,
	updateConfigJsonStr *string,
	collectionMetadata []*dbmodel.CollectionMetadata,
) (*string, *string, error) {
	if updateConfigJsonStr == nil || *updateConfigJsonStr == "{}" || *updateConfigJsonStr == "" {
		return nil, nil, nil
	}

	// Parse update configuration
	var updateConfig model.InternalUpdateCollectionConfiguration
	if err := json.Unmarshal([]byte(*updateConfigJsonStr), &updateConfig); err != nil {
		return nil, nil, fmt.Errorf("failed to parse update configuration: %w", err)
	}

	// Check if schema exists and merge config into it
	if existingSchemaStr != nil && *existingSchemaStr != "" && *existingSchemaStr != "{}" {
		// Schema is the source of truth - merge the updated config into schema
		newSchemaStr, err := model.UpdateSchemaFromConfig(updateConfig, *existingSchemaStr)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to merge config into schema: %w", err)
		}
		return nil, &newSchemaStr, nil
	}

	// Parse existing configuration
	var existingConfig model.InternalCollectionConfiguration
	var parseErr error
	if existingConfigJsonStr != nil {
		parseErr = json.Unmarshal([]byte(*existingConfigJsonStr), &existingConfig)
		if parseErr != nil {
			// Try to create config from legacy metadata
			metadataMap := make(map[string]interface{})
			for _, m := range collectionMetadata {
				if m.StrValue != nil {
					metadataMap[*m.Key] = *m.StrValue
				} else if m.IntValue != nil {
					metadataMap[*m.Key] = float64(*m.IntValue)
				} else if m.FloatValue != nil {
					metadataMap[*m.Key] = *m.FloatValue
				} else if m.BoolValue != nil {
					metadataMap[*m.Key] = *m.BoolValue
				}
			}
			existingConfig = *model.FromLegacyMetadata(metadataMap)
		} else if existingConfig.VectorIndex == nil {
			// If the config was parsed but has no vector index, use default HNSW
			existingConfig = *model.DefaultHnswCollectionConfiguration()
		}
	} else {
		// If no existing config, try to create from legacy metadata first
		metadataMap := make(map[string]interface{})
		for _, m := range collectionMetadata {
			if m.StrValue != nil {
				metadataMap[*m.Key] = *m.StrValue
			} else if m.IntValue != nil {
				metadataMap[*m.Key] = float64(*m.IntValue)
			} else if m.FloatValue != nil {
				metadataMap[*m.Key] = *m.FloatValue
			} else if m.BoolValue != nil {
				metadataMap[*m.Key] = *m.BoolValue
			}
		}
		if len(metadataMap) > 0 {
			existingConfig = *model.FromLegacyMetadata(metadataMap)
		} else {
			// If no legacy metadata, use default HNSW
			existingConfig = *model.DefaultHnswCollectionConfiguration()
		}
	}

	// Update existing configuration with new values
	if updateConfig.VectorIndex != nil {
		if updateConfig.VectorIndex.Hnsw != nil {
			if existingConfig.VectorIndex == nil || existingConfig.VectorIndex.Hnsw == nil {
				return existingConfigJsonStr, nil, nil
			}
			if updateConfig.VectorIndex.Hnsw.EfSearch != nil {
				existingConfig.VectorIndex.Hnsw.EfSearch = *updateConfig.VectorIndex.Hnsw.EfSearch
			}
			if updateConfig.VectorIndex.Hnsw.MaxNeighbors != nil {
				existingConfig.VectorIndex.Hnsw.MaxNeighbors = *updateConfig.VectorIndex.Hnsw.MaxNeighbors
			}
			if updateConfig.VectorIndex.Hnsw.NumThreads != nil {
				existingConfig.VectorIndex.Hnsw.NumThreads = *updateConfig.VectorIndex.Hnsw.NumThreads
			}
			if updateConfig.VectorIndex.Hnsw.ResizeFactor != nil {
				existingConfig.VectorIndex.Hnsw.ResizeFactor = *updateConfig.VectorIndex.Hnsw.ResizeFactor
			}
			if updateConfig.VectorIndex.Hnsw.SyncThreshold != nil {
				existingConfig.VectorIndex.Hnsw.SyncThreshold = *updateConfig.VectorIndex.Hnsw.SyncThreshold
			}
			if updateConfig.VectorIndex.Hnsw.BatchSize != nil {
				existingConfig.VectorIndex.Hnsw.BatchSize = *updateConfig.VectorIndex.Hnsw.BatchSize
			}
		} else if updateConfig.VectorIndex.Spann != nil {
			if existingConfig.VectorIndex == nil || existingConfig.VectorIndex.Spann == nil {
				return existingConfigJsonStr, nil, nil
			}
			if updateConfig.VectorIndex.Spann.EfSearch != nil {
				existingConfig.VectorIndex.Spann.EfSearch = *updateConfig.VectorIndex.Spann.EfSearch
			}
			if updateConfig.VectorIndex.Spann.SearchNprobe != nil {
				existingConfig.VectorIndex.Spann.SearchNprobe = *updateConfig.VectorIndex.Spann.SearchNprobe
			}
		}
	}

	if updateConfig.EmbeddingFunction != nil {
		existingConfig.EmbeddingFunction = updateConfig.EmbeddingFunction
	}

	// Serialize updated config back to JSON
	updatedConfigBytes, err := json.Marshal(existingConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to serialize updated configuration: %w", err)
	}
	updatedConfigStr := string(updatedConfigBytes)
	return &updatedConfigStr, nil, nil
}

func (tc *Catalog) UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection, ts types.Timestamp) (*model.Collection, error) {
	log.Info("updating collection", zap.String("collectionId", updateCollection.ID.String()))
	var result *model.Collection

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Check if collection exists
		collections, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(
			[]string{updateCollection.ID.String()},
			nil,
			updateCollection.TenantID,
			updateCollection.DatabaseName,
			nil,
			nil,
			false,
		)
		if err != nil {
			return err
		}
		if len(collections) == 0 {
			return common.ErrCollectionNotFound
		}
		collection := collections[0]

		// Update configuration and/or schema
		newConfigJsonStr, newSchemaStr, err := tc.updateCollectionConfigurationAndSchema(
			collection.Collection.ConfigurationJsonStr,
			collection.Collection.SchemaStr,
			updateCollection.NewConfigurationJsonStr,
			collection.CollectionMetadata,
		)
		if err != nil {
			return err
		}

		dbCollection := &dbmodel.Collection{
			ID:                   updateCollection.ID.String(),
			Name:                 updateCollection.Name,
			Dimension:            updateCollection.Dimension,
			ConfigurationJsonStr: newConfigJsonStr,
			SchemaStr:            newSchemaStr,
			Ts:                   ts,
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
		collectionList, err := tc.metaDomain.CollectionDb(txCtx).GetCollections([]string{updateCollection.ID.String()}, nil, tenantID, databaseName, nil, nil, false)
		if err != nil {
			return err
		}
		if len(collectionList) == 0 {
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

func (tc *Catalog) getLineageFile(ctx context.Context, lineageFileName *string) (*coordinatorpb.CollectionLineageFile, error) {
	if lineageFileName == nil {
		// There is no lineage file for the given collection
		return &coordinatorpb.CollectionLineageFile{
			Dependencies: []*coordinatorpb.CollectionVersionDependency{},
		}, nil
	}

	// Safe to deref.
	return tc.s3Store.GetLineageFile(ctx, *lineageFileName)
}

func (tc *Catalog) ForkCollection(ctx context.Context, forkCollection *model.ForkCollection) (*model.Collection, []*model.Segment, error) {
	log.Info("Forking collection", zap.String("sourceCollectionId", forkCollection.SourceCollectionID.String()), zap.String("targetCollectionName", forkCollection.TargetCollectionName))

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		var err error
		var rootCollectionID types.UniqueID
		var rootCollectionIDStr string
		var sourceCollection *model.Collection
		var sourceSegments []*model.Segment
		var newLineageFileFullName string
		var oldLineageFileName *string
		var lineageFileTenantId string

		ts := time.Now().UTC()

		sourceCollectionIDStr := forkCollection.SourceCollectionID.String()

		// NOTE: We need to retrieve the source collection to get root collection id, then acquire locks on source and root collections in order to avoid deadlock.
		// This step is safe because root collection id is always populated when the collection is created and is never modified.
		// If source collection is deleted then cannot fork.
		isDeleted := false
		sourceCollectionDb, err := tc.metaDomain.CollectionDb(txCtx).GetCollectionWithoutMetadata(&sourceCollectionIDStr, nil, &isDeleted)
		if err != nil {
			return err
		}
		if sourceCollectionDb == nil {
			return common.ErrCollectionNotFound
		}

		if sourceCollectionDb.RootCollectionId != nil {
			rootCollectionID, err = types.Parse(*sourceCollectionDb.RootCollectionId)
			if err != nil {
				return err
			}
		} else {
			rootCollectionID = forkCollection.SourceCollectionID
		}
		rootCollectionIDStr = rootCollectionID.String()

		// NOTE: the locking order (first a collection, then its root collection) must be EXACTLY THE SAME as the locking order used for hard deleting a collection to avoid deadlocks.
		collectionsToLock := []string{sourceCollectionIDStr}
		if rootCollectionID != forkCollection.SourceCollectionID {
			collectionsToLock = append(collectionsToLock, rootCollectionIDStr)
		}
		for _, collectionID := range collectionsToLock {
			isDeleted, e := tc.metaDomain.CollectionDb(txCtx).LockCollection(collectionID)
			if e != nil {
				return e
			}
			// It's ok for the root collection to be deleted but not for the source collection.
			// We disable hard delete for soft deleted collections that are root.
			if collectionID == sourceCollectionIDStr && *isDeleted {
				return common.ErrCollectionNotFound
			}
		}

		// Get source and root collections after they are locked
		// They can't get deleted (both soft as well as hard) concurrently since we have the locks
		sourceCollection, sourceSegments, err = tc.GetCollectionWithSegments(txCtx, forkCollection.SourceCollectionID, false)
		if err != nil {
			return err
		}
		// this can't happen and implies something weird in the system since
		// we have the locks on the source collection
		if sourceCollection == nil || sourceSegments == nil {
			return common.ErrCollectionDeletedWithLocksHeld
		}
		if rootCollectionID != forkCollection.SourceCollectionID {
			// Get root collection. It is ok to get soft deleted root collection entry here.
			collection, err := tc.metaDomain.CollectionDb(txCtx).GetCollectionWithoutMetadata(&rootCollectionIDStr, nil, nil)
			if err != nil {
				return err
			}
			// Implies a hard deleted and should not happen
			if collection == nil {
				return common.ErrCollectionDeletedWithLocksHeld
			}
			// Root should always have a lineage file
			if collection.LineageFileName == nil {
				return common.ErrMissingLineageFileName
			}
			lineageFileTenantId = collection.Tenant
			oldLineageFileName = collection.LineageFileName
		} else {
			lineageFileTenantId = sourceCollection.TenantID
			oldLineageFileName = sourceCollection.LineageFileName
		}
		databases, err := tc.metaDomain.DatabaseDb(txCtx).GetDatabases(sourceCollection.TenantID, sourceCollection.DatabaseName)
		if err != nil {
			return err
		}
		if len(databases) == 0 {
			return common.ErrDatabaseNotFound
		}

		databaseID := databases[0].ID

		// Verify that the source collection log position is between the compaction offset (inclusive) and enumeration offset (inclusive)
		// This check is necessary for next compaction to fetch the right logs
		// This scenario could occur during fork because we will reach out to log service first to fork logs. For exampls:
		// t0: Fork source collection in log with offset [200, 300] (i.e. compaction offset 200, enumeration offset 300)
		// t1: User writes to source collection, compaction takes place, source collection log offset become [400, 500]
		// t2: Fork source collection in sysdb, the latest source collection compaction offset is 400. If we add new logs, it will start after offset 300, and the data is lost after compaction.
		latestSourceCompactionOffset := uint64(sourceCollection.LogPosition)
		if forkCollection.SourceCollectionLogEnumerationOffset < latestSourceCompactionOffset {
			log.Error("CollectionLogPositionStale", zap.Uint64("latestSourceCompactionOffset", latestSourceCompactionOffset), zap.Uint64("forkCollection.SourceCollectionLogEnumerationOffset ", forkCollection.SourceCollectionLogEnumerationOffset))
			return common.ErrCollectionLogPositionStale
		}
		if latestSourceCompactionOffset < forkCollection.SourceCollectionLogCompactionOffset {
			log.Error("CompactionOffsetSomehowAhead", zap.Uint64("latestSourceCompactionOffset", latestSourceCompactionOffset), zap.Uint64("forkCollection.SourceCollectionLogCompactionOffset", forkCollection.SourceCollectionLogCompactionOffset))
			return common.ErrCompactionOffsetSomehowAhead
		}

		// Create the new collection with source collection information
		createCollection := &model.CreateCollection{
			ID:                         forkCollection.TargetCollectionID,
			Name:                       forkCollection.TargetCollectionName,
			ConfigurationJsonStr:       sourceCollection.ConfigurationJsonStr,
			SchemaStr:                  sourceCollection.SchemaStr,
			Dimension:                  sourceCollection.Dimension,
			Metadata:                   sourceCollection.Metadata,
			GetOrCreate:                false,
			TenantID:                   sourceCollection.TenantID,
			DatabaseName:               sourceCollection.DatabaseName,
			Ts:                         ts.Unix(),
			LogPosition:                sourceCollection.LogPosition,
			RootCollectionId:           &rootCollectionIDStr,
			TotalRecordsPostCompaction: sourceCollection.TotalRecordsPostCompaction,
			SizeBytesPostCompaction:    sourceCollection.SizeBytesPostCompaction,
			LastCompactionTimeSecs:     sourceCollection.LastCompactionTimeSecs,
		}

		createSegments := []*model.Segment{}
		flushFilePaths := []*model.FlushSegmentCompaction{}
		for _, segment := range sourceSegments {
			newSegmentID := types.NewUniqueID()
			createSegment := &model.Segment{
				ID:           newSegmentID,
				Type:         segment.Type,
				Scope:        segment.Scope,
				CollectionID: forkCollection.TargetCollectionID,
				Metadata:     segment.Metadata,
				Ts:           ts.Unix(),
				FilePaths:    segment.FilePaths,
			}
			createSegments = append(createSegments, createSegment)
			flushFilePath := &model.FlushSegmentCompaction{
				ID:        newSegmentID,
				FilePaths: segment.FilePaths,
			}
			flushFilePaths = append(flushFilePaths, flushFilePath)
		}

		_, _, err = tc.CreateCollectionAndSegments(txCtx, createCollection, createSegments, ts.Unix())
		if err != nil {
			return err
		}

		err = tc.metaDomain.SegmentDb(txCtx).RegisterFilePaths(flushFilePaths)
		if err != nil {
			return err
		}

		// Update the lineage file
		lineageFile, err := tc.getLineageFile(txCtx, oldLineageFileName)
		if err != nil {
			return err
		}
		// Defensive backstop to prevent too many forks
		if len(lineageFile.Dependencies) > 1000000 {
			return common.ErrCollectionTooManyFork
		}
		lineageFile.Dependencies = append(lineageFile.Dependencies, &coordinatorpb.CollectionVersionDependency{
			SourceCollectionId:      sourceCollectionIDStr,
			SourceCollectionVersion: uint64(sourceCollection.Version),
			TargetCollectionId:      forkCollection.TargetCollectionID.String(),
		})

		newLineageFileId, err := uuid.NewV7()
		if err != nil {
			return err
		}

		newLineageFileBaseName := fmt.Sprintf("%s.binpb", newLineageFileId.String())
		newLineageFileFullName, err = tc.s3Store.PutLineageFile(txCtx, lineageFileTenantId, databaseID, rootCollectionIDStr, newLineageFileBaseName, lineageFile)
		if err != nil {
			return err
		}

		return tc.metaDomain.CollectionDb(txCtx).UpdateCollectionLineageFilePath(rootCollectionIDStr, oldLineageFileName, newLineageFileFullName)
	})
	if err != nil {
		return nil, nil, err
	}

	return tc.GetCollectionWithSegments(ctx, forkCollection.TargetCollectionID, false)
}

func (tc *Catalog) CountForks(ctx context.Context, sourceCollectionID types.UniqueID) (uint64, error) {
	var rootCollectionID types.UniqueID

	sourceCollectionIDStr := sourceCollectionID.String()
	isDeleted := false
	sourceCollectionDb, err := tc.metaDomain.CollectionDb(ctx).GetCollectionWithoutMetadata(&sourceCollectionIDStr, nil, &isDeleted)
	if err != nil {
		return 0, err
	}
	if sourceCollectionDb == nil {
		return 0, common.ErrCollectionNotFound
	}

	if sourceCollectionDb.RootCollectionId != nil {
		rootCollectionID, err = types.Parse(*sourceCollectionDb.RootCollectionId)
		if err != nil {
			return 0, err
		}
	} else {
		rootCollectionID = sourceCollectionID
	}

	limit := int32(1)
	collections, err := tc.GetCollections(ctx, []types.UniqueID{rootCollectionID}, nil, "", "", &limit, nil, false)
	if err != nil {
		return 0, err
	}
	if len(collections) == 0 {
		return 0, common.ErrCollectionNotFound
	}
	rootCollection := collections[0]

	lineageFile, err := tc.getLineageFile(ctx, rootCollection.LineageFileName)
	if err != nil {
		return 0, err
	}

	if lineageFile == nil || lineageFile.Dependencies == nil {
		return 0, nil
	}
	return uint64(len(lineageFile.Dependencies)), nil
}

func (tc *Catalog) CreateSegment(ctx context.Context, createSegment *model.Segment, ts types.Timestamp) (*model.Segment, error) {
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

func (tc *Catalog) createSegmentImpl(txCtx context.Context, createSegment *model.Segment, ts types.Timestamp) (*model.Segment, error) {
	var result *model.Segment

	// insert segment
	collectionString := createSegment.CollectionID.String()
	dbSegment := &dbmodel.Segment{
		ID:           createSegment.ID.String(),
		CollectionID: &collectionString,
		Type:         createSegment.Type,
		Scope:        createSegment.Scope,
		Ts:           ts,
		FilePaths:    createSegment.FilePaths,
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

func (tc *Catalog) createFirstVersionFile(ctx context.Context, databaseID string, createCollection *model.CreateCollection, createSegments []*model.Segment, ts types.Timestamp) (string, error) {
	segmentCompactionInfos := make([]*coordinatorpb.FlushSegmentCompactionInfo, 0, len(createSegments))
	for _, segment := range createSegments {
		convertedPaths := make(map[string]*coordinatorpb.FilePaths)
		for k, v := range segment.FilePaths {
			convertedPaths[k] = &coordinatorpb.FilePaths{Paths: v}
		}

		info := &coordinatorpb.FlushSegmentCompactionInfo{
			SegmentId: segment.ID.String(),
			FilePaths: convertedPaths,
		}
		segmentCompactionInfos = append(segmentCompactionInfos, info)
	}

	collectionVersionFilePb := &coordinatorpb.CollectionVersionFile{
		CollectionInfoImmutable: &coordinatorpb.CollectionInfoImmutable{
			TenantId:               createCollection.TenantID,
			DatabaseId:             databaseID,
			CollectionId:           createCollection.ID.String(),
			CollectionName:         createCollection.Name,
			CollectionCreationSecs: int64(ts),
		},
		VersionHistory: &coordinatorpb.CollectionVersionHistory{
			Versions: []*coordinatorpb.CollectionVersionInfo{
				{
					Version:       0,
					CreatedAtSecs: int64(ts),
					SegmentInfo: &coordinatorpb.CollectionSegmentInfo{
						SegmentCompactionInfo: segmentCompactionInfos,
					},
				},
			},
		},
	}
	// Construct the version file name.
	versionFileName := "0"
	fullFilePath, err := tc.s3Store.PutVersionFile(ctx, createCollection.TenantID, databaseID, createCollection.ID.String(), versionFileName, collectionVersionFilePb)
	if err != nil {
		return "", err
	}
	return fullFilePath, nil
}

func (tc *Catalog) CreateCollectionAndSegments(ctx context.Context, createCollection *model.CreateCollection, createSegments []*model.Segment, ts types.Timestamp) (*model.Collection, bool, error) {
	var resultCollection *model.Collection
	created := false

	if createCollection.GetOrCreate {
		existingCollections, err := tc.metaDomain.CollectionDb(ctx).GetCollections(nil, &createCollection.Name, createCollection.TenantID, createCollection.DatabaseName, nil, nil, false)

		if err != nil {
			log.Error("error getting existing collection", zap.Error(err))
			return nil, false, err
		}
		if len(existingCollections) > 0 {
			log.Info("collection already exists, skipping creation")
			return convertCollectionToModel(existingCollections)[0], false, nil
		}
	}

	// Create the first Version file in S3.
	// If the transaction below fails, then there will be an orphan file in S3.
	// This orphan file will not affect new collection creations.
	// An alternative approach is to create this file after the transaction is committed.
	// and let FlushCollectionCompaction do any repair work if first version file is missing.
	versionFileName := ""
	var err error
	if tc.versionFileEnabled {
		databases, err := tc.metaDomain.DatabaseDb(ctx).GetDatabases(createCollection.TenantID, createCollection.DatabaseName)
		if err != nil {
			log.Error("error getting database", zap.Error(err))
			return nil, false, err
		}
		if len(databases) == 0 {
			log.Error("database not found for database", zap.String("database_name", createCollection.DatabaseName), zap.String("tenant_id", createCollection.TenantID))
			return nil, false, common.ErrDatabaseNotFound
		}

		versionFileName, err = tc.createFirstVersionFile(ctx, databases[0].ID, createCollection, createSegments, ts)
		if err != nil {
			return nil, false, err
		}
	}

	log.Info("creating collection and segments", zap.Any("createCollection", createCollection), zap.Any("createSegments", createSegments), zap.Any("versionFileName", versionFileName))
	err = tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Create the collection using the refactored helper
		var err error
		resultCollection, created, err = tc.createCollectionImpl(txCtx, createCollection, versionFileName, ts)
		if err != nil {
			log.Error("error creating collection", zap.Error(err))
			return err
		}

		// If collection already exists, then do not create segments.
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

func (tc *Catalog) SetTenantResourceName(ctx context.Context, tenantID string, resourceName string) error {
	return tc.metaDomain.TenantDb(ctx).SetTenantResourceName(tenantID, resourceName)
}

// ListCollectionVersions lists all versions of a collection that have not been marked for deletion.
func (tc *Catalog) ListCollectionVersions(ctx context.Context,
	collectionID types.UniqueID,
	tenantID string,
	maxCount *int64,
	versionsBefore *int64,
	versionsAtOrAfter *int64,
	includeMarkedForDeletion bool,
) ([]*coordinatorpb.CollectionVersionInfo, error) {
	// Get collection entry to get version file name
	isDeleted := false
	collectionEntry, err := tc.metaDomain.CollectionDb(ctx).GetCollectionWithoutMetadata(types.FromUniqueID(collectionID), nil, &isDeleted)
	if err != nil {
		log.Error("error getting collection entry", zap.Error(err))
		return nil, err
	}
	if collectionEntry == nil {
		return nil, common.ErrCollectionNotFound
	}

	// Get version file from S3
	log.Info("getting version file from S3",
		zap.String("tenant_id", tenantID),
		zap.String("collection_id", collectionID.String()),
		zap.Int64("version", int64(collectionEntry.Version)),
		zap.String("version_file_name", collectionEntry.VersionFileName))

	versionFile, err := tc.s3Store.GetVersionFile(ctx, collectionEntry.VersionFileName)
	if err != nil {
		log.Error("error getting version file", zap.Error(err))
		return nil, err
	}

	if versionFile.GetVersionHistory() == nil || len(versionFile.GetVersionHistory().Versions) == 0 {
		return []*coordinatorpb.CollectionVersionInfo{}, nil
	}

	// Filter versions based on criteria and build result
	versions := versionFile.GetVersionHistory().Versions
	filteredVersions := make([]*coordinatorpb.CollectionVersionInfo, 0)

	for _, version := range versions {
		// Skip versions marked for deletion
		if version.MarkedForDeletion && !includeMarkedForDeletion {
			continue
		}

		// Apply time range filters if specified
		if versionsBefore != nil && version.CreatedAtSecs >= *versionsBefore {
			continue
		}
		if versionsAtOrAfter != nil && version.CreatedAtSecs < *versionsAtOrAfter {
			continue
		}

		filteredVersions = append(filteredVersions, version)
	}

	// Apply maxCount limit if specified
	if maxCount != nil && int64(len(filteredVersions)) > *maxCount {
		filteredVersions = filteredVersions[:*maxCount]
	}

	return filteredVersions, nil
}

func (tc *Catalog) modifyVersionFileInPlace(ctx context.Context, versionFilePb *coordinatorpb.CollectionVersionFile, flushCollectionCompaction *model.FlushCollectionCompaction, previousSegmentInfo []*model.Segment, ts_secs int64) error {
	segmentCompactionInfos := make([]*coordinatorpb.FlushSegmentCompactionInfo, 0, len(flushCollectionCompaction.FlushSegmentCompactions))
	// If flushCollectionCompaction.FlushSegmentCompactions is empty then use previousSegmentInfo.
	if len(flushCollectionCompaction.FlushSegmentCompactions) == 0 {
		for _, segment := range previousSegmentInfo {
			convertedPaths := make(map[string]*coordinatorpb.FilePaths)
			for k, v := range segment.FilePaths {
				convertedPaths[k] = &coordinatorpb.FilePaths{Paths: v}
			}
			info := &coordinatorpb.FlushSegmentCompactionInfo{
				SegmentId: segment.ID.String(),
				FilePaths: convertedPaths,
			}
			segmentCompactionInfos = append(segmentCompactionInfos, info)
		}
	} else {
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
	}

	versionFilePb.GetVersionHistory().Versions = append(versionFilePb.GetVersionHistory().Versions, &coordinatorpb.CollectionVersionInfo{
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
	return nil
}

func (tc *Catalog) updateVersionFileInS3(ctx context.Context, versionFilePb *coordinatorpb.CollectionVersionFile, flushCollectionCompaction *model.FlushCollectionCompaction) (string, error) {

	// Write the new version file to S3.
	// Format of version file name: <version>_<uuid>_flush
	// The version should be left padded with 0s upto 6 digits.
	newVersionFileName := fmt.Sprintf("%06d_%s_flush", flushCollectionCompaction.CurrentCollectionVersion+1, uuid.New().String())
	fullFilePath, err := tc.s3Store.PutVersionFile(ctx, flushCollectionCompaction.TenantID, versionFilePb.CollectionInfoImmutable.DatabaseId, flushCollectionCompaction.ID.String(), newVersionFileName, versionFilePb)
	if err != nil {
		return "", err
	}

	return fullFilePath, nil
}

func (tc *Catalog) FlushCollectionCompaction(ctx context.Context, flushCollectionCompaction *model.FlushCollectionCompaction) (*model.FlushCollectionInfo, error) {
	// This is the core path now, since version files are enabled
	if tc.versionFileEnabled {
		return tc.FlushCollectionCompactionForVersionedCollection(ctx, flushCollectionCompaction, nil)
	}
	collectionID := types.FromUniqueID(flushCollectionCompaction.ID)

	flushCollectionInfo := &model.FlushCollectionInfo{
		ID: flushCollectionCompaction.ID.String(),
	}

	// Use explicit transaction parameter to ensure both operations run in the same transaction
	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// Check if collection exists.
		collection, err := tc.metaDomain.CollectionDb(txCtx).GetCollectionWithoutMetadata(collectionID, nil, nil)
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
		lastCompactionTime := time.Now().Unix()
		collectionVersion, err := tc.metaDomain.CollectionDb(txCtx).UpdateLogPositionVersionTotalRecordsAndLogicalSize(flushCollectionCompaction.ID.String(), flushCollectionCompaction.LogPosition, flushCollectionCompaction.CurrentCollectionVersion, flushCollectionCompaction.TotalRecordsPostCompaction, flushCollectionCompaction.SizeBytesPostCompaction, uint64(lastCompactionTime), flushCollectionCompaction.TenantID, flushCollectionCompaction.SchemaStr)
		if err != nil {
			return err
		}
		flushCollectionInfo.CollectionVersion = collectionVersion

		// update tenant last compaction time
		// TODO: add a system configuration to disable
		// since this might cause resource contention if one tenant has a lot of collection compactions at the same time
		err = tc.metaDomain.TenantDb(txCtx).UpdateTenantLastCompactionTime(flushCollectionCompaction.TenantID, lastCompactionTime)
		if err != nil {
			return err
		}
		flushCollectionInfo.TenantLastCompactionTime = lastCompactionTime

		// return nil will commit the transaction
		return nil
	})
	log.Info("FlushCollectionCompaction", zap.String("collection_id", *collectionID), zap.Int64("log_position", flushCollectionCompaction.LogPosition))
	if err != nil {
		return nil, err
	}
	return flushCollectionInfo, nil
}

// FlushCollectionCompactionAndAttachedFunction atomically updates collection compaction data and attached function completion offset.
// NOTE: This does NOT advance next_nonce - that is done separately by AdvanceAttachedFunction.
// This only updates the completion_offset to record how far we've processed.
// This is only supported for versioned collections (the modern/default path).
func (tc *Catalog) FlushCollectionCompactionAndAttachedFunction(
	ctx context.Context,
	flushCollectionCompaction *model.FlushCollectionCompaction,
	attachedFunctionID uuid.UUID,
	runNonce uuid.UUID,
	completionOffset int64,
) (*model.FlushCollectionInfo, error) {
	if !tc.versionFileEnabled {
		// Attached-function-based compactions are only supported with versioned collections
		log.Error("FlushCollectionCompactionAndAttachedFunction is only supported for versioned collections")
		return nil, errors.New("attached-function-based compaction requires versioned collections")
	}

	var flushCollectionInfo *model.FlushCollectionInfo

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		var err error
		// Get the transaction from context to pass to FlushCollectionCompactionForVersionedCollection
		tx := dbcore.GetDB(txCtx)
		flushCollectionInfo, err = tc.FlushCollectionCompactionForVersionedCollection(txCtx, flushCollectionCompaction, tx)
		if err != nil {
			return err
		}

		// Update ONLY completion_offset - next_nonce was already advanced by AdvanceAttachedFunction
		// We still validate runNonce to ensure we're updating the correct nonce
		err = tc.metaDomain.AttachedFunctionDb(txCtx).UpdateCompletionOffset(attachedFunctionID, runNonce, completionOffset)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// Populate attached function fields with authoritative values from database
	flushCollectionInfo.AttachedFunctionCompletionOffset = &completionOffset

	log.Info("FlushCollectionCompactionAndAttachedFunction",
		zap.String("collection_id", flushCollectionCompaction.ID.String()),
		zap.String("attached_function_id", attachedFunctionID.String()),
		zap.Int64("completion_offset", completionOffset))

	return flushCollectionInfo, nil
}

func (tc *Catalog) validateVersionFile(versionFile *coordinatorpb.CollectionVersionFile, collectionID string, version int64) error {
	if versionFile.GetCollectionInfoImmutable().GetCollectionId() != collectionID {
		log.Error("collection id mismatch", zap.String("collection_id", collectionID), zap.String("version_file_collection_id", versionFile.GetCollectionInfoImmutable().GetCollectionId()))
		return errors.New("collection id mismatch")
	}
	if versionFile.GetVersionHistory() == nil || len(versionFile.GetVersionHistory().GetVersions()) == 0 {
		log.Error("version history is empty")
		return errors.New("version history is empty")
	}
	versions := versionFile.GetVersionHistory().GetVersions()
	seenPaths := false
	if versions != nil && len(versions) > 1 {
		for idx, vx := range versions {
			if idx == 0 {
				continue
			}
			segments := vx.GetSegmentInfo().GetSegmentCompactionInfo()
			if segments == nil || len(segments) == 0 {
				log.Error("version has no segments", zap.String("collection_id", collectionID), zap.Int64("version", vx.Version))
				return errors.New("version has no segments")
			}
			for _, seg := range segments {
				file_paths := seg.GetFilePaths()
				if seenPaths && (file_paths == nil || len(file_paths) == 0) {
					log.Error("version has no file paths", zap.String("collection_id", collectionID), zap.Int64("version", vx.Version), zap.String("segment_id", seg.GetSegmentId()))
					return errors.New("version has no file paths")
				} else if file_paths != nil && len(file_paths) > 0 {
					seenPaths = true
				}
			}
		}
	}
	lastVersion := versions[len(versions)-1].GetVersion()
	if lastVersion != version {
		// Extract all version numbers for logging
		versionNumbers := make([]int64, len(versions))
		for i, v := range versions {
			versionNumbers[i] = v.GetVersion()
		}
		log.Error("version mismatch",
			zap.Int64("expected_version", version),
			zap.Int64("last_version", lastVersion),
			zap.Int64s("version_history", versionNumbers))
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
func (tc *Catalog) FlushCollectionCompactionForVersionedCollection(ctx context.Context, flushCollectionCompaction *model.FlushCollectionCompaction, tx *gorm.DB) (*model.FlushCollectionInfo, error) {
	// The result that is sent back to the Compactor.
	flushCollectionInfo := &model.FlushCollectionInfo{
		ID: flushCollectionCompaction.ID.String(),
	}

	log.Info("FlushCollectionCompaction", zap.String("collection_id", flushCollectionInfo.ID), zap.Int64("log_position", flushCollectionCompaction.LogPosition))

	// If a transaction is provided, do a single attempt without retry - any failure should propagate up
	// to let the outer transaction fail atomically.
	maxAttemptsForThisCall := maxAttempts
	if tx != nil {
		maxAttemptsForThisCall = 1
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
	for numAttempts < maxAttemptsForThisCall {
		numAttempts++
		// Get the current version info and the version file from the table.
		collectionEntry, segments, err := tc.GetCollectionWithSegments(ctx, flushCollectionCompaction.ID, true)
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
			// Compactor is trying to flush a stale version, since
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
		var existingVersionFilePb *coordinatorpb.CollectionVersionFile
		if existingVersionFileName == "" {
			// The VersionFile has not been created.
			existingVersionFilePb = &coordinatorpb.CollectionVersionFile{
				CollectionInfoImmutable: &coordinatorpb.CollectionInfoImmutable{
					TenantId:               collectionEntry.TenantID,
					DatabaseId:             collectionEntry.DatabaseId.String(),
					CollectionId:           collectionEntry.ID.String(),
					CollectionName:         collectionEntry.Name,
					CollectionCreationSecs: collectionEntry.CreatedAt.Unix(),
				},
				VersionHistory: &coordinatorpb.CollectionVersionHistory{
					Versions: []*coordinatorpb.CollectionVersionInfo{},
				},
			}
		} else {
			// Read the VersionFile from S3MetaStore.
			existingVersionFilePb, err = tc.s3Store.GetVersionFile(ctx, existingVersionFileName)
			if err != nil {
				return nil, err
			}

			// There was previously a bug that resulted in the tenant ID missing from some version files (https://github.com/chroma-core/chroma/pull/4408).
			// This line can be removed once all corrupted version files are fixed.
			existingVersionFilePb.CollectionInfoImmutable.TenantId = collectionEntry.TenantID
		}

		err = tc.modifyVersionFileInPlace(ctx, existingVersionFilePb, flushCollectionCompaction, segments, time.Now().Unix())
		if err != nil {
			log.Error("version file modification failed", zap.Error(err))
			return nil, err
		}
		existingVersion += 1

		err = tc.validateVersionFile(existingVersionFilePb, collectionEntry.ID.String(), existingVersion)
		if err != nil {
			log.Error("version file validation failed", zap.Error(err))
			return nil, err
		}

		// The update function takes the content of the existing version file,
		// and the set of segments that are part of the new version file.
		// NEW VersionFile is created in S3 at this step.
		newVersionFileName, err := tc.updateVersionFileInS3(ctx, existingVersionFilePb, flushCollectionCompaction)
		if err != nil {
			return nil, err
		}

		numActiveVersions := tc.getNumberOfActiveVersions(existingVersionFilePb)

		// Execute the database operations - either within provided transaction or new transaction
		var txErr error

		executeOperations := func(ctx context.Context, tx *gorm.DB) error {
			// NOTE: DO NOT move UpdateTenantLastCompactionTime & RegisterFilePaths to the end of the transaction.
			//		 Keep both these operations before the UpdateLogPositionAndVersionInfo.
			//       UpdateLogPositionAndVersionInfo acts as a CAS operation whose failure will roll back the transaction.
			//       If order is changed, we can still potentially loose an update to Collection entry by
			//       a concurrent transaction that updates Collection entry immediately after UpdateLogPositionAndVersionInfo completes.
			// The other approach is to use a "SELECT FOR UPDATE" to lock the Collection entry at the start of the transaction,
			// which is costlier than the current approach that does not lock the Collection entry.

			// Create context with transaction if provided
			if tx != nil {
				ctx = dbcore.CtxWithTransaction(ctx, tx)
			}

			// register files to Segment metadata
			err := tc.metaDomain.SegmentDb(ctx).RegisterFilePaths(flushCollectionCompaction.FlushSegmentCompactions)
			if err != nil {
				return err
			}
			// update tenant last compaction time
			// TODO: add a system configuration to disable
			// since this might cause resource contention if one tenant has a lot of collection compactions at the same time
			lastCompactionTime := time.Now().Unix()
			err = tc.metaDomain.TenantDb(ctx).UpdateTenantLastCompactionTime(flushCollectionCompaction.TenantID, lastCompactionTime)
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
			rowsAffected, err := tc.metaDomain.CollectionDb(ctx).UpdateLogPositionAndVersionInfo(
				flushCollectionCompaction.ID.String(),
				flushCollectionCompaction.LogPosition,
				flushCollectionCompaction.CurrentCollectionVersion,
				existingVersionFileName,
				flushCollectionCompaction.CurrentCollectionVersion+1,
				newVersionFileName,
				flushCollectionCompaction.TotalRecordsPostCompaction,
				flushCollectionCompaction.SizeBytesPostCompaction,
				// SAFETY(hammadb): This int64 to uint64 conversion is ok because we always are in post-epoch time.
				// and the value is always positive.
				uint64(lastCompactionTime),
				uint64(numActiveVersions),
				flushCollectionCompaction.SchemaStr,
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

			// Success
			return nil
		}

		// Check if a transaction was provided - if so, use it directly instead of creating nested transaction
		if tx != nil {
			// Use provided transaction directly - no nested transaction
			txErr = executeOperations(ctx, tx)
		} else {
			// Create new transaction
			txErr = tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
				return executeOperations(txCtx, nil)
			})
		}

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

func (tc *Catalog) updateProtoWithMarkedForDeletion(versionFilePb *coordinatorpb.CollectionVersionFile, versions []int64) error {
	// Check if version history exists
	if versionFilePb.GetVersionHistory() == nil || len(versionFilePb.GetVersionHistory().Versions) == 0 {
		log.Error("version history not found")
		return errors.New("version history not found")
	}

	// Create a map for lookup of requested versions
	requestedVersions := make(map[int64]bool)
	for _, v := range versions {
		requestedVersions[v] = true
	}

	// Find and mark the requested versions
	versionsFound := 0
	for _, version := range versionFilePb.GetVersionHistory().Versions {
		if requestedVersions[version.Version] {
			version.MarkedForDeletion = true
			versionsFound++
		}
	}

	// Check if all requested versions were found
	if versionsFound != len(versions) {
		log.Error("requested versions not found", zap.Int("versions_found", versionsFound), zap.Int("requested_versions", len(versions)))
		return errors.New("requested versions not found in the version file")
	}

	return nil
}

// Mark the versions for deletion.
// GC minics a 2PC protocol.
// 1. Mark the versions for deletion by calling MarkVersionForDeletion.
// 2. Compute the diffs and delete the files from S3.
// 3. Delete the versions from the version file by calling DeleteCollectionVersion.
//
// NOTE about concurrency:
// This method updates the version file which can concurrently with FlushCollectionCompaction.
func (tc *Catalog) markVersionForDeletionInSingleCollection(
	ctx context.Context,
	tenantID string,
	collectionID string,
	versions []int64,
) error {
	// Logic -
	// Read the existing version file.
	// Prepare the new version file with the marked versions.
	// Write the new version file to S3.
	// Update the version file name in Postgres table.

	// Limit the loop to 10 attempts to avoid infinite loops.
	numAttempts := 0
	for {
		numAttempts++
		if numAttempts > maxAttemptsToMarkVersionForDeletion {
			return errors.New("too many attempts to mark version for deletion")
		}

		// Read the existing version file.
		collectionIDPtr := &collectionID
		isDeleted := false
		collectionEntry, err := tc.metaDomain.CollectionDb(ctx).GetCollectionWithoutMetadata(collectionIDPtr, nil, &isDeleted)
		if err != nil {
			return err
		}
		if collectionEntry == nil {
			return common.ErrCollectionNotFound
		}
		// TODO(rohit): log error if collection in file is different from the one in request.

		existingVersionFileName := collectionEntry.VersionFileName
		versionFilePb, err := tc.s3Store.GetVersionFile(ctx, existingVersionFileName)
		if err != nil {
			return err
		}

		err = tc.updateProtoWithMarkedForDeletion(versionFilePb, versions)
		if err != nil {
			return err
		}

		// Write the new version file to S3.
		// Create the new version file name with the following format:
		// <version_number>_<uuid>_gc_mark
		newVersionFileName := fmt.Sprintf(
			"%d_%s_gc_mark",
			collectionEntry.Version,
			uuid.New().String(),
		)
		newVerFileFullPath, err := tc.s3Store.PutVersionFile(ctx, tenantID, collectionEntry.DatabaseID, collectionID, newVersionFileName, versionFilePb)
		if err != nil {
			return err
		}

		// Update the version file name in Postgres table as a CAS operation.
		// TODO(rohit): Investigate if we really need a Tx here.
		rowsAffected, err := tc.metaDomain.CollectionDb(ctx).UpdateVersionRelatedFields(collectionID, existingVersionFileName, newVerFileFullPath, nil, nil)
		if err != nil {
			// Delete the newly created version file from S3 since it is not needed.
			tc.s3Store.DeleteVersionFile(ctx, tenantID, collectionEntry.DatabaseID, collectionID, newVersionFileName)
			return err
		}
		if rowsAffected == 0 {
			// CAS operation failed.
			// Retry the operation.
			log.Info("CAS operation failed", zap.String("collection_id", collectionID), zap.Int64s("versions", versions))
			continue
		}

		// CAS operation succeeded.
		return nil
	}
}

func (tc *Catalog) MarkVersionForDeletion(ctx context.Context, req *coordinatorpb.MarkVersionForDeletionRequest) (*coordinatorpb.MarkVersionForDeletionResponse, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.MarkVersionForDeletion")
		defer span.End()
	}

	result := coordinatorpb.MarkVersionForDeletionResponse{
		CollectionIdToSuccess: make(map[string]bool),
	}

	for _, collectionVersionList := range req.Versions {
		err := tc.markVersionForDeletionInSingleCollection(ctx, collectionVersionList.TenantId, collectionVersionList.CollectionId, collectionVersionList.Versions)
		result.CollectionIdToSuccess[collectionVersionList.CollectionId] = err == nil
	}

	return &result, nil
}

func (tc *Catalog) updateProtoRemoveVersionEntries(versionFilePb *coordinatorpb.CollectionVersionFile, versions []int64) error {
	// Check if version history exists
	if versionFilePb.GetVersionHistory() == nil {
		log.Error("version history not found")
		return errors.New("version history not found")
	}

	// Create a map for lookup of versions to be removed
	versionsToRemove := make(map[int64]bool)
	for _, v := range versions {
		versionsToRemove[v] = true
	}

	// Create a new slice to hold versions that should be kept
	newVersions := make([]*coordinatorpb.CollectionVersionInfo, 0, len(versionFilePb.GetVersionHistory().Versions))

	// Only keep versions that are not in the versionsToRemove map
	for _, version := range versionFilePb.GetVersionHistory().Versions {
		if !versionsToRemove[version.Version] {
			newVersions = append(newVersions, version)
		}
	}

	// Update the version history with the filtered versions
	versionFilePb.GetVersionHistory().Versions = newVersions

	return nil
}

func (tc *Catalog) getNumberOfActiveVersions(versionFilePb *coordinatorpb.CollectionVersionFile) int {
	// Use a map to track unique active versions
	activeVersions := make(map[int64]bool)
	for _, version := range versionFilePb.GetVersionHistory().Versions {
		activeVersions[version.Version] = true
	}
	return len(activeVersions)
}

func (tc *Catalog) getOldestVersionTs(versionFilePb *coordinatorpb.CollectionVersionFile) *time.Time {
	if versionFilePb.GetVersionHistory() == nil || len(versionFilePb.GetVersionHistory().Versions) == 0 {
		return nil
	}
	oldestVersionTs := versionFilePb.GetVersionHistory().Versions[0].CreatedAtSecs

	ts := time.Unix(oldestVersionTs, 0)
	return &ts
}

func (tc *Catalog) DeleteVersionEntriesForCollection(ctx context.Context, tenantID string, collectionID string, versions []int64) error {
	// Limit the loop to 5 attempts to avoid infinite loops
	numAttempts := 0
	for {
		numAttempts++
		if numAttempts > maxAttemptsToDeleteVersionEntries {
			return errors.New("too many attempts to delete version entries")
		}

		// Read the existing version file
		collectionIDPtr := &collectionID
		collectionEntry, err := tc.metaDomain.CollectionDb(ctx).GetCollectionWithoutMetadata(collectionIDPtr, nil, nil)
		if err != nil {
			return err
		}
		if collectionEntry == nil {
			return common.ErrCollectionNotFound
		}

		existingVersionFileName := collectionEntry.VersionFileName
		versionFilePb, err := tc.s3Store.GetVersionFile(ctx, existingVersionFileName)
		if err != nil {
			return err
		}

		err = tc.updateProtoRemoveVersionEntries(versionFilePb, versions)
		if err != nil {
			return err
		}

		numActiveVersions := tc.getNumberOfActiveVersions(versionFilePb)
		if numActiveVersions < 1 && !collectionEntry.IsDeleted {
			// No remaining valid versions after GC.
			return errors.New("no valid versions after gc")
		}

		// Get the creation time of the oldest version.
		oldestVersionTs := tc.getOldestVersionTs(versionFilePb)
		if oldestVersionTs == nil {
			if !collectionEntry.IsDeleted {
				return errors.New("oldest version timestamp is nil after GC, this should only happen if all versions are deleted")
			}
		} else if oldestVersionTs.IsZero() {
			// This should never happen.
			log.Error("oldest version timestamp is zero after GC.", zap.String("collection_id", collectionID))
			// No versions to delete.
			return errors.New("oldest version timestamp is zero after GC")
		}

		// Write the new version file to S3
		// Create the new version file name with the format: <version_number>_<uuid>_gc_delete
		newVersionFileName := fmt.Sprintf(
			"%d_%s_gc_delete",
			collectionEntry.Version,
			uuid.New().String(),
		)
		newVerFileFullPath, err := tc.s3Store.PutVersionFile(ctx, tenantID, collectionEntry.DatabaseID, collectionID, newVersionFileName, versionFilePb)
		if err != nil {
			return err
		}

		// Update the version file name in Postgres table as a CAS operation
		rowsAffected, err := tc.metaDomain.CollectionDb(ctx).UpdateVersionRelatedFields(collectionID, existingVersionFileName, newVerFileFullPath, oldestVersionTs, &numActiveVersions)
		if err != nil {
			// Delete the newly created version file from S3 since it is not needed
			tc.s3Store.DeleteVersionFile(ctx, tenantID, collectionEntry.DatabaseID, collectionID, newVersionFileName)
			return err
		}
		if rowsAffected == 0 {
			// CAS operation failed, retry the operation
			log.Info("CAS operation failed during version deletion",
				zap.String("collection_id", collectionID),
				zap.Int64s("versions", versions))
			continue
		}

		// CAS operation succeeded
		return nil
	}
}

func (tc *Catalog) DeleteCollectionVersion(ctx context.Context, req *coordinatorpb.DeleteCollectionVersionRequest) (*coordinatorpb.DeleteCollectionVersionResponse, error) {
	result := coordinatorpb.DeleteCollectionVersionResponse{
		CollectionIdToSuccess: make(map[string]bool),
	}
	var firstErr error
	for _, collectionVersionList := range req.Versions {
		err := tc.DeleteVersionEntriesForCollection(ctx, collectionVersionList.TenantId, collectionVersionList.CollectionId, collectionVersionList.Versions)
		result.CollectionIdToSuccess[collectionVersionList.CollectionId] = err == nil
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}
	return &result, firstErr
}

func (tc *Catalog) BatchGetCollectionVersionFilePaths(ctx context.Context, collectionIds []string) (*coordinatorpb.BatchGetCollectionVersionFilePathsResponse, error) {
	result := coordinatorpb.BatchGetCollectionVersionFilePathsResponse{
		CollectionIdToVersionFilePath: make(map[string]string),
	}

	paths, err := tc.metaDomain.CollectionDb(ctx).BatchGetCollectionVersionFilePaths(collectionIds)
	if err != nil {
		return nil, err
	}
	result.CollectionIdToVersionFilePath = paths

	return &result, nil
}

func (tc *Catalog) BatchGetCollectionSoftDeleteStatus(ctx context.Context, collectionIds []string) (*coordinatorpb.BatchGetCollectionSoftDeleteStatusResponse, error) {
	result := coordinatorpb.BatchGetCollectionSoftDeleteStatusResponse{
		CollectionIdToIsSoftDeleted: make(map[string]bool),
	}

	status, err := tc.metaDomain.CollectionDb(ctx).BatchGetCollectionSoftDeleteStatus(collectionIds)
	if err != nil {
		return nil, err
	}
	result.CollectionIdToIsSoftDeleted = status

	return &result, nil
}

func (tc *Catalog) GetVersionFileNamesForCollection(ctx context.Context, tenantID string, collectionID string) (string, error) {
	collectionIDPtr := &collectionID
	isDeleted := false
	collectionEntry, err := tc.metaDomain.CollectionDb(ctx).GetCollectionWithoutMetadata(collectionIDPtr, nil, &isDeleted)
	if err != nil {
		return "", err
	}
	if collectionEntry == nil {
		return "", common.ErrCollectionNotFound
	}

	return collectionEntry.VersionFileName, nil
}

func (tc *Catalog) FinishDatabaseDeletion(ctx context.Context, cutoffTime time.Time) (uint64, error) {
	return tc.metaDomain.DatabaseDb(ctx).FinishDatabaseDeletion(cutoffTime)
}
