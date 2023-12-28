package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/metastore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/notification"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// The catalog backed by databases using GORM.
type Catalog struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
	store      notification.NotificationStore
}

func NewTableCatalog(txImpl dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain) *Catalog {
	return &Catalog{
		txImpl:     txImpl,
		metaDomain: metaDomain,
	}
}

func NewTableCatalogWithNotification(txImpl dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain, store notification.NotificationStore) *Catalog {
	catalog := NewTableCatalog(txImpl, metaDomain)
	catalog.store = store
	return catalog
}

var _ metastore.Catalog = (*Catalog)(nil)

func (tc *Catalog) ResetState(ctx context.Context) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		err := tc.metaDomain.CollectionDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset collection db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reest collection metadata db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.SegmentDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset segment db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.SegmentMetadataDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset segment metadata db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.DatabaseDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset database db", zap.Error(err))
			return err
		}

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
			ID: common.DefaultTenant,
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

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
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
		dbTenant := &dbmodel.Tenant{
			ID: createTenant.Name,
			Ts: ts,
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

func (tc *Catalog) CreateCollection(ctx context.Context, createCollection *model.CreateCollection, ts types.Timestamp) (*model.Collection, error) {
	var result *model.Collection

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// insert collection
		databaseName := createCollection.DatabaseName
		tenantID := createCollection.TenantID
		databases, err := tc.metaDomain.DatabaseDb(txCtx).GetDatabases(tenantID, databaseName)
		if err != nil {
			log.Error("error getting database", zap.Error(err))
			return err
		}
		if len(databases) == 0 {
			log.Error("database not found", zap.Error(err))
			return common.ErrDatabaseNotFound
		}

		collectionName := createCollection.Name
		existing, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(types.FromUniqueID(createCollection.ID), &collectionName, nil, tenantID, databaseName)
		if err != nil {
			log.Error("error getting collection", zap.Error(err))
			return err
		}
		if len(existing) != 0 {
			if createCollection.GetOrCreate {
				collection := convertCollectionToModel(existing)[0]
				if createCollection.Metadata != nil && !createCollection.Metadata.Equals(collection.Metadata) {
					updatedCollection, err := tc.UpdateCollection(ctx, &model.UpdateCollection{
						ID:           collection.ID,
						Metadata:     createCollection.Metadata,
						TenantID:     tenantID,
						DatabaseName: databaseName,
					}, ts)
					if err != nil {
						log.Error("error updating collection", zap.Error(err))
					}
					result = updatedCollection
				} else {
					result = collection
				}
				return nil
			} else {
				return common.ErrCollectionUniqueConstraintViolation
			}
		}

		dbCollection := &dbmodel.Collection{
			ID:         createCollection.ID.String(),
			Name:       &createCollection.Name,
			Topic:      &createCollection.Topic,
			Dimension:  createCollection.Dimension,
			DatabaseID: databases[0].ID,
			Ts:         ts,
		}

		err = tc.metaDomain.CollectionDb(txCtx).Insert(dbCollection)
		if err != nil {
			log.Error("error inserting collection", zap.Error(err))
			return err
		}
		// insert collection metadata
		metadata := createCollection.Metadata
		dbCollectionMetadataList := convertCollectionMetadataToDB(createCollection.ID.String(), metadata)
		if len(dbCollectionMetadataList) != 0 {
			err = tc.metaDomain.CollectionMetadataDb(txCtx).Insert(dbCollectionMetadataList)
			if err != nil {
				return err
			}
		}
		// get collection
		collectionList, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(types.FromUniqueID(createCollection.ID), nil, nil, tenantID, databaseName)
		if err != nil {
			log.Error("error getting collection", zap.Error(err))
			return err
		}
		result = convertCollectionToModel(collectionList)[0]
		result.Created = true

		notificationRecord := &dbmodel.Notification{
			CollectionID: result.ID.String(),
			Type:         dbmodel.NotificationTypeCreateCollection,
			Status:       dbmodel.NotificationStatusPending,
		}
		err = tc.metaDomain.NotificationDb(txCtx).Insert(notificationRecord)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Error("error creating collection", zap.Error(err))
		return nil, err
	}
	log.Info("collection created", zap.Any("collection", result))
	return result, nil
}

func (tc *Catalog) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string, tenandID string, databaseName string) ([]*model.Collection, error) {
	collectionAndMetadataList, err := tc.metaDomain.CollectionDb(ctx).GetCollections(types.FromUniqueID(collectionID), collectionName, collectionTopic, tenandID, databaseName)
	if err != nil {
		return nil, err
	}
	collections := convertCollectionToModel(collectionAndMetadataList)
	return collections, nil
}

func (tc *Catalog) DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		collectionID := deleteCollection.ID
		err := tc.metaDomain.CollectionDb(txCtx).DeleteCollectionByID(collectionID.String())
		if err != nil {
			return err
		}
		err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(collectionID.String())
		if err != nil {
			return err
		}
		notificationRecord := &dbmodel.Notification{
			CollectionID: collectionID.String(),
			Type:         dbmodel.NotificationTypeDeleteCollection,
			Status:       dbmodel.NotificationStatusPending,
		}
		err = tc.metaDomain.NotificationDb(txCtx).Insert(notificationRecord)
		if err != nil {
			return err
		}
		return nil
	})
}

func (tc *Catalog) UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection, ts types.Timestamp) (*model.Collection, error) {
	var result *model.Collection

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		dbCollection := &dbmodel.Collection{
			ID:        updateCollection.ID.String(),
			Name:      updateCollection.Name,
			Topic:     updateCollection.Topic,
			Dimension: updateCollection.Dimension,
			Ts:        ts,
		}
		err := tc.metaDomain.CollectionDb(txCtx).Update(dbCollection)
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
				err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(updateCollection.ID.String())
				if err != nil {
					return err
				}
			}
		} else {
			if metadata != nil { // Case 3
				err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(updateCollection.ID.String())
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
		collectionList, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(types.FromUniqueID(updateCollection.ID), nil, nil, tenantID, databaseName)
		if err != nil {
			return err
		}
		result = convertCollectionToModel(collectionList)[0]
		return nil
	})
	if err != nil {
		return nil, err
	}
	log.Info("collection updated", zap.Any("collection", result))
	return result, nil
}

func (tc *Catalog) CreateSegment(ctx context.Context, createSegment *model.CreateSegment, ts types.Timestamp) (*model.Segment, error) {
	var result *model.Segment

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// insert segment
		collectionString := createSegment.CollectionID.String()
		dbSegment := &dbmodel.Segment{
			ID:           createSegment.ID.String(),
			CollectionID: &collectionString,
			Type:         createSegment.Type,
			Scope:        createSegment.Scope,
			Ts:           ts,
		}
		if createSegment.Topic != nil {
			dbSegment.Topic = createSegment.Topic
		}
		err := tc.metaDomain.SegmentDb(txCtx).Insert(dbSegment)
		if err != nil {
			log.Error("error inserting segment", zap.Error(err))
			return err
		}
		// insert segment metadata
		metadata := createSegment.Metadata
		if metadata != nil {
			dbSegmentMetadataList := convertSegmentMetadataToDB(createSegment.ID.String(), metadata)
			if len(dbSegmentMetadataList) != 0 {
				err = tc.metaDomain.SegmentMetadataDb(txCtx).Insert(dbSegmentMetadataList)
				if err != nil {
					log.Error("error inserting segment metadata", zap.Error(err))
					return err
				}
			}
		}
		// get segment
		segmentList, err := tc.metaDomain.SegmentDb(txCtx).GetSegments(createSegment.ID, nil, nil, nil, types.NilUniqueID())
		if err != nil {
			log.Error("error getting segment", zap.Error(err))
			return err
		}
		result = convertSegmentToModel(segmentList)[0]
		return nil
	})
	if err != nil {
		log.Error("error creating segment", zap.Error(err))
		return nil, err
	}
	log.Info("segment created", zap.Any("segment", result))
	return result, nil
}

func (tc *Catalog) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID, ts types.Timestamp) ([]*model.Segment, error) {
	segmentAndMetadataList, err := tc.metaDomain.SegmentDb(ctx).GetSegments(segmentID, segmentType, scope, topic, collectionID)
	if err != nil {
		return nil, err
	}
	segments := make([]*model.Segment, 0, len(segmentAndMetadataList))
	for _, segmentAndMetadata := range segmentAndMetadataList {
		segment := &model.Segment{
			ID:    types.MustParse(segmentAndMetadata.Segment.ID),
			Type:  segmentAndMetadata.Segment.Type,
			Scope: segmentAndMetadata.Segment.Scope,
			Topic: segmentAndMetadata.Segment.Topic,
			Ts:    segmentAndMetadata.Segment.Ts,
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

func (tc *Catalog) DeleteSegment(ctx context.Context, segmentID types.UniqueID) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		err := tc.metaDomain.SegmentDb(txCtx).DeleteSegmentByID(segmentID.String())
		if err != nil {
			log.Error("error deleting segment", zap.Error(err))
			return err
		}
		err = tc.metaDomain.SegmentMetadataDb(txCtx).DeleteBySegmentID(segmentID.String())
		if err != nil {
			log.Error("error deleting segment metadata", zap.Error(err))
			return err
		}
		return nil
	})
}

func (tc *Catalog) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment, ts types.Timestamp) (*model.Segment, error) {
	var result *model.Segment

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// update segment
		dbSegment := &dbmodel.UpdateSegment{
			ID:              updateSegment.ID.String(),
			Topic:           updateSegment.Topic,
			ResetTopic:      updateSegment.ResetTopic,
			Collection:      updateSegment.Collection,
			ResetCollection: updateSegment.ResetCollection,
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
		segmentList, err := tc.metaDomain.SegmentDb(txCtx).GetSegments(updateSegment.ID, nil, nil, nil, types.NilUniqueID())
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
