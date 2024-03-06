package coordinator

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgconn"
	"sync"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/notification"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// IMeta is an interface that defines methods for the cache of the catalog.
type IMeta interface {
	ResetState(ctx context.Context) error
	AddCollection(ctx context.Context, createCollection *model.CreateCollection) (*model.Collection, error)
	GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string, tenantID string, databaseName string) ([]*model.Collection, error)
	DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error
	UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection) (*model.Collection, error)
	AddSegment(ctx context.Context, createSegment *model.CreateSegment) error
	GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*model.Segment, error)
	DeleteSegment(ctx context.Context, segmentID types.UniqueID) error
	UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment) (*model.Segment, error)
	CreateDatabase(ctx context.Context, createDatabase *model.CreateDatabase) (*model.Database, error)
	GetDatabase(ctx context.Context, getDatabase *model.GetDatabase) (*model.Database, error)
	CreateTenant(ctx context.Context, createTenant *model.CreateTenant) (*model.Tenant, error)
	GetTenant(ctx context.Context, getTenant *model.GetTenant) (*model.Tenant, error)
	SetNotificationProcessor(notificationProcessor notification.NotificationProcessor)
}

// MetaTable is an implementation of IMeta. It loads the system catalog during startup
// and caches in memory. The implmentation needs to make sure that the in memory cache
// is consistent with the system catalog.
//
// Operations of MetaTable are protected by a read write lock and are thread safe.
type MetaTable struct {
	ddLock                        sync.RWMutex
	ctx                           context.Context
	catalog                       metastore.Catalog
	segmentsCache                 map[types.UniqueID]*model.Segment
	tenantDatabaseCollectionCache map[string]map[string]map[types.UniqueID]*model.Collection
	tenantDatabaseCache           map[string]map[string]*model.Database
	notificationProcessor         notification.NotificationProcessor
}

var _ IMeta = (*MetaTable)(nil)

func NewMetaTable(ctx context.Context, catalog metastore.Catalog) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:                           ctx,
		catalog:                       catalog,
		segmentsCache:                 make(map[types.UniqueID]*model.Segment),
		tenantDatabaseCollectionCache: make(map[string]map[string]map[types.UniqueID]*model.Collection),
		tenantDatabaseCache:           make(map[string]map[string]*model.Database),
	}
	if err := mt.reloadWithLock(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reloadWithLock() error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()
	return mt.reload()
}

func (mt *MetaTable) reload() error {
	tenants, err := mt.catalog.GetAllTenants(mt.ctx, 0)
	if err != nil {
		return err
	}
	for _, tenant := range tenants {
		tenantID := tenant.Name
		mt.tenantDatabaseCollectionCache[tenantID] = make(map[string]map[types.UniqueID]*model.Collection)
		mt.tenantDatabaseCache[tenantID] = make(map[string]*model.Database)
	}
	// reload databases
	databases, err := mt.catalog.GetAllDatabases(mt.ctx, 0)
	if err != nil {
		return err
	}
	for _, database := range databases {
		databaseName := database.Name
		tenantID := database.Tenant
		mt.tenantDatabaseCollectionCache[tenantID][databaseName] = make(map[types.UniqueID]*model.Collection)
		mt.tenantDatabaseCache[tenantID][databaseName] = database
	}
	for tenantID, databases := range mt.tenantDatabaseCollectionCache {
		for databaseName := range databases {
			collections, err := mt.catalog.GetCollections(mt.ctx, types.NilUniqueID(), nil, nil, tenantID, databaseName)
			if err != nil {
				return err
			}
			for _, collection := range collections {
				mt.tenantDatabaseCollectionCache[tenantID][databaseName][collection.ID] = collection
			}
		}
	}

	oldSegments, err := mt.catalog.GetSegments(mt.ctx, types.NilUniqueID(), nil, nil, nil, types.NilUniqueID(), 0)
	if err != nil {
		return err
	}
	// reload is idempotent
	mt.segmentsCache = make(map[types.UniqueID]*model.Segment)
	for _, segment := range oldSegments {
		mt.segmentsCache[segment.ID] = segment
	}
	return nil
}

func (mt *MetaTable) SetNotificationProcessor(notificationProcessor notification.NotificationProcessor) {
	mt.notificationProcessor = notificationProcessor
}

func (mt *MetaTable) ResetState(ctx context.Context) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if err := mt.catalog.ResetState(ctx); err != nil {
		return err
	}
	mt.segmentsCache = make(map[types.UniqueID]*model.Segment)
	mt.tenantDatabaseCache = make(map[string]map[string]*model.Database)
	mt.tenantDatabaseCollectionCache = make(map[string]map[string]map[types.UniqueID]*model.Collection)

	if err := mt.reload(); err != nil {
		return err
	}
	return nil
}

func (mt *MetaTable) CreateDatabase(ctx context.Context, createDatabase *model.CreateDatabase) (*model.Database, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	tenant := createDatabase.Tenant
	databaseName := createDatabase.Name
	if _, ok := mt.tenantDatabaseCache[tenant]; !ok {
		log.Error("tenant not found", zap.Any("tenant", tenant))
		return nil, common.ErrTenantNotFound
	}
	if _, ok := mt.tenantDatabaseCache[tenant][databaseName]; ok {
		log.Error("database already exists", zap.Any("database", databaseName))
		return nil, common.ErrDatabaseUniqueConstraintViolation
	}
	database, err := mt.catalog.CreateDatabase(ctx, createDatabase, createDatabase.Ts)
	if err != nil {
		log.Info("create database failed", zap.Error(err))
		return nil, err
	}
	mt.tenantDatabaseCache[tenant][databaseName] = database
	mt.tenantDatabaseCollectionCache[tenant][databaseName] = make(map[types.UniqueID]*model.Collection)
	return database, nil
}

func (mt *MetaTable) GetDatabase(ctx context.Context, getDatabase *model.GetDatabase) (*model.Database, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	tenant := getDatabase.Tenant
	databaseName := getDatabase.Name
	if _, ok := mt.tenantDatabaseCache[tenant]; !ok {
		log.Error("tenant not found", zap.Any("tenant", tenant))
		return nil, common.ErrTenantNotFound
	}
	if _, ok := mt.tenantDatabaseCache[tenant][databaseName]; !ok {
		log.Error("database not found", zap.Any("database", databaseName))
		return nil, common.ErrDatabaseNotFound
	}

	return mt.tenantDatabaseCache[tenant][databaseName], nil
}

func (mt *MetaTable) CreateTenant(ctx context.Context, createTenant *model.CreateTenant) (*model.Tenant, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	tenantName := createTenant.Name
	if _, ok := mt.tenantDatabaseCache[tenantName]; ok {
		log.Error("tenant already exists", zap.Any("tenant", tenantName))
		return nil, common.ErrTenantUniqueConstraintViolation
	}
	tenant, err := mt.catalog.CreateTenant(ctx, createTenant, createTenant.Ts)
	if err != nil {
		return nil, err
	}
	mt.tenantDatabaseCache[tenantName] = make(map[string]*model.Database)
	mt.tenantDatabaseCollectionCache[tenantName] = make(map[string]map[types.UniqueID]*model.Collection)
	return tenant, nil
}

func (mt *MetaTable) GetTenant(ctx context.Context, getTenant *model.GetTenant) (*model.Tenant, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()
	tenantID := getTenant.Name
	if _, ok := mt.tenantDatabaseCache[tenantID]; !ok {
		log.Error("tenant not found", zap.Any("tenant", tenantID))
		return nil, common.ErrTenantNotFound
	}
	return &model.Tenant{Name: tenantID}, nil
}

func (mt *MetaTable) AddCollection(ctx context.Context, createCollection *model.CreateCollection) (*model.Collection, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	tenantID := createCollection.TenantID
	databaseName := createCollection.DatabaseName
	if _, ok := mt.tenantDatabaseCollectionCache[tenantID]; !ok {
		log.Error("tenant not found", zap.Any("tenantID", tenantID))
		return nil, common.ErrTenantNotFound
	}
	if _, ok := mt.tenantDatabaseCollectionCache[tenantID][databaseName]; !ok {
		log.Error("database not found", zap.Any("databaseName", databaseName))
		return nil, common.ErrDatabaseNotFound
	}
	collection, err := mt.catalog.CreateCollection(ctx, createCollection, createCollection.Ts)
	if err != nil {
		log.Error("create collection failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			log.Error("Postgres Error")
			switch pgErr.Code {
			case "23505":
				log.Error("collection id already exists")
				return nil, common.ErrCollectionUniqueConstraintViolation
			default:
				return nil, err
			}
		}
		return nil, err
	}
	mt.tenantDatabaseCollectionCache[tenantID][databaseName][collection.ID] = collection
	log.Info("collection added", zap.Any("collection", mt.tenantDatabaseCollectionCache[tenantID][databaseName][collection.ID]))

	triggerMessage := notification.TriggerMessage{
		Msg: model.Notification{
			CollectionID: collection.ID.String(),
			Type:         model.NotificationTypeCreateCollection,
			Status:       model.NotificationStatusPending,
		},
		ResultChan: make(chan error),
	}
	mt.notificationProcessor.Trigger(ctx, triggerMessage)
	return collection, nil
}

func (mt *MetaTable) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string, tenantID string, databaseName string) ([]*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	// There are three cases
	// In the case of getting by id, we do not care about the tenant and database name.
	// In the case of getting by name, we need the fully qualified path of the collection which is the tenant/database/name.
	// In the case of getting by topic, we need the fully qualified path of the collection which is the tenant/database/topic.
	collections := make([]*model.Collection, 0, len(mt.tenantDatabaseCollectionCache))
	if collectionID != types.NilUniqueID() {
		// Case 1: getting by id
		// Due to how the cache is constructed, we iterate over the whole cache to find the collection.
		// This is not efficient but it is not a problem for now because the number of collections is small.
		// HACK warning. TODO: fix this when we remove the cache.
		for _, search_databases := range mt.tenantDatabaseCollectionCache {
			for _, search_collections := range search_databases {
				for _, collection := range search_collections {
					if model.FilterCollection(collection, collectionID, collectionName, collectionTopic) {
						collections = append(collections, collection)
					}
				}
			}
		}
	} else {
		// Case 2 & 3: getting by name or topic
		// Note: The support for case 3 is not correct here, we shouldn't require the database name and tenant to get by topic.
		if _, ok := mt.tenantDatabaseCollectionCache[tenantID]; !ok {
			log.Error("tenant not found", zap.Any("tenantID", tenantID))
			return nil, common.ErrTenantNotFound
		}
		if _, ok := mt.tenantDatabaseCollectionCache[tenantID][databaseName]; !ok {
			return nil, common.ErrDatabaseNotFound
		}
		for _, collection := range mt.tenantDatabaseCollectionCache[tenantID][databaseName] {
			if model.FilterCollection(collection, collectionID, collectionName, collectionTopic) {
				collections = append(collections, collection)
			}
		}
	}
	log.Info("meta collections", zap.Any("collections", collections))
	return collections, nil

}

func (mt *MetaTable) DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	tenantID := deleteCollection.TenantID
	databaseName := deleteCollection.DatabaseName
	collectionID := deleteCollection.ID
	if _, ok := mt.tenantDatabaseCollectionCache[tenantID]; !ok {
		log.Error("tenant not found", zap.Any("tenantID", tenantID))
		return common.ErrTenantNotFound
	}
	if _, ok := mt.tenantDatabaseCollectionCache[tenantID][databaseName]; !ok {
		log.Error("database not found", zap.Any("databaseName", databaseName))
		return common.ErrDatabaseNotFound
	}
	collections := mt.tenantDatabaseCollectionCache[tenantID][databaseName]

	if _, ok := collections[collectionID]; !ok {
		log.Error("collection not found", zap.Any("collectionID", collectionID))
		return common.ErrCollectionDeleteNonExistingCollection
	}

	if err := mt.catalog.DeleteCollection(ctx, deleteCollection); err != nil {
		return err
	}
	delete(collections, collectionID)
	log.Info("collection deleted", zap.Any("collection", deleteCollection))

	triggerMessage := notification.TriggerMessage{
		Msg: model.Notification{
			CollectionID: collectionID.String(),
			Type:         model.NotificationTypeDeleteCollection,
			Status:       model.NotificationStatusPending,
		},
		ResultChan: make(chan error),
	}
	mt.notificationProcessor.Trigger(ctx, triggerMessage)
	return nil
}

func (mt *MetaTable) UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection) (*model.Collection, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	var oldCollection *model.Collection
	for tenant := range mt.tenantDatabaseCollectionCache {
		for database := range mt.tenantDatabaseCollectionCache[tenant] {
			for _, collection := range mt.tenantDatabaseCollectionCache[tenant][database] {
				if collection.ID == updateCollection.ID {
					oldCollection = collection
					break
				}
			}
		}
	}
	if oldCollection == nil {
		log.Error("collection not found", zap.Any("collectionID", updateCollection.ID))
		return nil, common.ErrCollectionNotFound
	}

	updateCollection.DatabaseName = oldCollection.DatabaseName
	updateCollection.TenantID = oldCollection.TenantID

	collection, err := mt.catalog.UpdateCollection(ctx, updateCollection, updateCollection.Ts)
	if err != nil {
		return nil, err
	}
	mt.tenantDatabaseCollectionCache[collection.TenantID][collection.DatabaseName][collection.ID] = collection
	log.Info("collection updated", zap.Any("collection", collection))
	return collection, nil
}

func (mt *MetaTable) AddSegment(ctx context.Context, createSegment *model.CreateSegment) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	segment, err := mt.catalog.CreateSegment(ctx, createSegment, createSegment.Ts)
	if err != nil {
		log.Error("create segment failed", zap.Error(err))
		var pgErr *pgconn.PgError
		ok := errors.As(err, &pgErr)
		if ok {
			log.Error("Postgres Error")
			switch pgErr.Code {
			case "23505":
				log.Error("segment id already exists")
				return common.ErrSegmentUniqueConstraintViolation
			default:
				return err
			}
		}
		return err
	}
	mt.segmentsCache[createSegment.ID] = segment
	log.Info("segment added", zap.Any("segment", segment))
	return nil
}

func (mt *MetaTable) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*model.Segment, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	segments := make([]*model.Segment, 0, len(mt.segmentsCache))
	for _, segment := range mt.segmentsCache {
		if model.FilterSegments(segment, segmentID, segmentType, scope, topic, collectionID) {
			segments = append(segments, segment)
		}
	}
	log.Info("meta get segments", zap.Any("segments", segments))
	return segments, nil
}

func (mt *MetaTable) DeleteSegment(ctx context.Context, segmentID types.UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if _, ok := mt.segmentsCache[segmentID]; !ok {
		return common.ErrSegmentDeleteNonExistingSegment
	}

	if err := mt.catalog.DeleteSegment(ctx, segmentID); err != nil {
		log.Error("delete segment failed", zap.Error(err))
		return err
	}
	delete(mt.segmentsCache, segmentID)
	log.Info("segment deleted", zap.Any("segmentID", segmentID))
	return nil
}

func (mt *MetaTable) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment) (*model.Segment, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	segment, err := mt.catalog.UpdateSegment(ctx, updateSegment, updateSegment.Ts)
	if err != nil {
		log.Error("update segment failed", zap.Error(err))
		return nil, err
	}
	mt.segmentsCache[updateSegment.ID] = segment
	log.Info("segment updated", zap.Any("segment", segment))
	return segment, nil
}
