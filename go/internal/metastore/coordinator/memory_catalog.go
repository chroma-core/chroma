package coordinator

import (
	"context"

	"github.com/chroma-core/chroma/go/internal/common"
	"github.com/chroma-core/chroma/go/internal/metastore"
	"github.com/chroma-core/chroma/go/internal/model"
	"github.com/chroma-core/chroma/go/internal/notification"
	"github.com/chroma-core/chroma/go/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// MemoryCatalog is a reference implementation of Catalog interface to ensure the
// application logic is correctly implemented.
type MemoryCatalog struct {
	segments                  map[types.UniqueID]*model.Segment
	tenantDatabaseCollections map[string]map[string]map[types.UniqueID]*model.Collection
	tenantDatabases           map[string]map[string]*model.Database
	store                     notification.NotificationStore
}

var _ metastore.Catalog = (*MemoryCatalog)(nil)

func NewMemoryCatalog() *MemoryCatalog {
	memoryCatalog := MemoryCatalog{
		segments:                  make(map[types.UniqueID]*model.Segment),
		tenantDatabaseCollections: make(map[string]map[string]map[types.UniqueID]*model.Collection),
		tenantDatabases:           make(map[string]map[string]*model.Database),
	}
	// Add a default tenant and database
	memoryCatalog.tenantDatabases[common.DefaultTenant] = make(map[string]*model.Database)
	memoryCatalog.tenantDatabases[common.DefaultTenant][common.DefaultDatabase] = &model.Database{
		ID:     types.NilUniqueID().String(),
		Name:   common.DefaultDatabase,
		Tenant: common.DefaultTenant,
	}
	memoryCatalog.tenantDatabaseCollections[common.DefaultTenant] = make(map[string]map[types.UniqueID]*model.Collection)
	memoryCatalog.tenantDatabaseCollections[common.DefaultTenant][common.DefaultDatabase] = make(map[types.UniqueID]*model.Collection)
	return &memoryCatalog
}

func NewMemoryCatalogWithNotification(store notification.NotificationStore) *MemoryCatalog {
	memoryCatalog := NewMemoryCatalog()
	memoryCatalog.store = store
	return memoryCatalog
}

func (mc *MemoryCatalog) ResetState(ctx context.Context) error {
	mc.segments = make(map[types.UniqueID]*model.Segment)
	mc.tenantDatabases = make(map[string]map[string]*model.Database)
	mc.tenantDatabases[common.DefaultTenant] = make(map[string]*model.Database)
	mc.tenantDatabases[common.DefaultTenant][common.DefaultDatabase] = &model.Database{
		ID:     types.NilUniqueID().String(),
		Name:   common.DefaultDatabase,
		Tenant: common.DefaultTenant,
	}
	mc.tenantDatabaseCollections[common.DefaultTenant] = make(map[string]map[types.UniqueID]*model.Collection)
	mc.tenantDatabaseCollections[common.DefaultTenant][common.DefaultDatabase] = make(map[types.UniqueID]*model.Collection)
	return nil
}

func (mc *MemoryCatalog) CreateDatabase(ctx context.Context, createDatabase *model.CreateDatabase, ts types.Timestamp) (*model.Database, error) {
	tenant := createDatabase.Tenant
	databaseName := createDatabase.Name
	if _, ok := mc.tenantDatabases[tenant]; !ok {
		log.Error("tenant not found", zap.String("tenant", tenant))
		return nil, common.ErrTenantNotFound
	}
	if _, ok := mc.tenantDatabases[tenant][databaseName]; ok {
		log.Error("database already exists", zap.String("database", databaseName))
		return nil, common.ErrDatabaseUniqueConstraintViolation
	}
	mc.tenantDatabases[tenant][databaseName] = &model.Database{
		ID:     createDatabase.ID,
		Name:   createDatabase.Name,
		Tenant: createDatabase.Tenant,
	}
	mc.tenantDatabaseCollections[tenant][databaseName] = make(map[types.UniqueID]*model.Collection)
	log.Info("database created", zap.Any("database", mc.tenantDatabases[tenant][databaseName]))
	return mc.tenantDatabases[tenant][databaseName], nil
}

func (mc *MemoryCatalog) GetDatabases(ctx context.Context, getDatabase *model.GetDatabase, ts types.Timestamp) (*model.Database, error) {
	tenant := getDatabase.Tenant
	databaseName := getDatabase.Name
	if _, ok := mc.tenantDatabases[tenant]; !ok {
		log.Error("tenant not found", zap.String("tenant", tenant))
		return nil, common.ErrTenantNotFound
	}
	if _, ok := mc.tenantDatabases[tenant][databaseName]; !ok {
		log.Error("database not found", zap.String("database", databaseName))
		return nil, common.ErrDatabaseNotFound
	}
	log.Info("database found", zap.Any("database", mc.tenantDatabases[tenant][databaseName]))
	return mc.tenantDatabases[tenant][databaseName], nil
}

func (mc *MemoryCatalog) GetAllDatabases(ctx context.Context, ts types.Timestamp) ([]*model.Database, error) {
	databases := make([]*model.Database, 0)
	for _, database := range mc.tenantDatabases {
		for _, db := range database {
			databases = append(databases, db)
		}
	}
	return databases, nil
}

func (mc *MemoryCatalog) CreateTenant(ctx context.Context, createTenant *model.CreateTenant, ts types.Timestamp) (*model.Tenant, error) {
	tenant := createTenant.Name
	if _, ok := mc.tenantDatabases[tenant]; ok {
		log.Error("tenant already exists", zap.String("tenant", tenant))
		return nil, common.ErrTenantUniqueConstraintViolation
	}
	mc.tenantDatabases[tenant] = make(map[string]*model.Database)
	mc.tenantDatabaseCollections[tenant] = make(map[string]map[types.UniqueID]*model.Collection)
	return &model.Tenant{Name: tenant}, nil
}

func (mc *MemoryCatalog) GetTenants(ctx context.Context, getTenant *model.GetTenant, ts types.Timestamp) (*model.Tenant, error) {
	tenant := getTenant.Name
	if _, ok := mc.tenantDatabases[tenant]; !ok {
		log.Error("tenant not found", zap.String("tenant", tenant))
		return nil, common.ErrTenantNotFound
	}
	return &model.Tenant{Name: tenant}, nil
}

func (mc *MemoryCatalog) GetAllTenants(ctx context.Context, ts types.Timestamp) ([]*model.Tenant, error) {
	tenants := make([]*model.Tenant, 0, len(mc.tenantDatabases))
	for tenant := range mc.tenantDatabases {
		tenants = append(tenants, &model.Tenant{Name: tenant})
	}
	return tenants, nil
}

func (mc *MemoryCatalog) CreateCollection(ctx context.Context, createCollection *model.CreateCollection, ts types.Timestamp) (*model.Collection, error) {
	collectionName := createCollection.Name
	tenantID := createCollection.TenantID
	databaseName := createCollection.DatabaseName

	if _, ok := mc.tenantDatabaseCollections[tenantID]; !ok {
		log.Error("tenant not found", zap.String("tenant", tenantID))
		return nil, common.ErrTenantNotFound
	}
	if _, ok := mc.tenantDatabaseCollections[tenantID][databaseName]; !ok {
		log.Error("database not found", zap.String("database", databaseName))
		return nil, common.ErrDatabaseNotFound
	}
	// Check if the collection already by global id
	for tenant := range mc.tenantDatabaseCollections {
		for database := range mc.tenantDatabaseCollections[tenant] {
			collections := mc.tenantDatabaseCollections[tenant][database]
			if _, ok := collections[createCollection.ID]; ok {
				if tenant != tenantID || database != databaseName {
					log.Info("collection already exists", zap.Any("collection", collections[createCollection.ID]))
					return nil, common.ErrCollectionUniqueConstraintViolation
				} else if !createCollection.GetOrCreate {
					return nil, common.ErrCollectionUniqueConstraintViolation
				}
			}

		}
	}
	// Check if the collection already exists in database by colllection name
	collections := mc.tenantDatabaseCollections[tenantID][databaseName]
	for _, collection := range collections {
		if collection.Name == collectionName {
			log.Info("collection already exists", zap.Any("collection", collections[createCollection.ID]))
			if createCollection.GetOrCreate {
				if createCollection.Metadata != nil {
					// For getOrCreate, update the metadata
					collection.Metadata = createCollection.Metadata
				}
				return collection, nil
			} else {
				return nil, common.ErrCollectionUniqueConstraintViolation
			}
		}
	}
	collection := &model.Collection{
		ID:           createCollection.ID,
		Name:         createCollection.Name,
		Topic:        createCollection.Topic,
		Dimension:    createCollection.Dimension,
		Metadata:     createCollection.Metadata,
		Created:      true,
		TenantID:     createCollection.TenantID,
		DatabaseName: createCollection.DatabaseName,
	}
	log.Info("collection created", zap.Any("collection", collection))
	collections[collection.ID] = collection
	return collection, nil
}

func (mc *MemoryCatalog) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string, tenantID string, databaseName string) ([]*model.Collection, error) {
	if _, ok := mc.tenantDatabaseCollections[tenantID]; !ok {
		log.Error("tenant not found", zap.String("tenant", tenantID))
		return nil, common.ErrTenantNotFound
	}
	if _, ok := mc.tenantDatabaseCollections[tenantID][databaseName]; !ok {
		log.Error("database not found", zap.String("database", databaseName))
		return nil, common.ErrDatabaseNotFound
	}
	collections := make([]*model.Collection, 0, len(mc.tenantDatabaseCollections[tenantID][databaseName]))
	for _, collection := range mc.tenantDatabaseCollections[tenantID][databaseName] {
		if model.FilterCollection(collection, collectionID, collectionName, collectionTopic) {
			collections = append(collections, collection)
		}
	}
	return collections, nil
}

func (mc *MemoryCatalog) DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error {
	tenantID := deleteCollection.TenantID
	databaseName := deleteCollection.DatabaseName
	collectionID := deleteCollection.ID
	if _, ok := mc.tenantDatabaseCollections[tenantID]; !ok {
		log.Error("tenant not found", zap.String("tenant", tenantID))
		return common.ErrTenantNotFound
	}
	if _, ok := mc.tenantDatabaseCollections[tenantID][databaseName]; !ok {
		log.Error("database not found", zap.String("database", databaseName))
		return common.ErrDatabaseNotFound
	}
	collections := mc.tenantDatabaseCollections[tenantID][databaseName]
	if _, ok := collections[collectionID]; !ok {
		log.Error("collection not found", zap.String("collection", collectionID.String()))
		return common.ErrCollectionDeleteNonExistingCollection
	}
	delete(collections, collectionID)
	log.Info("collection deleted", zap.String("collection", collectionID.String()))
	mc.store.AddNotification(ctx, model.Notification{
		CollectionID: collectionID.String(),
		Type:         model.NotificationTypeDeleteCollection,
		Status:       model.NotificationStatusPending,
	})
	return nil
}

func (mc *MemoryCatalog) UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection, ts types.Timestamp) (*model.Collection, error) {
	collectionID := updateCollection.ID
	var oldCollection *model.Collection
	for tenant := range mc.tenantDatabaseCollections {
		for database := range mc.tenantDatabaseCollections[tenant] {
			log.Info("database", zap.Any("database", database))
			collections := mc.tenantDatabaseCollections[tenant][database]
			if _, ok := collections[collectionID]; ok {
				oldCollection = collections[collectionID]
			}
		}
	}

	topic := updateCollection.Topic
	if topic != nil {
		oldCollection.Topic = *topic
	}
	name := updateCollection.Name
	if name != nil {
		oldCollection.Name = *name
	}
	if updateCollection.Dimension != nil {
		oldCollection.Dimension = updateCollection.Dimension
	}

	// Case 1: if resetMetadata is true, then delete all metadata for the collection
	// Case 2: if resetMetadata is true and metadata is not nil -> THIS SHOULD NEVER HAPPEN
	// Case 3: if resetMetadata is false, and the metadata is not nil - set the metadata to the value in metadata
	// Case 4: if resetMetadata is false and metadata is nil, then leave the metadata as is
	resetMetadata := updateCollection.ResetMetadata
	if resetMetadata {
		oldCollection.Metadata = nil
	} else {
		if updateCollection.Metadata != nil {
			oldCollection.Metadata = updateCollection.Metadata
		}
	}
	tenantID := oldCollection.TenantID
	databaseName := oldCollection.DatabaseName
	mc.tenantDatabaseCollections[tenantID][databaseName][oldCollection.ID] = oldCollection
	// Better to return a copy of the collection to avoid being modified by others.
	log.Debug("collection metadata", zap.Any("metadata", oldCollection.Metadata))
	return oldCollection, nil
}

func (mc *MemoryCatalog) CreateSegment(ctx context.Context, createSegment *model.CreateSegment, ts types.Timestamp) (*model.Segment, error) {
	if _, ok := mc.segments[createSegment.ID]; ok {
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
	mc.segments[createSegment.ID] = segment
	log.Debug("segment created", zap.Any("segment", segment))
	return segment, nil
}

func (mc *MemoryCatalog) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID, ts types.Timestamp) ([]*model.Segment, error) {
	segments := make([]*model.Segment, 0, len(mc.segments))
	for _, segment := range mc.segments {
		if model.FilterSegments(segment, segmentID, segmentType, scope, topic, collectionID) {
			segments = append(segments, segment)
		}
	}
	return segments, nil
}

func (mc *MemoryCatalog) DeleteSegment(ctx context.Context, segmentID types.UniqueID) error {
	if _, ok := mc.segments[segmentID]; !ok {
		return common.ErrSegmentDeleteNonExistingSegment
	}

	delete(mc.segments, segmentID)
	return nil
}

func (mc *MemoryCatalog) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment, ts types.Timestamp) (*model.Segment, error) {
	// Case 1: if ResetTopic is true and topic is nil, then set the topic to nil
	// Case 2: if ResetTopic is true and topic is not nil -> THIS SHOULD NEVER HAPPEN
	// Case 3: if ResetTopic is false and topic is not nil - set the topic to the value in topic
	// Case 4: if ResetTopic is false and topic is nil, then leave the topic as is
	oldSegment := mc.segments[updateSegment.ID]
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
					oldSegment.Metadata.Remove(key)
				} else {
					oldSegment.Metadata.Set(key, value)
				}
			}
		}
	}
	mc.segments[updateSegment.ID] = oldSegment
	return oldSegment, nil
}
