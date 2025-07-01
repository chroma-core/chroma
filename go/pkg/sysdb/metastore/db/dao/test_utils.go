package dao

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

const SegmentType = "urn:chroma:segment/vector/hnsw-distributed"

func GetSegmentScopes() []string {
	return []string{"VECTOR", "METADATA"}
}

func CreateTestTenantAndDatabase(db *gorm.DB, tenant string, database string) (string, error) {
	log.Info("create test tenant and database", zap.String("tenant", tenant), zap.String("database", database))
	tenantDb := &tenantDb{
		db: db,
	}
	databaseDb := &databaseDb{
		db: db,
	}

	err := tenantDb.Insert(&dbmodel.Tenant{
		ID:                 tenant,
		LastCompactionTime: time.Now().Unix(),
	})
	if err != nil {
		return "", err
	}

	databaseId := types.NewUniqueID().String()
	err = databaseDb.Insert(&dbmodel.Database{
		ID:       databaseId,
		Name:     database,
		TenantID: tenant,
	})
	if err != nil {
		return "", err
	}

	return databaseId, nil
}

func CreateTestDatabase(db *gorm.DB, tenant string, database string) (string, error) {
	log.Info("create test database", zap.String("tenant", tenant), zap.String("database", database))
	databaseDb := &databaseDb{
		db: db,
	}

	databaseId := types.NewUniqueID().String()
	err := databaseDb.Insert(&dbmodel.Database{
		ID:       databaseId,
		Name:     database,
		TenantID: tenant,
	})
	if err != nil {
		return "", err
	}

	return databaseId, nil
}

func CleanUpTestDatabase(db *gorm.DB, tenantName string, databaseName string) error {
	log.Info("clean up test database", zap.String("tenantName", tenantName), zap.String("databaseName", databaseName))
	// clean up collections
	collectionDb := &collectionDb{
		db: db,
	}
	collections, err := collectionDb.GetCollections(nil, nil, tenantName, databaseName, nil, nil, false)
	log.Info("clean up test database", zap.Int("collections", len(collections)))
	if err != nil {
		return err
	}
	for _, collection := range collections {
		err = CleanUpTestCollection(db, collection.Collection.ID)
		if err != nil {
			return err
		}
	}

	// clean up database
	databaseDb := &databaseDb{
		db: db,
	}

	_, err = databaseDb.DeleteByTenantIdAndName(tenantName, databaseName)
	if err != nil {
		return err
	}

	return nil
}

func CleanUpTestTenant(db *gorm.DB, tenantName string) error {
	log.Info("clean up test tenant", zap.String("tenantName", tenantName))
	tenantDb := &tenantDb{
		db: db,
	}
	databaseDb := &databaseDb{
		db: db,
	}

	// clean up databases
	databases, err := databaseDb.GetDatabasesByTenantID(tenantName)
	if err != nil {
		return err
	}
	for _, database := range databases {
		err = CleanUpTestDatabase(db, tenantName, database.Name)
		if err != nil {
			return err
		}
	}

	// clean up tenant
	_, err = tenantDb.DeleteByID(tenantName)
	if err != nil {
		return err
	}
	return nil
}

func CreateTestCollection(db *gorm.DB, collection *dbmodel.Collection) (string, error) {
	log.Info("create test collection", zap.String("collectionID", collection.ID), zap.Stringp("collectionName", collection.Name), zap.Int32p("dimension", collection.Dimension), zap.String("databaseID", collection.DatabaseID))
	collectionDb := &collectionDb{
		db: db,
	}
	segmentDb := &segmentDb{
		db: db,
	}
	if err := collectionDb.Insert(collection); err != nil {
		return "", err
	}

	for _, scope := range GetSegmentScopes() {
		segmentId := types.NewUniqueID().String()
		if err := segmentDb.Insert(&dbmodel.Segment{
			CollectionID: &collection.ID,
			ID:           segmentId,
			Type:         SegmentType,
			Scope:        scope,
		}); err != nil {
			return "", err
		}
	}
	// Avoid to have the same create time for a collection, postgres have a millisecond precision, in unit test we can have multiple collections created in the same millisecond
	// TODO(eculver): this can be removed when we replace calls to this method with collection values that have timestamps that are unique
	time.Sleep(10 * time.Millisecond)
	return collection.ID, nil
}

func CleanUpTestCollection(db *gorm.DB, collectionId string) error {
	log.Info("clean up collection", zap.String("collectionId", collectionId))
	collectionDb := &collectionDb{
		db: db,
	}
	collectionMetadataDb := &collectionMetadataDb{
		db: db,
	}
	segmentDb := &segmentDb{
		db: db,
	}
	segmentMetadataDb := &segmentMetadataDb{
		db: db,
	}

	_, err := collectionMetadataDb.DeleteByCollectionID(collectionId)
	if err != nil {
		return err
	}
	_, err = collectionDb.DeleteCollectionByID(collectionId)
	if err != nil {
		return err
	}
	segments, err := segmentDb.GetSegments(types.NilUniqueID(), nil, nil, types.MustParse(collectionId))
	if err != nil {
		return err
	}
	for _, segment := range segments {
		err = segmentDb.DeleteSegmentByID(segment.Segment.ID)
		if err != nil {
			return err
		}
		err = segmentMetadataDb.DeleteBySegmentID(segment.Segment.ID)
		if err != nil {
			return err
		}
	}

	return nil
}

func SetTestTenantResourceName(db *gorm.DB, tenantID, resourceName string) error {
	tenantDb := &tenantDb{db: db}
	return tenantDb.SetTenantResourceName(tenantID, resourceName)
}
