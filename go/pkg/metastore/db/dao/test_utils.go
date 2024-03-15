package dao

import (
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/gorm"
	"time"
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

func CleanUpTestDatabase(db *gorm.DB, tenantName string, databaseName string) error {
	log.Info("clean up test database", zap.String("tenantName", tenantName), zap.String("databaseName", databaseName))
	// clean up collections
	collectionDb := &collectionDb{
		db: db,
	}
	collections, err := collectionDb.GetCollections(nil, nil, nil, tenantName, databaseName)
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

func CreateTestCollection(db *gorm.DB, collectionName string, topic string, dimension int32, databaseID string) (string, error) {
	log.Info("create test collection", zap.String("collectionName", collectionName), zap.String("topic", topic), zap.Int32("dimension", dimension), zap.String("databaseID", databaseID))
	collectionDb := &collectionDb{
		db: db,
	}
	segmentDb := &segmentDb{
		db: db,
	}
	collectionId := types.NewUniqueID().String()

	err := collectionDb.Insert(&dbmodel.Collection{
		ID:         collectionId,
		Name:       &collectionName,
		Topic:      &topic,
		Dimension:  &dimension,
		DatabaseID: databaseID,
	})
	if err != nil {
		return "", err
	}

	for _, scope := range GetSegmentScopes() {
		segmentId := types.NewUniqueID().String()
		err = segmentDb.Insert(&dbmodel.Segment{
			CollectionID: &collectionId,
			ID:           segmentId,
			Type:         SegmentType,
			Scope:        scope,
		})
		if err != nil {
			return "", err
		}
	}

	return collectionId, nil
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
	segments, err := segmentDb.GetSegments(types.NilUniqueID(), nil, nil, nil, types.MustParse(collectionId))
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
