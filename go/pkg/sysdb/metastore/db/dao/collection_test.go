package dao

import (
	"fmt"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"gorm.io/gorm"
)

type CollectionDbTestSuite struct {
	suite.Suite
	db           *gorm.DB
	read_db      *gorm.DB
	collectionDb *collectionDb
	tenantName   string
	databaseName string
	databaseId   string
}

func (suite *CollectionDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db, suite.read_db = dbcore.ConfigDatabaseForTesting()
	suite.collectionDb = &collectionDb{
		db:      suite.db,
		read_db: suite.read_db,
	}
	suite.tenantName = "test_collection_tenant"
	suite.databaseName = "test_collection_database"
	DbId, err := CreateTestTenantAndDatabase(suite.db, suite.tenantName, suite.databaseName)
	suite.NoError(err)
	suite.databaseId = DbId
}

func (suite *CollectionDbTestSuite) TearDownSuite() {
	log.Info("teardown suite")
	err := CleanUpTestDatabase(suite.db, suite.tenantName, suite.databaseName)
	suite.NoError(err)
	err = CleanUpTestTenant(suite.db, suite.tenantName)
	suite.NoError(err)
}

func (suite *CollectionDbTestSuite) TestCollectionDb_GetCollections() {
	collectionName := "test_collection_get_collections"
	dim := int32(128)
	collectionID, err := CreateTestCollection(suite.db, collectionName, dim, suite.databaseId, nil)
	suite.NoError(err)

	testKey := "test"
	testValue := "test"
	metadata := &dbmodel.CollectionMetadata{
		CollectionID: collectionID,
		Key:          &testKey,
		StrValue:     &testValue,
	}
	err = suite.db.Create(metadata).Error
	suite.NoError(err)

	query := suite.db.Table("collections").Select("collections.id").Where("collections.id = ?", collectionID)
	rows, err := query.Rows()
	suite.NoError(err)
	for rows.Next() {
		var scanedCollectionID string
		err = rows.Scan(&scanedCollectionID)
		suite.NoError(err)
		suite.Equal(collectionID, scanedCollectionID)
	}
	collections, err := suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID, collections[0].Collection.ID)
	suite.Equal(collectionName, *collections[0].Collection.Name)
	suite.Len(collections[0].CollectionMetadata, 1)
	suite.Equal(metadata.Key, collections[0].CollectionMetadata[0].Key)
	suite.Equal(metadata.StrValue, collections[0].CollectionMetadata[0].StrValue)
	suite.Equal(uint64(100), collections[0].Collection.TotalRecordsPostCompaction)
	suite.Equal(uint64(500000), collections[0].Collection.SizeBytesPostCompaction)
	suite.Equal(uint64(1741037006), collections[0].Collection.LastCompactionTimeSecs)
	suite.Equal(collections[0].DatabaseName, suite.databaseName)
	suite.Equal(collections[0].TenantID, suite.tenantName)
	suite.Equal(collections[0].Collection.Dimension, &dim)
	defaultConfig := "{\"a\": \"param\", \"b\": \"param2\", \"3\": true}"
	suite.Equal(collections[0].Collection.ConfigurationJsonStr, &defaultConfig)
	suite.Equal(collections[0].Collection.DatabaseID, suite.databaseId)
	suite.Equal(collections[0].Collection.LogPosition, int64(0))
	suite.Equal(collections[0].Collection.Version, int32(0))
	suite.Equal(collections[0].Collection.IsDeleted, false)

	// Test when filtering by ID
	collections, err = suite.collectionDb.GetCollections(&collectionID, nil, "", "", nil, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID, collections[0].Collection.ID)

	// Test when filtering by name
	collections, err = suite.collectionDb.GetCollections(nil, &collectionName, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID, collections[0].Collection.ID)

	collectionID2, err := CreateTestCollection(suite.db, "test_collection_get_collections2", 128, suite.databaseId, nil)
	suite.NoError(err)

	// Test order by. Collections are ordered by create time so collectionID2 should be second
	allCollections, err := suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Len(allCollections, 2)
	suite.Equal(collectionID, allCollections[0].Collection.ID)
	suite.Equal(collectionID2, allCollections[1].Collection.ID)

	// Test limit and offset
	limit := int32(1)
	offset := int32(1)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, &limit, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(allCollections[0].Collection.ID, collections[0].Collection.ID)

	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, &limit, &offset)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(allCollections[1].Collection.ID, collections[0].Collection.ID)

	offset = int32(2)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, &limit, &offset)
	suite.NoError(err)
	suite.Equal(len(collections), 0)

	// Create another database for the same tenant.
	databaseName := "test_collection_database_2"
	DbId, err := CreateTestDatabase(suite.db, suite.tenantName, databaseName)
	suite.NoError(err)

	// Create two collections in the new database.
	collectionID3, err := CreateTestCollection(suite.db, "test_collection_get_collections3", 128, DbId, nil)
	suite.NoError(err)

	collectionID4, err := CreateTestCollection(suite.db, "test_collection_get_collections4", 128, DbId, nil)
	suite.NoError(err)

	// Test count collections
	// Count collections in the first database
	count, err := suite.collectionDb.CountCollections(suite.tenantName, &suite.databaseName)
	suite.NoError(err)
	suite.Equal(uint64(2), count)

	// Count collections in the second database
	count, err = suite.collectionDb.CountCollections(suite.tenantName, &databaseName)
	suite.NoError(err)
	suite.Equal(uint64(2), count)

	// Count collections by tenant
	count, err = suite.collectionDb.CountCollections(suite.tenantName, nil)
	suite.NoError(err)
	suite.Equal(uint64(4), count)

	// clean up
	err = CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
	err = CleanUpTestCollection(suite.db, collectionID2)
	suite.NoError(err)
	err = CleanUpTestCollection(suite.db, collectionID3)
	suite.NoError(err)
	err = CleanUpTestCollection(suite.db, collectionID4)
	suite.NoError(err)
	err = CleanUpTestDatabase(suite.db, suite.tenantName, databaseName)
	suite.NoError(err)
}

func (suite *CollectionDbTestSuite) TestCollectionDb_UpdateLogPositionVersionTotalRecordsAndLogicalSize() {
	collectionName := "test_collection_get_collections"
	collectionID, _ := CreateTestCollection(suite.db, collectionName, 128, suite.databaseId, nil)
	// verify default values
	collections, err := suite.collectionDb.GetCollections(&collectionID, nil, "", "", nil, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(int64(0), collections[0].Collection.LogPosition)
	suite.Equal(int32(0), collections[0].Collection.Version)

	// update log position and version
	version, err := suite.collectionDb.UpdateLogPositionVersionTotalRecordsAndLogicalSize(collectionID, int64(10), 0, uint64(100), uint64(1000), uint64(10), "test_tenant2")
	suite.NoError(err)
	suite.Equal(int32(1), version)
	collections, _ = suite.collectionDb.GetCollections(&collectionID, nil, "", "", nil, nil)
	suite.Len(collections, 1)
	suite.Equal(int64(10), collections[0].Collection.LogPosition)
	suite.Equal(int32(1), collections[0].Collection.Version)
	suite.Equal(uint64(100), collections[0].Collection.TotalRecordsPostCompaction)
	suite.Equal(uint64(1000), collections[0].Collection.SizeBytesPostCompaction)
	suite.Equal("test_tenant2", collections[0].Collection.Tenant)
	suite.Equal(uint64(10), collections[0].Collection.LastCompactionTimeSecs)

	// invalid log position
	_, err = suite.collectionDb.UpdateLogPositionVersionTotalRecordsAndLogicalSize(collectionID, int64(5), 0, uint64(100), uint64(1000), uint64(10), "test_tenant2")
	suite.Error(err, "collection log position Stale")

	// invalid version
	_, err = suite.collectionDb.UpdateLogPositionVersionTotalRecordsAndLogicalSize(collectionID, int64(20), 0, uint64(100), uint64(1000), uint64(10), "test_tenant2")
	suite.Error(err, "collection version invalid")
	_, err = suite.collectionDb.UpdateLogPositionVersionTotalRecordsAndLogicalSize(collectionID, int64(20), 3, uint64(100), uint64(1000), uint64(10), "test_tenant2")
	suite.Error(err, "collection version invalid")

	//clean up
	err = CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func (suite *CollectionDbTestSuite) TestCollectionDb_SoftDelete() {
	// Ensure there are no collections from before.
	collections, err := suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	if len(collections) != 0 {
		suite.FailNow(fmt.Sprintf(
			"expected 0 collections, got %d. Printing name of first collection: %s", len(collections), *collections[0].Collection.Name))
	}

	// Test goal -
	// Create 2 collections. Soft delete one.
	// Check that the deleted collection does not appear in the normal get collection results.
	// Check that the deleted collection does appear in the soft deleted collection results.

	// Create 2 collections.
	collectionName1 := "test_collection_soft_delete1"
	collectionName2 := "test_collection_soft_delete2"
	collectionID1, err := CreateTestCollection(suite.db, collectionName1, 128, suite.databaseId, nil)
	suite.NoError(err)
	collectionID2, err := CreateTestCollection(suite.db, collectionName2, 128, suite.databaseId, nil)
	suite.NoError(err)

	// Soft delete collection 1 by Updating the is_deleted column
	err = suite.collectionDb.Update(&dbmodel.Collection{
		ID:         collectionID1,
		DatabaseID: suite.databaseId,
		IsDeleted:  true,
		UpdatedAt:  time.Now(),
	})
	suite.NoError(err)

	// Verify normal get collections only returns non-deleted collection
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID2, collections[0].Collection.ID)
	suite.Equal(collectionName2, *collections[0].Collection.Name)

	// Verify getting soft deleted collections
	collections, err = suite.collectionDb.GetSoftDeletedCollections(&collectionID1, "", suite.databaseName, 10)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID1, collections[0].Collection.ID)
	suite.Equal(collectionName1, *collections[0].Collection.Name)

	// Clean up
	err = CleanUpTestCollection(suite.db, collectionID1)
	suite.NoError(err)
	err = CleanUpTestCollection(suite.db, collectionID2)
	suite.NoError(err)
}

func (suite *CollectionDbTestSuite) TestCollectionDb_GetCollectionSize() {
	collectionName := "test_collection_get_collection_size"
	collectionID, err := CreateTestCollection(suite.db, collectionName, 128, suite.databaseId, nil)
	suite.NoError(err)

	total_records_post_compaction, err := suite.collectionDb.GetCollectionSize(collectionID)
	suite.NoError(err)
	suite.Equal(uint64(100), total_records_post_compaction)

	err = CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func TestCollectionDbTestSuiteSuite(t *testing.T) {
	testSuite := new(CollectionDbTestSuite)
	suite.Run(t, testSuite)
}
