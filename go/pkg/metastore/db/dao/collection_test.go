package dao

import (
	"testing"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"gorm.io/gorm"
)

type CollectionDbTestSuite struct {
	suite.Suite
	db           *gorm.DB
	collectionDb *collectionDb
	tenantName   string
	databaseName string
	databaseId   string
}

func (suite *CollectionDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db = dbcore.ConfigDatabaseForTesting()
	suite.collectionDb = &collectionDb{
		db: suite.db,
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
	collectionID, err := CreateTestCollection(suite.db, collectionName, 128, suite.databaseId)
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

	// Test when filtering by ID
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID, collections[0].Collection.ID)

	// Test when filtering by name
	collections, err = suite.collectionDb.GetCollections(nil, &collectionName, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID, collections[0].Collection.ID)

	// Test limit and offset
	_, err = CreateTestCollection(suite.db, "test_collection_get_collections2", 128, suite.databaseId)
	suite.NoError(err)

	allCollections, err := suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Len(allCollections, 2)

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
	suite.Nil(collections)

	// clean up
	err = CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func (suite *CollectionDbTestSuite) TestCollectionDb_UpdateLogPositionAndVersion() {
	collectionName := "test_collection_get_collections"
	collectionID, err := CreateTestCollection(suite.db, collectionName, 128, suite.databaseId)
	// verify default values
	collections, err := suite.collectionDb.GetCollections(&collectionID, nil, "", "", nil, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(int64(0), collections[0].Collection.LogPosition)
	suite.Equal(int32(0), collections[0].Collection.Version)

	// update log position and version
	version, err := suite.collectionDb.UpdateLogPositionAndVersion(collectionID, int64(10), 0)
	suite.NoError(err)
	suite.Equal(int32(1), version)
	collections, err = suite.collectionDb.GetCollections(&collectionID, nil, "", "", nil, nil)
	suite.Len(collections, 1)
	suite.Equal(int64(10), collections[0].Collection.LogPosition)
	suite.Equal(int32(1), collections[0].Collection.Version)

	// invalid log position
	_, err = suite.collectionDb.UpdateLogPositionAndVersion(collectionID, int64(5), 0)
	suite.Error(err, "collection log position Stale")

	// invalid version
	_, err = suite.collectionDb.UpdateLogPositionAndVersion(collectionID, int64(20), 0)
	suite.Error(err, "collection version invalid")
	_, err = suite.collectionDb.UpdateLogPositionAndVersion(collectionID, int64(20), 3)
	suite.Error(err, "collection version invalid")

	//clean up
	err = CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func TestCollectionDbTestSuiteSuite(t *testing.T) {
	testSuite := new(CollectionDbTestSuite)
	suite.Run(t, testSuite)
}
