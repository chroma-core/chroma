package dao

import (
	"fmt"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao/daotest"
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
	collectionID, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, dim, suite.databaseId, nil))
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
	collections, err := suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, nil)
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
	collections, err = suite.collectionDb.GetCollections([]string{collectionID}, nil, "", "", nil, nil, false, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID, collections[0].Collection.ID)

	// Test when filtering by name
	collections, err = suite.collectionDb.GetCollections(nil, &collectionName, suite.tenantName, suite.databaseName, nil, nil, false, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(collectionID, collections[0].Collection.ID)

	collectionID2, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection("test_collection_get_collections2", 128, suite.databaseId, nil))
	suite.NoError(err)

	// Test order by. Collections are ordered by create time so collectionID2 should be second
	allCollections, err := suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, nil)
	suite.NoError(err)
	suite.Len(allCollections, 2)
	suite.Equal(collectionID, allCollections[0].Collection.ID)
	suite.Equal(collectionID2, allCollections[1].Collection.ID)

	// Test limit and offset
	limit := int32(1)
	offset := int32(1)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, &limit, nil, false, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(allCollections[0].Collection.ID, collections[0].Collection.ID)

	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, &limit, &offset, false, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(allCollections[1].Collection.ID, collections[0].Collection.ID)

	offset = int32(2)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, &limit, &offset, false, nil)
	suite.NoError(err)
	suite.Equal(len(collections), 0)

	// Create another database for the same tenant.
	databaseName := "test_collection_database_2"
	DbId, err := CreateTestDatabase(suite.db, suite.tenantName, databaseName)
	suite.NoError(err)

	// Create two collections in the new database.
	collectionID3, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection("test_collection_get_collections3", 128, DbId, nil))
	suite.NoError(err)

	collectionID4, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection("test_collection_get_collections4", 128, DbId, nil))
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
	collectionID, _ := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, 128, suite.databaseId, nil))
	ids := []string{collectionID}
	// verify default values
	collections, err := suite.collectionDb.GetCollections(ids, nil, "", "", nil, nil, false, nil)
	suite.NoError(err)
	suite.Len(collections, 1)
	suite.Equal(int64(0), collections[0].Collection.LogPosition)
	suite.Equal(int32(0), collections[0].Collection.Version)

	// update log position and version
	version, err := suite.collectionDb.UpdateLogPositionVersionTotalRecordsAndLogicalSize(collectionID, int64(10), 0, uint64(100), uint64(1000), uint64(10), "test_tenant2")
	suite.NoError(err)
	suite.Equal(int32(1), version)
	collections, _ = suite.collectionDb.GetCollections(ids, nil, "", "", nil, nil, false, nil)
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
	collections, err := suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, nil)
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
	collectionID1, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName1, 128, suite.databaseId, nil))
	suite.NoError(err)
	collectionID2, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName2, 128, suite.databaseId, nil))
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
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, nil)
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
	collectionID, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, 128, suite.databaseId, nil))
	suite.NoError(err)

	total_records_post_compaction, err := suite.collectionDb.GetCollectionSize(collectionID)
	suite.NoError(err)
	suite.Equal(uint64(100), total_records_post_compaction)

	err = CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func (suite *CollectionDbTestSuite) TestCollectionDb_GetCollectionByResourceName() {
	tenantResourceName := "test_tenant_resource_name"
	tenantID := "test_tenant_id"

	tenantDb := &tenantDb{
		db: suite.db,
	}
	// Create tenant first
	err := tenantDb.Insert(&dbmodel.Tenant{
		ID: tenantID,
	})
	suite.NoError(err)

	// Set tenant resource name
	err = tenantDb.SetTenantResourceName(tenantID, tenantResourceName)
	suite.NoError(err)

	databaseName := "test_database"
	databaseID, err := CreateTestDatabase(suite.db, tenantID, databaseName)
	suite.NoError(err)

	collectionName := "test_collection"
	dim := int32(128)
	collectionID, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, dim, databaseID, nil))
	suite.NoError(err)

	collectionResult, err := suite.collectionDb.GetCollectionByResourceName(tenantResourceName, databaseName, collectionName)
	suite.NoError(err)
	suite.NotNil(collectionResult)
	suite.Equal(collectionID, collectionResult.Collection.ID)
	suite.Equal(collectionName, *collectionResult.Collection.Name)
	suite.Equal(databaseID, collectionResult.Collection.DatabaseID)
	suite.Equal(tenantID, collectionResult.TenantID)
	suite.Equal(databaseName, collectionResult.DatabaseName)

	nonExistentCollection, err := suite.collectionDb.GetCollectionByResourceName(tenantResourceName, databaseName, "non_existent_collection")
	suite.Error(err, "collection not found")
	suite.Nil(nonExistentCollection)

	nonExistentCollection, err = suite.collectionDb.GetCollectionByResourceName(tenantResourceName, "non_existent_database", collectionName)
	suite.Error(err, "collection not found")
	suite.Nil(nonExistentCollection)

	nonExistentCollection, err = suite.collectionDb.GetCollectionByResourceName("non_existent_resource_name", databaseName, collectionName)
	suite.Error(err, "collection not found")
	suite.Nil(nonExistentCollection)

	err = CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
	err = CleanUpTestDatabase(suite.db, tenantID, databaseName)
	suite.NoError(err)
	err = suite.db.Delete(&dbmodel.Tenant{}, "id = ?", tenantID).Error
	suite.NoError(err)
}

func (suite *CollectionDbTestSuite) TestCollectionDb_WhereFiltering() {
	// Test various where clause filtering scenarios
	collectionName1 := "test_collection_where_filtering_1"
	collectionName2 := "test_collection_where_filtering_2"
	collectionName3 := "test_collection_where_filtering_3"

	// Create test collections
	collectionID1, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName1, 128, suite.databaseId, nil))
	suite.NoError(err)
	collectionID2, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName2, 128, suite.databaseId, nil))
	suite.NoError(err)
	collectionID3, err := CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName3, 128, suite.databaseId, nil))
	suite.NoError(err)

	// Create different types of metadata for testing
	metadata1 := []*dbmodel.CollectionMetadata{
		{CollectionID: collectionID1, Key: stringPtr("string_key"), StrValue: stringPtr("test_value")},
		{CollectionID: collectionID1, Key: stringPtr("int_key"), IntValue: int64Ptr(42)},
		{CollectionID: collectionID1, Key: stringPtr("float_key"), FloatValue: float64Ptr(3.14)},
		{CollectionID: collectionID1, Key: stringPtr("bool_key"), BoolValue: boolPtr(true)},
	}

	metadata2 := []*dbmodel.CollectionMetadata{
		{CollectionID: collectionID2, Key: stringPtr("string_key"), StrValue: stringPtr("different_value")},
		{CollectionID: collectionID2, Key: stringPtr("int_key"), IntValue: int64Ptr(100)},
		{CollectionID: collectionID2, Key: stringPtr("float_key"), FloatValue: float64Ptr(2.71)},
		{CollectionID: collectionID2, Key: stringPtr("bool_key"), BoolValue: boolPtr(false)},
	}

	metadata3 := []*dbmodel.CollectionMetadata{
		{CollectionID: collectionID3, Key: stringPtr("string_key"), StrValue: stringPtr("test_value")},
		{CollectionID: collectionID3, Key: stringPtr("int_key"), IntValue: int64Ptr(50)},
		{CollectionID: collectionID3, Key: stringPtr("float_key"), FloatValue: float64Ptr(1.41)},
		{CollectionID: collectionID3, Key: stringPtr("bool_key"), BoolValue: boolPtr(true)},
	}

	// Insert all metadata
	for _, md := range metadata1 {
		err = suite.db.Create(md).Error
		suite.NoError(err)
	}
	for _, md := range metadata2 {
		err = suite.db.Create(md).Error
		suite.NoError(err)
	}
	for _, md := range metadata3 {
		err = suite.db.Create(md).Error
		suite.NoError(err)
	}

	// Test 1: String equality filter
	whereClause := createStringEqualityWhere("string_key", "test_value")
	collections, err := suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 2) // collection1 and collection3 should match
	collectionIds := []string{collections[0].Collection.ID, collections[1].Collection.ID}
	suite.Contains(collectionIds, collectionID1)
	suite.Contains(collectionIds, collectionID3)

	// Test 2: String not equality filter
	whereClause = createStringNotEqualityWhere("string_key", "test_value")
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 1) // only collection2 should match
	suite.Equal(collectionID2, collections[0].Collection.ID)

	// Test 3: Integer equality filter
	whereClause = createIntEqualityWhere("int_key", 42)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 1) // only collection1 should match
	suite.Equal(collectionID1, collections[0].Collection.ID)

	// Test 4: Integer greater than filter
	whereClause = createIntGreaterThanWhere("int_key", 45)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 2) // collection2 (100) and collection3 (50) should match
	collectionIds = []string{collections[0].Collection.ID, collections[1].Collection.ID}
	suite.Contains(collectionIds, collectionID2)
	suite.Contains(collectionIds, collectionID3)

	// Test 5: Integer less than filter
	whereClause = createIntLessThanWhere("int_key", 60)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 2) // collection1 (42) and collection3 (50) should match
	collectionIds = []string{collections[0].Collection.ID, collections[1].Collection.ID}
	suite.Contains(collectionIds, collectionID1)
	suite.Contains(collectionIds, collectionID3)

	// Test 6: Float equality filter
	whereClause = createFloatEqualityWhere("float_key", 3.14)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 1) // only collection1 should match
	suite.Equal(collectionID1, collections[0].Collection.ID)

	// Test 7: Float greater than or equal filter
	whereClause = createFloatGreaterThanOrEqualWhere("float_key", 2.0)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 2) // collection1 (3.14) and collection2 (2.71) should match
	collectionIds = []string{collections[0].Collection.ID, collections[1].Collection.ID}
	suite.Contains(collectionIds, collectionID1)
	suite.Contains(collectionIds, collectionID2)

	// Test 8: Boolean equality filter
	whereClause = createBoolEqualityWhere("bool_key", true)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 2) // collection1 and collection3 should match
	collectionIds = []string{collections[0].Collection.ID, collections[1].Collection.ID}
	suite.Contains(collectionIds, collectionID1)
	suite.Contains(collectionIds, collectionID3)

	// Test 9: String list IN filter
	whereClause = createStringListInWhere("string_key", []string{"test_value", "different_value"})
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 3) // all collections should match

	// Test 10: String list NOT IN filter
	whereClause = createStringListNotInWhere("string_key", []string{"different_value"})
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 2) // collection1 and collection3 should match
	collectionIds = []string{collections[0].Collection.ID, collections[1].Collection.ID}
	suite.Contains(collectionIds, collectionID1)
	suite.Contains(collectionIds, collectionID3)

	// Test 11: Integer list IN filter
	whereClause = createIntListInWhere("int_key", []int64{42, 100})
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 2) // collection1 and collection2 should match
	collectionIds = []string{collections[0].Collection.ID, collections[1].Collection.ID}
	suite.Contains(collectionIds, collectionID1)
	suite.Contains(collectionIds, collectionID2)

	// Test 12: AND composite expression
	whereClause = createAndWhere(
		createStringEqualityWhere("string_key", "test_value"),
		createIntGreaterThanWhere("int_key", 45),
	)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 1) // only collection3 should match (string_key="test_value" AND int_key=50>45)
	suite.Equal(collectionID3, collections[0].Collection.ID)

	// Test 13: OR composite expression
	whereClause = createOrWhere(
		createIntEqualityWhere("int_key", 42),
		createFloatEqualityWhere("float_key", 2.71),
	)
	collections, err = suite.collectionDb.GetCollections(nil, nil, suite.tenantName, suite.databaseName, nil, nil, false, whereClause)
	suite.NoError(err)
	suite.Len(collections, 2) // collection1 (int_key=42) and collection2 (float_key=2.71) should match
	collectionIds = []string{collections[0].Collection.ID, collections[1].Collection.ID}
	suite.Contains(collectionIds, collectionID1)
	suite.Contains(collectionIds, collectionID2)

	// Clean up
	err = CleanUpTestCollection(suite.db, collectionID1)
	suite.NoError(err)
	err = CleanUpTestCollection(suite.db, collectionID2)
	suite.NoError(err)
	err = CleanUpTestCollection(suite.db, collectionID3)
	suite.NoError(err)
}

// Helper functions for creating where clauses

func stringPtr(s string) *string {
	return &s
}

func int64Ptr(i int64) *int64 {
	return &i
}

func float64Ptr(f float64) *float64 {
	return &f
}

func boolPtr(b bool) *bool {
	return &b
}

func createStringEqualityWhere(key, value string) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_SingleStringOperand{
					SingleStringOperand: &coordinatorpb.SingleStringComparison{
						Value:      value,
						Comparator: coordinatorpb.GenericComparator_EQ,
					},
				},
			},
		},
	}
}

func createStringNotEqualityWhere(key, value string) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_SingleStringOperand{
					SingleStringOperand: &coordinatorpb.SingleStringComparison{
						Value:      value,
						Comparator: coordinatorpb.GenericComparator_NE,
					},
				},
			},
		},
	}
}

func createIntEqualityWhere(key string, value int64) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_SingleIntOperand{
					SingleIntOperand: &coordinatorpb.SingleIntComparison{
						Value: value,
						Comparator: &coordinatorpb.SingleIntComparison_GenericComparator{
							GenericComparator: coordinatorpb.GenericComparator_EQ,
						},
					},
				},
			},
		},
	}
}

func createIntGreaterThanWhere(key string, value int64) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_SingleIntOperand{
					SingleIntOperand: &coordinatorpb.SingleIntComparison{
						Value: value,
						Comparator: &coordinatorpb.SingleIntComparison_NumberComparator{
							NumberComparator: coordinatorpb.NumberComparator_GT,
						},
					},
				},
			},
		},
	}
}

func createIntLessThanWhere(key string, value int64) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_SingleIntOperand{
					SingleIntOperand: &coordinatorpb.SingleIntComparison{
						Value: value,
						Comparator: &coordinatorpb.SingleIntComparison_NumberComparator{
							NumberComparator: coordinatorpb.NumberComparator_LT,
						},
					},
				},
			},
		},
	}
}

func createFloatEqualityWhere(key string, value float64) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_SingleDoubleOperand{
					SingleDoubleOperand: &coordinatorpb.SingleDoubleComparison{
						Value: value,
						Comparator: &coordinatorpb.SingleDoubleComparison_GenericComparator{
							GenericComparator: coordinatorpb.GenericComparator_EQ,
						},
					},
				},
			},
		},
	}
}

func createFloatGreaterThanOrEqualWhere(key string, value float64) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_SingleDoubleOperand{
					SingleDoubleOperand: &coordinatorpb.SingleDoubleComparison{
						Value: value,
						Comparator: &coordinatorpb.SingleDoubleComparison_NumberComparator{
							NumberComparator: coordinatorpb.NumberComparator_GTE,
						},
					},
				},
			},
		},
	}
}

func createBoolEqualityWhere(key string, value bool) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_SingleBoolOperand{
					SingleBoolOperand: &coordinatorpb.SingleBoolComparison{
						Value:      value,
						Comparator: coordinatorpb.GenericComparator_EQ,
					},
				},
			},
		},
	}
}

func createStringListInWhere(key string, values []string) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_StringListOperand{
					StringListOperand: &coordinatorpb.StringListComparison{
						Values:       values,
						ListOperator: coordinatorpb.ListOperator_IN,
					},
				},
			},
		},
	}
}

func createStringListNotInWhere(key string, values []string) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_StringListOperand{
					StringListOperand: &coordinatorpb.StringListComparison{
						Values:       values,
						ListOperator: coordinatorpb.ListOperator_NIN,
					},
				},
			},
		},
	}
}

func createIntListInWhere(key string, values []int64) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_DirectComparison{
			DirectComparison: &coordinatorpb.DirectComparison{
				Key: key,
				Comparison: &coordinatorpb.DirectComparison_IntListOperand{
					IntListOperand: &coordinatorpb.IntListComparison{
						Values:       values,
						ListOperator: coordinatorpb.ListOperator_IN,
					},
				},
			},
		},
	}
}

func createAndWhere(left, right *coordinatorpb.Where) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_Children{
			Children: &coordinatorpb.WhereChildren{
				Children: []*coordinatorpb.Where{left, right},
				Operator: coordinatorpb.BooleanOperator_AND,
			},
		},
	}
}

func createOrWhere(left, right *coordinatorpb.Where) *coordinatorpb.Where {
	return &coordinatorpb.Where{
		Where: &coordinatorpb.Where_Children{
			Children: &coordinatorpb.WhereChildren{
				Children: []*coordinatorpb.Where{left, right},
				Operator: coordinatorpb.BooleanOperator_OR,
			},
		},
	}
}

func TestCollectionDbTestSuiteSuite(t *testing.T) {
	testSuite := new(CollectionDbTestSuite)
	suite.Run(t, testSuite)
}
