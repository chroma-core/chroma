package dao

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
)

func TestDatabaseMetadataInsertAndGet(t *testing.T) {
	db, _ := dbcore.ConfigDatabaseForTesting()
	databaseMetadata := &databaseMetadataDb{db: db}
	tenantDb := &tenantDb{db: db}
	databaseDb := &databaseDb{db: db}

	tenantID := "test_db_metadata_tenant"
	require.NoError(t, tenantDb.Insert(&dbmodel.Tenant{ID: tenantID}))
	defer db.Delete(&dbmodel.Tenant{}, "id = ?", tenantID)

	dbID := types.NewUniqueID().String()
	require.NoError(t, databaseDb.Insert(&dbmodel.Database{
		ID:       dbID,
		Name:     "test_db_metadata_database",
		TenantID: tenantID,
	}))
	defer db.Unscoped().Delete(&dbmodel.Database{}, "id = ?", dbID)

	strKey := "string_key"
	strValue := "string_value"
	intKey := "int_key"
	intValue := int64(42)
	floatKey := "float_key"
	floatValue := 3.14
	boolKey := "bool_key"
	boolValue := true

	metadata := []*dbmodel.DatabaseMetadata{
		{DatabaseID: dbID, Key: &strKey, StrValue: &strValue},
		{DatabaseID: dbID, Key: &intKey, IntValue: &intValue},
		{DatabaseID: dbID, Key: &floatKey, FloatValue: &floatValue},
		{DatabaseID: dbID, Key: &boolKey, BoolValue: &boolValue},
	}

	err := databaseMetadata.Insert(metadata)
	require.NoError(t, err)

	result, err := databaseMetadata.GetByDatabaseID(dbID)
	require.NoError(t, err)
	require.Len(t, result, 4)

	metaMap := make(map[string]*dbmodel.DatabaseMetadata)
	for _, m := range result {
		metaMap[*m.Key] = m
	}

	require.NotNil(t, metaMap[strKey])
	require.Equal(t, strValue, *metaMap[strKey].StrValue)

	require.NotNil(t, metaMap[intKey])
	require.Equal(t, intValue, *metaMap[intKey].IntValue)

	require.NotNil(t, metaMap[floatKey])
	require.Equal(t, floatValue, *metaMap[floatKey].FloatValue)

	require.NotNil(t, metaMap[boolKey])
	require.Equal(t, boolValue, *metaMap[boolKey].BoolValue)
}

func TestDatabaseMetadataGetByDatabaseIDs(t *testing.T) {
	db, _ := dbcore.ConfigDatabaseForTesting()
	databaseMetadata := &databaseMetadataDb{db: db}
	tenantDb := &tenantDb{db: db}
	databaseDb := &databaseDb{db: db}

	tenantID := "test_db_metadata_ids_tenant"
	require.NoError(t, tenantDb.Insert(&dbmodel.Tenant{ID: tenantID}))
	defer db.Delete(&dbmodel.Tenant{}, "id = ?", tenantID)

	dbID1 := types.NewUniqueID().String()
	dbID2 := types.NewUniqueID().String()
	require.NoError(t, databaseDb.Insert(&dbmodel.Database{
		ID:       dbID1,
		Name:     "test_db_metadata_ids_database1",
		TenantID: tenantID,
	}))
	require.NoError(t, databaseDb.Insert(&dbmodel.Database{
		ID:       dbID2,
		Name:     "test_db_metadata_ids_database2",
		TenantID: tenantID,
	}))
	defer db.Unscoped().Delete(&dbmodel.Database{}, "id IN ?", []string{dbID1, dbID2})

	key1 := "key1"
	value1 := "value1"
	key2 := "key2"
	value2 := "value2"

	err := databaseMetadata.Insert([]*dbmodel.DatabaseMetadata{
		{DatabaseID: dbID1, Key: &key1, StrValue: &value1},
		{DatabaseID: dbID2, Key: &key2, StrValue: &value2},
	})
	require.NoError(t, err)

	result, err := databaseMetadata.GetByDatabaseIDs([]string{dbID1, dbID2})
	require.NoError(t, err)
	require.Len(t, result, 2)

	foundDB1 := false
	foundDB2 := false
	for _, m := range result {
		if m.DatabaseID == dbID1 && *m.Key == key1 && *m.StrValue == value1 {
			foundDB1 = true
		}
		if m.DatabaseID == dbID2 && *m.Key == key2 && *m.StrValue == value2 {
			foundDB2 = true
		}
	}
	require.True(t, foundDB1, "expected to find metadata for database 1")
	require.True(t, foundDB2, "expected to find metadata for database 2")
}

func TestDatabaseMetadataDeleteByDatabaseID(t *testing.T) {
	db, _ := dbcore.ConfigDatabaseForTesting()
	databaseMetadata := &databaseMetadataDb{db: db}
	tenantDb := &tenantDb{db: db}
	databaseDb := &databaseDb{db: db}

	tenantID := "test_db_metadata_delete_tenant"
	require.NoError(t, tenantDb.Insert(&dbmodel.Tenant{ID: tenantID}))
	defer db.Delete(&dbmodel.Tenant{}, "id = ?", tenantID)

	dbID := types.NewUniqueID().String()
	require.NoError(t, databaseDb.Insert(&dbmodel.Database{
		ID:       dbID,
		Name:     "test_db_metadata_delete_database",
		TenantID: tenantID,
	}))
	defer db.Unscoped().Delete(&dbmodel.Database{}, "id = ?", dbID)

	key := "key_to_delete"
	value := "value"
	err := databaseMetadata.Insert([]*dbmodel.DatabaseMetadata{
		{DatabaseID: dbID, Key: &key, StrValue: &value},
	})
	require.NoError(t, err)

	result, err := databaseMetadata.GetByDatabaseID(dbID)
	require.NoError(t, err)
	require.Len(t, result, 1)

	err = databaseMetadata.DeleteByDatabaseID(dbID)
	require.NoError(t, err)

	result, err = databaseMetadata.GetByDatabaseID(dbID)
	require.NoError(t, err)
	require.Len(t, result, 0)
}

func TestDatabaseMetadataUpsert(t *testing.T) {
	db, _ := dbcore.ConfigDatabaseForTesting()
	databaseMetadata := &databaseMetadataDb{db: db}
	tenantDb := &tenantDb{db: db}
	databaseDb := &databaseDb{db: db}

	tenantID := "test_db_metadata_upsert_tenant"
	require.NoError(t, tenantDb.Insert(&dbmodel.Tenant{ID: tenantID}))
	defer db.Delete(&dbmodel.Tenant{}, "id = ?", tenantID)

	dbID := types.NewUniqueID().String()
	require.NoError(t, databaseDb.Insert(&dbmodel.Database{
		ID:       dbID,
		Name:     "test_db_metadata_upsert_database",
		TenantID: tenantID,
	}))
	defer db.Unscoped().Delete(&dbmodel.Database{}, "id = ?", dbID)

	key := "upsert_key"
	initialValue := "initial"
	err := databaseMetadata.Insert([]*dbmodel.DatabaseMetadata{
		{DatabaseID: dbID, Key: &key, StrValue: &initialValue},
	})
	require.NoError(t, err)

	result, err := databaseMetadata.GetByDatabaseID(dbID)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, initialValue, *result[0].StrValue)

	time.Sleep(50 * time.Millisecond)

	updatedValue := "updated"
	err = databaseMetadata.Insert([]*dbmodel.DatabaseMetadata{
		{DatabaseID: dbID, Key: &key, StrValue: &updatedValue},
	})
	require.NoError(t, err)

	result, err = databaseMetadata.GetByDatabaseID(dbID)
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, updatedValue, *result[0].StrValue)
}
