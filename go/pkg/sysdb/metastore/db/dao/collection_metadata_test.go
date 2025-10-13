package dao

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao/daotest"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
)

func TestCollectionMetadataUpdatedAtIsRefreshedOnUpsert(t *testing.T) {
	db, _ := dbcore.ConfigDatabaseForTesting()
	collectionMetadata := &collectionMetadataDb{db: db}

	tenant := "test_collection_metadata_updated_at_tenant"
	database := "test_collection_metadata_updated_at_database"
	collectionName := "test_collection_metadata_updated_at_collection"

	databaseID, err := CreateTestTenantAndDatabase(db, tenant, database)
	require.NoError(t, err)

	defer func() {
		err := CleanUpTestTenant(db, tenant)
		require.NoError(t, err)
	}()

	collectionID, err := CreateTestCollection(db, daotest.NewDefaultTestCollection(collectionName, 128, databaseID, nil))
	require.NoError(t, err)

	key := "metadata_key"
	initialValue := "initial"
	err = collectionMetadata.Insert([]*dbmodel.CollectionMetadata{{
		CollectionID: collectionID,
		Key:          &key,
		StrValue:     &initialValue,
	}})
	require.NoError(t, err)

	var stored dbmodel.CollectionMetadata
	err = db.Where("collection_id = ? AND key = ?", collectionID, key).First(&stored).Error
	require.NoError(t, err)
	initialUpdatedAt := stored.UpdatedAt

	time.Sleep(50 * time.Millisecond)

	updatedValue := "updated"
	err = collectionMetadata.Insert([]*dbmodel.CollectionMetadata{{
		CollectionID: collectionID,
		Key:          &key,
		StrValue:     &updatedValue,
	}})
	require.NoError(t, err)

	var refreshed dbmodel.CollectionMetadata
	err = db.Where("collection_id = ? AND key = ?", collectionID, key).First(&refreshed).Error
	require.NoError(t, err)

	require.Equal(t, updatedValue, *refreshed.StrValue)
	require.True(t, refreshed.UpdatedAt.After(initialUpdatedAt), "expected updated_at (%s) to be after initial updated_at (%s)", refreshed.UpdatedAt, initialUpdatedAt)
	require.Equal(t, stored.CreatedAt, refreshed.CreatedAt)
}
