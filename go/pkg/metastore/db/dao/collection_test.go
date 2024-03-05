package dao

import (
	"testing"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCollectionDb_GetCollections(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(&dbmodel.Tenant{}, &dbmodel.Database{}, &dbmodel.Collection{}, &dbmodel.CollectionMetadata{})
	db.Model(&dbmodel.Tenant{}).Create(&dbmodel.Tenant{
		ID: common.DefaultTenant,
	})

	databaseID := types.NilUniqueID().String()
	db.Model(&dbmodel.Database{}).Create(&dbmodel.Database{
		ID:       databaseID,
		Name:     common.DefaultDatabase,
		TenantID: common.DefaultTenant,
	})

	assert.NoError(t, err)
	name := "test_name"
	topic := "test_topic"
	collection := &dbmodel.Collection{
		ID:         types.NewUniqueID().String(),
		Name:       &name,
		Topic:      &topic,
		DatabaseID: databaseID,
	}
	err = db.Create(collection).Error
	assert.NoError(t, err)

	testKey := "test"
	testValue := "test"
	metadata := &dbmodel.CollectionMetadata{
		CollectionID: collection.ID,
		Key:          &testKey,
		StrValue:     &testValue,
	}
	err = db.Create(metadata).Error
	assert.NoError(t, err)

	collectionDb := &collectionDb{
		db: db,
	}

	query := db.Table("collections").Select("collections.id")
	rows, err := query.Rows()
	assert.NoError(t, err)
	for rows.Next() {
		var collectionID string
		err = rows.Scan(&collectionID)
		assert.NoError(t, err)
		log.Info("collectionID", zap.String("collectionID", collectionID))
	}
	collections, err := collectionDb.GetCollections(nil, nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Len(t, collections, 1)
	assert.Equal(t, collection.ID, collections[0].Collection.ID)
	assert.Equal(t, collection.Name, collections[0].Collection.Name)
	assert.Equal(t, collection.Topic, collections[0].Collection.Topic)
	assert.Len(t, collections[0].CollectionMetadata, 1)
	assert.Equal(t, metadata.Key, collections[0].CollectionMetadata[0].Key)
	assert.Equal(t, metadata.StrValue, collections[0].CollectionMetadata[0].StrValue)

	// Test when filtering by ID
	collections, err = collectionDb.GetCollections(nil, nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Len(t, collections, 1)
	assert.Equal(t, collection.ID, collections[0].Collection.ID)

	// Test when filtering by name
	collections, err = collectionDb.GetCollections(nil, collection.Name, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Len(t, collections, 1)
	assert.Equal(t, collection.ID, collections[0].Collection.ID)

	// Test when filtering by topic
	collections, err = collectionDb.GetCollections(nil, nil, collection.Topic, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Len(t, collections, 1)
	assert.Equal(t, collection.ID, collections[0].Collection.ID)
}
