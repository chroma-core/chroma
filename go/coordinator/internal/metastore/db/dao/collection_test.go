package dao

import (
	"testing"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestCollectionDb_GetCollections(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(&dbmodel.Collection{}, &dbmodel.CollectionMetadata{})
	assert.NoError(t, err)
	name := "test_name"
	topic := "test_topic"
	collection := &dbmodel.Collection{
		ID:    types.NewUniqueID().String(),
		Name:  &name,
		Topic: &topic,
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

	// Test when all parameters are nil
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
