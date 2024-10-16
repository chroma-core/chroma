package coordinator

import (
	"context"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel/mocks"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	defaultTenant   = "default_tenant"
	defaultDatabase = "default_database"
)

func TestCatalog_CreateCollection(t *testing.T) {
	// create a mock transaction implementation
	mockTxImpl := &mocks.ITransaction{}

	// create a mock meta domain implementation
	mockMetaDomain := &mocks.IMetaDomain{}

	// create a new catalog instance
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain)

	// create a mock collection
	metadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	metadata.Add("test_key", &model.CollectionMetadataValueStringType{Value: "test_value"})
	collection := &model.CreateCollection{
		ID:       types.MustParse("00000000-0000-0000-0000-000000000001"),
		Name:     "test_collection",
		Metadata: metadata,
	}

	// create a mock timestamp
	ts := types.Timestamp(1234567890)

	// mock the insert collection method
	name := "test_collection"
	mockTxImpl.On("Transaction", context.Background(), mock.Anything).Return(nil)
	mockMetaDomain.On("CollectionDb", context.Background()).Return(&mocks.ICollectionDb{})
	mockMetaDomain.CollectionDb(context.Background()).(*mocks.ICollectionDb).On("Insert", &dbmodel.Collection{
		ID:   "00000000-0000-0000-0000-000000000001",
		Name: &name,
		Ts:   ts,
	}).Return(nil)

	// mock the insert collection metadata method
	testKey := "test_key"
	testValue := "test_value"
	mockMetaDomain.On("CollectionMetadataDb", context.Background()).Return(&mocks.ICollectionMetadataDb{})
	mockMetaDomain.CollectionMetadataDb(context.Background()).(*mocks.ICollectionMetadataDb).On("Insert", []*dbmodel.CollectionMetadata{
		{
			CollectionID: "00000000-0000-0000-0000-000000000001",
			Key:          &testKey,
			StrValue:     &testValue,
			Ts:           ts,
		},
	}).Return(nil)

	// call the CreateCollection method
	_, _, err := catalog.CreateCollection(context.Background(), collection, ts)

	// assert that the method returned no error
	assert.NoError(t, err)

	// assert that the mock methods were called as expected
	mockMetaDomain.AssertExpectations(t)
}

func TestCatalog_GetCollections(t *testing.T) {
	// create a mock meta domain implementation
	mockMetaDomain := &mocks.IMetaDomain{}

	// create a new catalog instance
	catalog := NewTableCatalog(nil, mockMetaDomain)

	// create a mock collection ID
	collectionID := types.MustParse("00000000-0000-0000-0000-000000000001")

	// create a mock collection name
	collectionName := "test_collection"

	// create a mock collection and metadata list
	name := "test_collection"
	testKey := "test_key"
	testValue := "test_value"
	collectionConfigurationJsonStr := "{\"a\": \"param\", \"b\": \"param2\", \"3\": true}"
	collectionAndMetadataList := []*dbmodel.CollectionAndMetadata{
		{
			Collection: &dbmodel.Collection{
				ID:                   "00000000-0000-0000-0000-000000000001",
				Name:                 &name,
				ConfigurationJsonStr: &collectionConfigurationJsonStr,
				Ts:                   types.Timestamp(1234567890),
			},
			CollectionMetadata: []*dbmodel.CollectionMetadata{
				{
					CollectionID: "00000000-0000-0000-0000-000000000001",
					Key:          &testKey,
					StrValue:     &testValue,
					Ts:           types.Timestamp(1234567890),
				},
			},
		},
	}

	// mock the get collections method
	mockMetaDomain.On("CollectionDb", context.Background()).Return(&mocks.ICollectionDb{})
	var n *int32
	mockMetaDomain.CollectionDb(context.Background()).(*mocks.ICollectionDb).On("GetCollections", types.FromUniqueID(collectionID), &collectionName, common.DefaultTenant, common.DefaultDatabase, n, n).Return(collectionAndMetadataList, nil)

	// call the GetCollections method
	collections, err := catalog.GetCollections(context.Background(), collectionID, &collectionName, defaultTenant, defaultDatabase, nil, nil)

	// assert that the method returned no error
	assert.NoError(t, err)

	// assert that the collections were returned as expected
	metadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	metadata.Add("test_key", &model.CollectionMetadataValueStringType{Value: "test_value"})
	assert.Equal(t, []*model.Collection{
		{
			ID:                   types.MustParse("00000000-0000-0000-0000-000000000001"),
			Name:                 "test_collection",
			ConfigurationJsonStr: collectionConfigurationJsonStr,
			Ts:                   types.Timestamp(1234567890),
			Metadata:             metadata,
		},
	}, collections)

	// assert that the mock methods were called as expected
	mockMetaDomain.AssertExpectations(t)
}
