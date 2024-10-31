package coordinator

import (
	"sort"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestConvertCollectionMetadataToModel(t *testing.T) {
	// Test case 1: collectionMetadataList is nil
	modelCollectionMetadata := convertCollectionMetadataToModel(nil)
	assert.Nil(t, modelCollectionMetadata)

	// Test case 2: collectionMetadataList is empty
	collectionMetadataList := []*dbmodel.CollectionMetadata{}
	modelCollectionMetadata = convertCollectionMetadataToModel(collectionMetadataList)
	assert.Nil(t, modelCollectionMetadata)
}

func TestConvertCollectionMetadataToDB(t *testing.T) {
	// Test case 1: metadata is nil
	dbCollectionMetadataList := convertCollectionMetadataToDB("collectionID", nil)
	assert.Nil(t, dbCollectionMetadataList)

	// Test case 2: metadata is not nil but empty
	metadata := &model.CollectionMetadata[model.CollectionMetadataValueType]{
		Metadata: map[string]model.CollectionMetadataValueType{},
	}
	dbCollectionMetadataList = convertCollectionMetadataToDB("collectionID", metadata)
	assert.NotNil(t, dbCollectionMetadataList)
	assert.Len(t, dbCollectionMetadataList, 0)

	// Test case 3: metadata is not nil and contains values
	metadata = &model.CollectionMetadata[model.CollectionMetadataValueType]{
		Metadata: map[string]model.CollectionMetadataValueType{
			"key1": &model.CollectionMetadataValueStringType{Value: "value1"},
			"key2": &model.CollectionMetadataValueInt64Type{Value: 123},
			"key3": &model.CollectionMetadataValueFloat64Type{Value: 3.14},
		},
	}
	dbCollectionMetadataList = convertCollectionMetadataToDB("collectionID", metadata)
	sort.Slice(dbCollectionMetadataList, func(i, j int) bool {
		return *dbCollectionMetadataList[i].Key < *dbCollectionMetadataList[j].Key
	})
	assert.NotNil(t, dbCollectionMetadataList)
	assert.Len(t, dbCollectionMetadataList, 3)
	assert.Equal(t, "collectionID", dbCollectionMetadataList[0].CollectionID)
	assert.Equal(t, "key1", *dbCollectionMetadataList[0].Key)
	assert.Equal(t, "value1", *dbCollectionMetadataList[0].StrValue)
	assert.Nil(t, dbCollectionMetadataList[0].IntValue)
	assert.Nil(t, dbCollectionMetadataList[0].FloatValue)
	assert.Equal(t, "collectionID", dbCollectionMetadataList[1].CollectionID)
	assert.Equal(t, "key2", *dbCollectionMetadataList[1].Key)
	assert.Nil(t, dbCollectionMetadataList[1].StrValue)
	assert.Equal(t, int64(123), *dbCollectionMetadataList[1].IntValue)
	assert.Nil(t, dbCollectionMetadataList[1].FloatValue)
	assert.Equal(t, "collectionID", dbCollectionMetadataList[2].CollectionID)
	assert.Equal(t, "key3", *dbCollectionMetadataList[2].Key)
	assert.Nil(t, dbCollectionMetadataList[2].StrValue)
	assert.Nil(t, dbCollectionMetadataList[2].IntValue)
	assert.Equal(t, 3.14, *dbCollectionMetadataList[2].FloatValue)
}
func TestConvertSegmentToModel(t *testing.T) {
	// Test case 1: segmentAndMetadataList is nil
	modelSegments := convertSegmentToModel(nil)
	assert.Nil(t, modelSegments)

	// Test case 2: segmentAndMetadataList is empty
	segmentAndMetadataList := []*dbmodel.SegmentAndMetadata{}
	modelSegments = convertSegmentToModel(segmentAndMetadataList)
	assert.Empty(t, modelSegments)

	// Test case 3: segmentAndMetadataList contains one segment with all fields set
	segmentID := types.MustParse("515fc331-e117-4b86-bd84-85341128c337")
	collectionID := "d9a75e2e-2929-45c4-af06-75b15630edd0"
	segmentAndMetadata := &dbmodel.SegmentAndMetadata{
		Segment: &dbmodel.Segment{
			ID:           segmentID.String(),
			Type:         "segment_type",
			Scope:        "segment_scope",
			CollectionID: &collectionID,
		},
		SegmentMetadata: []*dbmodel.SegmentMetadata{},
	}
	segmentAndMetadataList = []*dbmodel.SegmentAndMetadata{segmentAndMetadata}
	modelSegments = convertSegmentToModel(segmentAndMetadataList)
	assert.Len(t, modelSegments, 1)
	assert.Equal(t, segmentID, modelSegments[0].ID)
	assert.Equal(t, "segment_type", modelSegments[0].Type)
	assert.Equal(t, "segment_scope", modelSegments[0].Scope)
	assert.Equal(t, types.MustParse(collectionID), modelSegments[0].CollectionID)
	assert.Nil(t, modelSegments[0].Metadata)
}

func TestConvertSegmentMetadataToModel(t *testing.T) {
	// Test case 1: segmentMetadataList is nil
	modelSegmentMetadata := convertSegmentMetadataToModel(nil)
	assert.Nil(t, modelSegmentMetadata)

	// Test case 2: segmentMetadataList is empty
	segmentMetadataList := []*dbmodel.SegmentMetadata{}
	modelSegmentMetadata = convertSegmentMetadataToModel(segmentMetadataList)
	assert.Empty(t, modelSegmentMetadata)

	// Test case 3: segmentMetadataList contains one segment metadata with all fields set
	segmentID := types.MustParse("515fc331-e117-4b86-bd84-85341128c337")
	strKey := "strKey"
	strValue := "strValue"
	segmentMetadata := &dbmodel.SegmentMetadata{
		SegmentID: segmentID.String(),
		Key:       &strKey,
		StrValue:  &strValue,
	}
	segmentMetadataList = []*dbmodel.SegmentMetadata{segmentMetadata}
	modelSegmentMetadata = convertSegmentMetadataToModel(segmentMetadataList)
	assert.Len(t, modelSegmentMetadata.Keys(), 1)
	assert.Equal(t, &model.SegmentMetadataValueStringType{Value: strValue}, modelSegmentMetadata.Get(strKey))
}
func TestConvertCollectionToModel(t *testing.T) {
	// Test case 1: collectionAndMetadataList is nil
	modelCollections := convertCollectionToModel(nil)
	assert.Nil(t, modelCollections)

	// Test case 2: collectionAndMetadataList is empty
	collectionAndMetadataList := []*dbmodel.CollectionAndMetadata{}
	modelCollections = convertCollectionToModel(collectionAndMetadataList)
	assert.Empty(t, modelCollections)

	// Test case 3: collectionAndMetadataList contains one collection with all fields set
	collectionID := types.MustParse("d9a75e2e-2929-45c4-af06-75b15630edd0")
	collectionName := "collection_name"
	colllectionConfigurationJsonStr := "{\"a\": \"param\", \"b\": \"param2\", \"3\": true}"
	collectionDimension := int32(3)
	collectionAndMetadata := &dbmodel.CollectionAndMetadata{
		Collection: &dbmodel.Collection{
			ID:                   collectionID.String(),
			Name:                 &collectionName,
			ConfigurationJsonStr: &colllectionConfigurationJsonStr,
			Dimension:            &collectionDimension,
		},
		CollectionMetadata: []*dbmodel.CollectionMetadata{},
	}
	collectionAndMetadataList = []*dbmodel.CollectionAndMetadata{collectionAndMetadata}
	modelCollections = convertCollectionToModel(collectionAndMetadataList)
	assert.Len(t, modelCollections, 1)
	assert.Equal(t, collectionID, modelCollections[0].ID)
	assert.Equal(t, collectionName, modelCollections[0].Name)
	assert.Equal(t, colllectionConfigurationJsonStr, modelCollections[0].ConfigurationJsonStr)
	assert.Equal(t, collectionDimension, *modelCollections[0].Dimension)
	assert.Nil(t, modelCollections[0].Metadata)
}
