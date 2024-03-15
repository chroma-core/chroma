package grpc

import (
	"testing"

	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestConvertCollectionMetadataToModel(t *testing.T) {
	// Test case 1: collectionMetadata is nil
	metadata, err := convertCollectionMetadataToModel(nil)
	assert.Nil(t, metadata)
	assert.Nil(t, err)

	// Test case 2: collectionMetadata is not nil
	collectionMetadata := &coordinatorpb.UpdateMetadata{
		Metadata: map[string]*coordinatorpb.UpdateMetadataValue{
			"key1": {
				Value: &coordinatorpb.UpdateMetadataValue_StringValue{
					StringValue: "value1",
				},
			},
			"key2": {
				Value: &coordinatorpb.UpdateMetadataValue_IntValue{
					IntValue: 123,
				},
			},
			"key3": {
				Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
					FloatValue: 3.14,
				},
			},
		},
	}
	metadata, err = convertCollectionMetadataToModel(collectionMetadata)
	assert.NotNil(t, metadata)
	assert.Nil(t, err)
	assert.Equal(t, "value1", metadata.Get("key1").(*model.CollectionMetadataValueStringType).Value)
	assert.Equal(t, int64(123), metadata.Get("key2").(*model.CollectionMetadataValueInt64Type).Value)
	assert.Equal(t, 3.14, metadata.Get("key3").(*model.CollectionMetadataValueFloat64Type).Value)
}

func TestConvertCollectionToProto(t *testing.T) {
	// Test case 1: collection is nil
	collectionpb := convertCollectionToProto(nil)
	assert.Nil(t, collectionpb)

	// Test case 2: collection is not nil
	dimention := int32(10)
	collection := &model.Collection{
		ID:        types.NewUniqueID(),
		Name:      "test_collection",
		Topic:     "test_topic",
		Dimension: &dimention,
		Metadata: &model.CollectionMetadata[model.CollectionMetadataValueType]{
			Metadata: map[string]model.CollectionMetadataValueType{
				"key1": &model.CollectionMetadataValueStringType{Value: "value1"},
				"key2": &model.CollectionMetadataValueInt64Type{Value: 123},
				"key3": &model.CollectionMetadataValueFloat64Type{Value: 3.14},
			},
		},
	}
	collectionpb = convertCollectionToProto(collection)
	assert.NotNil(t, collectionpb)
	assert.Equal(t, collection.ID.String(), collectionpb.Id)
	assert.Equal(t, collection.Name, collectionpb.Name)
	assert.Equal(t, collection.Topic, collectionpb.Topic)
	assert.Equal(t, collection.Dimension, collectionpb.Dimension)
	assert.NotNil(t, collectionpb.Metadata)
	assert.Equal(t, "value1", collectionpb.Metadata.Metadata["key1"].GetStringValue())
	assert.Equal(t, int64(123), collectionpb.Metadata.Metadata["key2"].GetIntValue())
	assert.Equal(t, 3.14, collectionpb.Metadata.Metadata["key3"].GetFloatValue())
}

func TestConvertCollectionMetadataToProto(t *testing.T) {
	// Test case 1: collectionMetadata is nil
	metadatapb := convertCollectionMetadataToProto(nil)
	assert.Nil(t, metadatapb)

	// Test case 2: collectionMetadata is not nil
	collectionMetadata := &model.CollectionMetadata[model.CollectionMetadataValueType]{
		Metadata: map[string]model.CollectionMetadataValueType{
			"key1": &model.CollectionMetadataValueStringType{Value: "value1"},
			"key2": &model.CollectionMetadataValueInt64Type{Value: 123},
			"key3": &model.CollectionMetadataValueFloat64Type{Value: 3.14},
		},
	}
	metadatapb = convertCollectionMetadataToProto(collectionMetadata)
	assert.NotNil(t, metadatapb)
	assert.Equal(t, "value1", metadatapb.Metadata["key1"].GetStringValue())
	assert.Equal(t, int64(123), metadatapb.Metadata["key2"].GetIntValue())
	assert.Equal(t, 3.14, metadatapb.Metadata["key3"].GetFloatValue())
}

func TestConvertToCreateCollectionModel(t *testing.T) {
	// Test case 1: id is not a valid UUID
	req := &coordinatorpb.CreateCollectionRequest{
		Id: "invalid_uuid",
	}
	collectionMetadata, err := convertToCreateCollectionModel(req)
	assert.Nil(t, collectionMetadata)
	assert.NotNil(t, err)

	// Test case 2: everything is valid
	testDimension := int32(10)
	req = &coordinatorpb.CreateCollectionRequest{
		Id:   "e9e9d6c8-9e1a-4c5c-9b8c-8f6f5e5d5d5d",
		Name: "test_collection",
		Metadata: &coordinatorpb.UpdateMetadata{
			Metadata: map[string]*coordinatorpb.UpdateMetadataValue{
				"key1": {
					Value: &coordinatorpb.UpdateMetadataValue_StringValue{
						StringValue: "value1",
					},
				},
				"key2": {
					Value: &coordinatorpb.UpdateMetadataValue_IntValue{
						IntValue: 123,
					},
				},
				"key3": {
					Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
						FloatValue: 3.14,
					},
				},
			},
		},
		Dimension: &testDimension,
	}
	collectionMetadata, err = convertToCreateCollectionModel(req)
	assert.NotNil(t, collectionMetadata)
	assert.Nil(t, err)
	assert.Equal(t, "e9e9d6c8-9e1a-4c5c-9b8c-8f6f5e5d5d5d", collectionMetadata.ID.String())
	assert.Equal(t, "test_collection", collectionMetadata.Name)
	assert.Equal(t, int32(10), *collectionMetadata.Dimension)
	assert.NotNil(t, collectionMetadata.Metadata)
	assert.Equal(t, "value1", collectionMetadata.Metadata.Get("key1").(*model.CollectionMetadataValueStringType).Value)
	assert.Equal(t, int64(123), collectionMetadata.Metadata.Get("key2").(*model.CollectionMetadataValueInt64Type).Value)
	assert.Equal(t, 3.14, collectionMetadata.Metadata.Get("key3").(*model.CollectionMetadataValueFloat64Type).Value)
}

func TestConvertSegmentMetadataToModel(t *testing.T) {
	// Test case 1: segmentMetadata is nil
	metadata, err := convertSegmentMetadataToModel(nil)
	assert.Nil(t, metadata)
	assert.Nil(t, err)

	// Test case 2: segmentMetadata is not nil
	segmentMetadata := &coordinatorpb.UpdateMetadata{
		Metadata: map[string]*coordinatorpb.UpdateMetadataValue{
			"key1": {
				Value: &coordinatorpb.UpdateMetadataValue_StringValue{
					StringValue: "value1",
				},
			},
			"key2": {
				Value: &coordinatorpb.UpdateMetadataValue_IntValue{
					IntValue: 123,
				},
			},
			"key3": {
				Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
					FloatValue: 3.14,
				},
			},
		},
	}
	metadata, err = convertSegmentMetadataToModel(segmentMetadata)
	assert.NotNil(t, metadata)
	assert.Nil(t, err)
	assert.Equal(t, "value1", metadata.Get("key1").(*model.SegmentMetadataValueStringType).Value)
	assert.Equal(t, int64(123), metadata.Get("key2").(*model.SegmentMetadataValueInt64Type).Value)
	assert.Equal(t, 3.14, metadata.Get("key3").(*model.SegmentMetadataValueFloat64Type).Value)
}

func TestConvertSegmentToProto(t *testing.T) {
	// Test case 1: segment is nil
	segmentpb := convertSegmentToProto(nil)
	assert.Nil(t, segmentpb)

	// Test case 2: segment is not nil
	testTopic := "test_topic"
	segment := &model.Segment{
		ID:        types.NewUniqueID(),
		Type:      "test_type",
		Scope:     "METADATA",
		Topic:     &testTopic,
		Metadata:  nil,
		FilePaths: map[string][]string{},
	}
	segmentpb = convertSegmentToProto(segment)
	assert.NotNil(t, segmentpb)
	assert.Equal(t, segment.ID.String(), segmentpb.Id)
	assert.Equal(t, segment.Type, segmentpb.Type)
	assert.Equal(t, coordinatorpb.SegmentScope_METADATA, segmentpb.Scope)
	assert.Equal(t, segment.Topic, segmentpb.Topic)
	assert.Nil(t, segmentpb.Collection)
	assert.Nil(t, segmentpb.Metadata)
}
