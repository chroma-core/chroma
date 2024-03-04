package dao

import (
	"testing"

	"github.com/chroma-core/chroma/go/internal/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/internal/types"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestSegmentDb_GetSegments(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	assert.NoError(t, err)

	err = db.AutoMigrate(&dbmodel.Segment{}, &dbmodel.SegmentMetadata{})
	assert.NoError(t, err)

	uniqueID := types.NewUniqueID()
	collectionID := uniqueID.String()
	testTopic := "test_topic"
	segment := &dbmodel.Segment{
		ID:           uniqueID.String(),
		CollectionID: &collectionID,
		Type:         "test_type",
		Scope:        "test_scope",
		Topic:        &testTopic,
	}
	err = db.Create(segment).Error
	assert.NoError(t, err)

	testKey := "test"
	testValue := "test"
	metadata := &dbmodel.SegmentMetadata{
		SegmentID: segment.ID,
		Key:       &testKey,
		StrValue:  &testValue,
	}
	err = db.Create(metadata).Error
	assert.NoError(t, err)

	segmentDb := &segmentDb{
		db: db,
	}

	// Test when all parameters are nil
	segments, err := segmentDb.GetSegments(types.NilUniqueID(), nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Len(t, segments, 1)
	assert.Equal(t, segment.ID, segments[0].Segment.ID)
	assert.Equal(t, segment.CollectionID, segments[0].Segment.CollectionID)
	assert.Equal(t, segment.Type, segments[0].Segment.Type)
	assert.Equal(t, segment.Scope, segments[0].Segment.Scope)
	assert.Equal(t, segment.Topic, segments[0].Segment.Topic)
	assert.Len(t, segments[0].SegmentMetadata, 1)
	assert.Equal(t, metadata.Key, segments[0].SegmentMetadata[0].Key)
	assert.Equal(t, metadata.StrValue, segments[0].SegmentMetadata[0].StrValue)

	// Test when filtering by ID
	segments, err = segmentDb.GetSegments(types.MustParse(segment.ID), nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Len(t, segments, 1)
	assert.Equal(t, segment.ID, segments[0].Segment.ID)

	// Test when filtering by type
	segments, err = segmentDb.GetSegments(types.NilUniqueID(), &segment.Type, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Len(t, segments, 1)
	assert.Equal(t, segment.ID, segments[0].Segment.ID)

	// Test when filtering by scope
	segments, err = segmentDb.GetSegments(types.NilUniqueID(), nil, &segment.Scope, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Len(t, segments, 1)
	assert.Equal(t, segment.ID, segments[0].Segment.ID)

	// Test when filtering by topic
	segments, err = segmentDb.GetSegments(types.NilUniqueID(), nil, nil, segment.Topic, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Len(t, segments, 1)
	assert.Equal(t, segment.ID, segments[0].Segment.ID)

	// Test when filtering by collection ID
	segments, err = segmentDb.GetSegments(types.NilUniqueID(), nil, nil, nil, types.MustParse(*segment.CollectionID))
	assert.NoError(t, err)
	assert.Len(t, segments, 1)
	assert.Equal(t, segment.ID, segments[0].Segment.ID)
}
