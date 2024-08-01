package dao

import (
	"strconv"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"gorm.io/gorm"
)

type SegmentDbTestSuite struct {
	suite.Suite
	db        *gorm.DB
	segmentDb *segmentDb
}

func (suite *SegmentDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db = dbcore.ConfigDatabaseForTesting()
	suite.segmentDb = &segmentDb{
		db: suite.db,
	}
}

func (suite *SegmentDbTestSuite) TestSegmentDb_GetSegments() {
	uniqueID := types.NewUniqueID()
	collectionID := uniqueID.String()
	segment := &dbmodel.Segment{
		ID:           uniqueID.String(),
		CollectionID: &collectionID,
		Type:         "test_type",
		Scope:        "test_scope",
	}
	err := suite.db.Create(segment).Error
	suite.NoError(err)

	testKey := "test"
	testValue := "test"
	metadata := &dbmodel.SegmentMetadata{
		SegmentID: segment.ID,
		Key:       &testKey,
		StrValue:  &testValue,
	}
	err = suite.db.Create(metadata).Error
	suite.NoError(err)

	// Test when all parameters are nil
	segments, err := suite.segmentDb.GetSegments(types.NilUniqueID(), nil, nil, types.NilUniqueID())
	suite.NoError(err)
	suite.Len(segments, 1)
	suite.Equal(segment.ID, segments[0].Segment.ID)
	suite.Equal(segment.CollectionID, segments[0].Segment.CollectionID)
	suite.Equal(segment.Type, segments[0].Segment.Type)
	suite.Equal(segment.Scope, segments[0].Segment.Scope)
	suite.Equal(segment.ConfigurationJsonStr, segments[0].Segment.ConfigurationJsonStr)
	suite.Len(segments[0].SegmentMetadata, 1)
	suite.Equal(metadata.Key, segments[0].SegmentMetadata[0].Key)
	suite.Equal(metadata.StrValue, segments[0].SegmentMetadata[0].StrValue)

	// Test when filtering by ID
	segments, err = suite.segmentDb.GetSegments(types.MustParse(segment.ID), nil, nil, types.NilUniqueID())
	suite.NoError(err)
	suite.Len(segments, 1)
	suite.Equal(segment.ID, segments[0].Segment.ID)

	// Test when filtering by type
	segments, err = suite.segmentDb.GetSegments(types.NilUniqueID(), &segment.Type, nil, types.NilUniqueID())
	suite.NoError(err)
	suite.Len(segments, 1)
	suite.Equal(segment.ID, segments[0].Segment.ID)

	// Test when filtering by scope
	segments, err = suite.segmentDb.GetSegments(types.NilUniqueID(), nil, &segment.Scope, types.NilUniqueID())
	suite.NoError(err)
	suite.Len(segments, 1)
	suite.Equal(segment.ID, segments[0].Segment.ID)

	// Test when filtering by collection ID
	segments, err = suite.segmentDb.GetSegments(types.NilUniqueID(), nil, nil, types.MustParse(*segment.CollectionID))
	suite.NoError(err)
	suite.Len(segments, 1)
	suite.Equal(segment.ID, segments[0].Segment.ID)

	// clean up
	err = suite.db.Delete(segment).Error
	suite.NoError(err)
	err = suite.db.Delete(metadata).Error
	suite.NoError(err)
}

func (suite *SegmentDbTestSuite) TestSegmentDb_GetSegments_WithConfiguration() {
	uniqueID := types.NewUniqueID()
	collectionID := uniqueID.String()
	// From the perspective of the sysdb any json string is valid
	configurationJsonStr := `{"test_key": "test_value"}`

	segment := &dbmodel.Segment{
		ID:                   uniqueID.String(),
		CollectionID:         &collectionID,
		Type:                 "test_type",
		Scope:                "test_scope",
		ConfigurationJsonStr: &configurationJsonStr,
	}
	err := suite.db.Create(segment).Error
	suite.NoError(err)

	testKey := "test"
	testValue := "test"
	metadata := &dbmodel.SegmentMetadata{
		SegmentID: segment.ID,
		Key:       &testKey,
		StrValue:  &testValue,
	}
	err = suite.db.Create(metadata).Error
	suite.NoError(err)

	segments, err := suite.segmentDb.GetSegments(types.NilUniqueID(), nil, nil, types.NilUniqueID())
	suite.NoError(err)
	suite.Len(segments, 1)
	suite.Equal(segment.ID, segments[0].Segment.ID)
	suite.Equal(segment.CollectionID, segments[0].Segment.CollectionID)
	suite.Equal(segment.Type, segments[0].Segment.Type)
	suite.Equal(segment.Scope, segments[0].Segment.Scope)
	suite.Equal(segment.ConfigurationJsonStr, segments[0].Segment.ConfigurationJsonStr)
	suite.Len(segments[0].SegmentMetadata, 1)
	suite.Equal(metadata.Key, segments[0].SegmentMetadata[0].Key)
	suite.Equal(metadata.StrValue, segments[0].SegmentMetadata[0].StrValue)

	// clean up
	err = suite.db.Delete(segment).Error
	suite.NoError(err)
	err = suite.db.Delete(metadata).Error
	suite.NoError(err)
}

func (suite *SegmentDbTestSuite) TestSegmentDb_RegisterFilePath() {
	// create a collection for testing
	databaseId := types.NewUniqueID().String()
	collectionName := "test_segment_register_file_paths"
	collectionID, err := CreateTestCollection(suite.db, collectionName, 128, databaseId)
	suite.NoError(err)

	segments, err := suite.segmentDb.GetSegments(types.NilUniqueID(), nil, nil, types.MustParse(collectionID))
	suite.NoError(err)

	// create entries to flush
	segmentsFilePaths := make(map[string]map[string][]string)
	flushSegmentCompactions := make([]*model.FlushSegmentCompaction, 0)
	testFilePathTypes := []string{"TypeA", "TypeB", "TypeC", "TypeD"}
	for _, segment := range segments {
		segmentID := segment.Segment.ID
		segmentsFilePaths[segmentID] = make(map[string][]string)
		for i := 0; i < rand.Intn(len(testFilePathTypes)); i++ {
			filePaths := make([]string, 0)
			for j := 0; j < rand.Intn(5); j++ {
				filePaths = append(filePaths, "test_file_path_"+strconv.Itoa(j+1))
			}
			filePathTypeI := rand.Intn(len(testFilePathTypes))
			filePathType := testFilePathTypes[filePathTypeI]
			segmentsFilePaths[segmentID][filePathType] = filePaths
		}
		flushSegmentCompaction := &model.FlushSegmentCompaction{
			ID:        types.MustParse(segmentID),
			FilePaths: segmentsFilePaths[segmentID],
		}
		flushSegmentCompactions = append(flushSegmentCompactions, flushSegmentCompaction)
	}

	// flush the entries
	err = suite.segmentDb.RegisterFilePaths(flushSegmentCompactions)
	suite.NoError(err)

	// verify file paths registered
	segments, err = suite.segmentDb.GetSegments(types.NilUniqueID(), nil, nil, types.MustParse(collectionID))
	suite.NoError(err)
	for _, segment := range segments {
		suite.Contains(segmentsFilePaths, segment.Segment.ID)
		suite.Equal(segmentsFilePaths[segment.Segment.ID], segment.Segment.FilePaths)
	}

	// clean up
	err = CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func TestSegmentDbTestSuiteSuite(t *testing.T) {
	testSuite := new(SegmentDbTestSuite)
	suite.Run(t, testSuite)
}
