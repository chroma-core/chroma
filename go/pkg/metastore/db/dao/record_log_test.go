package dao

import (
	"github.com/chroma-core/chroma/go/pkg/logservice/testutils"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"testing"
)

type RecordLogDbTestSuite struct {
	suite.Suite
	db            *gorm.DB
	Db            *recordLogDb
	t             *testing.T
	collectionId1 types.UniqueID
	collectionId2 types.UniqueID
	records       [][]byte
}

func (suite *RecordLogDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db = dbcore.ConfigDatabaseForTesting()
	suite.Db = &recordLogDb{
		db: suite.db,
	}
	suite.collectionId1 = types.NewUniqueID()
	suite.collectionId2 = types.NewUniqueID()
	suite.records = make([][]byte, 0, 5)
	suite.records = append(suite.records, []byte("test1"), []byte("test2"),
		[]byte("test3"), []byte("test4"), []byte("test5"))
}

func (suite *RecordLogDbTestSuite) SetupTest() {
	log.Info("setup test")
	testutils.SetupTest(suite.db, suite.collectionId1, suite.collectionId2)
}

func (suite *RecordLogDbTestSuite) TearDownTest() {
	log.Info("teardown test")
	testutils.TearDownTest(suite.db)
}

func (suite *RecordLogDbTestSuite) TestRecordLogDb_PushLogs() {
	// run push logs in transaction
	// id: 0,
	// records: test1, test2, test3
	count, err := suite.Db.PushLogs(suite.collectionId1, suite.records[:3])
	assert.NoError(suite.t, err)
	assert.Equal(suite.t, 3, count)

	// verify logs are pushed
	var recordLogs []*dbmodel.RecordLog
	suite.db.Where("collection_id = ?", types.FromUniqueID(suite.collectionId1)).Find(&recordLogs)
	assert.Len(suite.t, recordLogs, 3)
	for index := range recordLogs {
		assert.Equal(suite.t, int64(index+1), recordLogs[index].ID)
		assert.Equal(suite.t, suite.records[index], *recordLogs[index].Record)
	}

	// run push logs in transaction
	// id: 1,
	// records: test4, test5
	count, err = suite.Db.PushLogs(suite.collectionId1, suite.records[3:])
	assert.NoError(suite.t, err)
	assert.Equal(suite.t, 2, count)

	// verify logs are pushed
	suite.db.Where("collection_id = ?", types.FromUniqueID(suite.collectionId1)).Find(&recordLogs)
	assert.Len(suite.t, recordLogs, 5)
	for index := range recordLogs {
		assert.Equal(suite.t, int64(index+1), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(suite.t, suite.records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	// run push logs in transaction
	// id: 0,
	// records: test1, test2, test3, test4, test5
	count, err = suite.Db.PushLogs(suite.collectionId2, suite.records)
	assert.NoError(suite.t, err)
	assert.Equal(suite.t, 5, count)

	// verify logs are pushed
	suite.db.Where("collection_id = ?", types.FromUniqueID(suite.collectionId2)).Find(&recordLogs)
	assert.Len(suite.t, recordLogs, 5)
	for index := range recordLogs {
		assert.Equal(suite.t, int64(index+1), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(suite.t, suite.records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}
}

func (suite *RecordLogDbTestSuite) TestRecordLogDb_PullLogsFromID() {
	// pull empty logs
	var recordLogs []*dbmodel.RecordLog
	recordLogs, err := suite.Db.PullLogs(suite.collectionId1, 0, 3)
	assert.NoError(suite.t, err)
	assert.Len(suite.t, recordLogs, 0)

	// push some logs
	count, err := suite.Db.PushLogs(suite.collectionId1, suite.records[:3])
	assert.NoError(suite.t, err)
	assert.Equal(suite.t, 3, count)
	count, err = suite.Db.PushLogs(suite.collectionId1, suite.records[3:])
	assert.NoError(suite.t, err)
	assert.Equal(suite.t, 2, count)

	// pull logs from id 0 batch_size 3
	recordLogs, err = suite.Db.PullLogs(suite.collectionId1, 0, 3)
	assert.NoError(suite.t, err)
	assert.Len(suite.t, recordLogs, 3)
	for index := range recordLogs {
		assert.Equal(suite.t, int64(index+1), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(suite.t, suite.records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	// pull logs from id 0 batch_size 6
	recordLogs, err = suite.Db.PullLogs(suite.collectionId1, 0, 6)
	assert.NoError(suite.t, err)
	assert.Len(suite.t, recordLogs, 5)

	for index := range recordLogs {
		assert.Equal(suite.t, int64(index+1), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(suite.t, suite.records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	// pull logs from id 3 batch_size 4
	recordLogs, err = suite.Db.PullLogs(suite.collectionId1, 3, 4)
	assert.NoError(suite.t, err)
	assert.Len(suite.t, recordLogs, 3)
	for index := range recordLogs {
		assert.Equal(suite.t, int64(index+3), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(suite.t, suite.records[index+2], *recordLogs[index].Record, "record mismatch for index %d", index)
	}
}

func (suite *RecordLogDbTestSuite) TestRecordLogDb_GetAllCollectionsToCompact() {
	// push some logs
	count, err := suite.Db.PushLogs(suite.collectionId1, suite.records)
	assert.NoError(suite.t, err)
	assert.Equal(suite.t, 5, count)

	// get all collection ids to compact
	collectionInfos, err := suite.Db.GetAllCollectionsToCompact()
	assert.NoError(suite.t, err)
	assert.Len(suite.t, collectionInfos, 1)
	assert.Equal(suite.t, suite.collectionId1.String(), *collectionInfos[0].CollectionID)
	assert.Equal(suite.t, int64(1), collectionInfos[0].ID)

	// move log position
	testutils.MoveLogPosition(suite.db, suite.collectionId1, 2)

	// get all collection ids to compact
	collectionInfos, err = suite.Db.GetAllCollectionsToCompact()
	assert.NoError(suite.t, err)
	assert.Len(suite.t, collectionInfos, 1)
	assert.Equal(suite.t, suite.collectionId1.String(), *collectionInfos[0].CollectionID)
	assert.Equal(suite.t, int64(3), collectionInfos[0].ID)

	// push some logs
	count, err = suite.Db.PushLogs(suite.collectionId2, suite.records)
	assert.NoError(suite.t, err)
	assert.Equal(suite.t, 5, count)

	// get all collection ids to compact
	collectionInfos, err = suite.Db.GetAllCollectionsToCompact()
	assert.NoError(suite.t, err)
	assert.Len(suite.t, collectionInfos, 2)
	assert.Equal(suite.t, suite.collectionId1.String(), *collectionInfos[0].CollectionID)
	assert.Equal(suite.t, int64(3), collectionInfos[0].ID)
	assert.Equal(suite.t, suite.collectionId2.String(), *collectionInfos[1].CollectionID)
	assert.Equal(suite.t, int64(1), collectionInfos[1].ID)
}

func TestRecordLogDbTestSuite(t *testing.T) {
	testSuite := new(RecordLogDbTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
