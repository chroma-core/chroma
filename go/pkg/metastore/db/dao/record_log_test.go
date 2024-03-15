package dao

import (
	"github.com/chroma-core/chroma/go/pkg/logservice/testutils"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"testing"
)

type RecordLogDbTestSuite struct {
	suite.Suite
	db            *gorm.DB
	Db            *recordLogDb
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
	suite.records = make([][]byte, 0, 5)
	suite.records = append(suite.records, []byte("test1"), []byte("test2"),
		[]byte("test3"), []byte("test4"), []byte("test5"))
	recordLogTableExist := suite.db.Migrator().HasTable(&dbmodel.RecordLog{})
	if !recordLogTableExist {
		err := suite.db.Migrator().CreateTable(&dbmodel.RecordLog{})
		suite.NoError(err)
	}
}

func (suite *RecordLogDbTestSuite) SetupTest() {
	log.Info("setup test")
	suite.collectionId1 = types.NewUniqueID()
	suite.collectionId2 = types.NewUniqueID()
	err := testutils.CreateCollections(suite.db, suite.collectionId1, suite.collectionId2)
	suite.NoError(err)
}

func (suite *RecordLogDbTestSuite) TearDownTest() {
	log.Info("teardown test")
	err := testutils.CleanupCollections(suite.db, suite.collectionId1, suite.collectionId2)
	suite.NoError(err)
}

func (suite *RecordLogDbTestSuite) TestRecordLogDb_PushLogs() {
	// run push logs in transaction
	// id: 0,
	// records: test1, test2, test3
	count, err := suite.Db.PushLogs(suite.collectionId1, suite.records[:3])
	suite.NoError(err)
	suite.Equal(3, count)

	// verify logs are pushed
	var recordLogs []*dbmodel.RecordLog
	suite.db.Where("collection_id = ?", types.FromUniqueID(suite.collectionId1)).Find(&recordLogs)
	suite.Len(recordLogs, 3)
	for index := range recordLogs {
		suite.Equal(int64(index+1), recordLogs[index].ID)
		suite.Equal(suite.records[index], *recordLogs[index].Record)
	}

	// run push logs in transaction
	// id: 1,
	// records: test4, test5
	count, err = suite.Db.PushLogs(suite.collectionId1, suite.records[3:])
	suite.NoError(err)
	suite.Equal(2, count)

	// verify logs are pushed
	suite.db.Where("collection_id = ?", types.FromUniqueID(suite.collectionId1)).Find(&recordLogs)
	suite.Len(recordLogs, 5)
	for index := range recordLogs {
		suite.Equal(int64(index+1), recordLogs[index].ID)
		suite.Equal(suite.records[index], *recordLogs[index].Record)
	}

	// run push logs in transaction
	// id: 0,
	// records: test1, test2, test3, test4, test5
	count, err = suite.Db.PushLogs(suite.collectionId2, suite.records)
	suite.NoError(err)
	suite.Equal(5, count)

	// verify logs are pushed
	suite.db.Where("collection_id = ?", types.FromUniqueID(suite.collectionId2)).Find(&recordLogs)
	suite.Len(recordLogs, 5)
	for index := range recordLogs {
		suite.Equal(int64(index+1), recordLogs[index].ID)
		suite.Equal(suite.records[index], *recordLogs[index].Record)
	}
}

func (suite *RecordLogDbTestSuite) TestRecordLogDb_PullLogsFromID() {
	// pull empty logs
	var recordLogs []*dbmodel.RecordLog
	recordLogs, err := suite.Db.PullLogs(suite.collectionId1, 0, 3)
	suite.NoError(err)
	suite.Len(recordLogs, 0)

	// push some logs
	count, err := suite.Db.PushLogs(suite.collectionId1, suite.records[:3])
	suite.NoError(err)
	suite.Equal(3, count)
	count, err = suite.Db.PushLogs(suite.collectionId1, suite.records[3:])
	suite.NoError(err)
	suite.Equal(2, count)

	// pull logs from id 0 batch_size 3
	recordLogs, err = suite.Db.PullLogs(suite.collectionId1, 0, 3)
	suite.NoError(err)
	suite.Len(recordLogs, 3)
	for index := range recordLogs {
		suite.Equal(int64(index+1), recordLogs[index].ID)
		suite.Equal(suite.records[index], *recordLogs[index].Record)
	}

	// pull logs from id 0 batch_size 6
	recordLogs, err = suite.Db.PullLogs(suite.collectionId1, 0, 6)
	suite.NoError(err)
	suite.Len(recordLogs, 5)

	for index := range recordLogs {
		suite.Equal(int64(index+1), recordLogs[index].ID)
		suite.Equal(suite.records[index], *recordLogs[index].Record)
	}

	// pull logs from id 3 batch_size 4
	recordLogs, err = suite.Db.PullLogs(suite.collectionId1, 3, 4)
	suite.NoError(err)
	suite.Len(recordLogs, 3)
	for index := range recordLogs {
		suite.Equal(int64(index+3), recordLogs[index].ID)
		suite.Equal(suite.records[index+2], *recordLogs[index].Record)
	}
}

func (suite *RecordLogDbTestSuite) TestRecordLogDb_GetAllCollectionsToCompact() {
	// push some logs
	count, err := suite.Db.PushLogs(suite.collectionId1, suite.records)
	suite.NoError(err)
	suite.Equal(5, count)

	// get all collection ids to compact
	collectionInfos, err := suite.Db.GetAllCollectionsToCompact()
	suite.NoError(err)
	suite.Len(collectionInfos, 1)
	suite.Equal(suite.collectionId1.String(), *collectionInfos[0].CollectionID)
	suite.Equal(int64(1), collectionInfos[0].ID)

	// move log position
	testutils.MoveLogPosition(suite.db, suite.collectionId1, 2)

	// get all collection ids to compact
	collectionInfos, err = suite.Db.GetAllCollectionsToCompact()
	suite.NoError(err)
	suite.Len(collectionInfos, 1)
	suite.Equal(suite.collectionId1.String(), *collectionInfos[0].CollectionID)
	suite.Equal(int64(3), collectionInfos[0].ID)

	// push some logs
	count, err = suite.Db.PushLogs(suite.collectionId2, suite.records)
	suite.NoError(err)
	suite.Equal(5, count)

	// get all collection ids to compact
	collectionInfos, err = suite.Db.GetAllCollectionsToCompact()
	suite.NoError(err)
	suite.Len(collectionInfos, 2)
	suite.Equal(suite.collectionId1.String(), *collectionInfos[0].CollectionID)
	suite.Equal(int64(3), collectionInfos[0].ID)
	suite.Equal(suite.collectionId2.String(), *collectionInfos[1].CollectionID)
	suite.Equal(int64(1), collectionInfos[1].ID)
}

func TestRecordLogDbTestSuite(t *testing.T) {
	testSuite := new(RecordLogDbTestSuite)
	suite.Run(t, testSuite)
}
