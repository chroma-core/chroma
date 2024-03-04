package grpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/chroma/chroma-coordinator/internal/logservice/testutils"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"github.com/chroma/chroma-coordinator/internal/proto/logservicepb"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"testing"
	"time"
)

type RecordLogServiceTestSuite struct {
	suite.Suite
	db           *gorm.DB
	s            *Server
	t            *testing.T
	collectionId types.UniqueID
}

func (suite *RecordLogServiceTestSuite) SetupSuite() {
	log.Info("setup suite")
	// setup server and db
	s, _ := New(Config{
		DBProvider: "postgres",
		DBConfig:   dbcore.GetDBConfigForTesting(),
		StartGrpc:  false,
	})
	suite.s = s
	suite.db = dbcore.GetDB(context.Background())
	suite.collectionId = types.NewUniqueID()
}

func (suite *RecordLogServiceTestSuite) SetupTest() {
	log.Info("setup test")
	testutils.SetupTest(suite.db, suite.collectionId)
}

func (suite *RecordLogServiceTestSuite) TearDownTest() {
	log.Info("teardown test")
	testutils.TearDownTest(suite.db)
}

func encodeVector(dimension int32, vector []float32, encoding coordinatorpb.ScalarEncoding) *coordinatorpb.Vector {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, vector)
	if err != nil {
		panic(err)
	}

	return &coordinatorpb.Vector{
		Dimension: dimension,
		Vector:    buf.Bytes(),
		Encoding:  encoding,
	}
}

func GetTestEmbeddingRecords(collectionId string) (recordsToSubmit []*coordinatorpb.SubmitEmbeddingRecord) {
	testVector1 := []float32{1.0, 2.0, 3.0}
	testVector2 := []float32{1.2, 2.24, 3.2}
	testVector3 := []float32{7.0, 8.0, 9.0}
	recordsToSubmit = make([]*coordinatorpb.SubmitEmbeddingRecord, 0)
	recordsToSubmit = append(recordsToSubmit, &coordinatorpb.SubmitEmbeddingRecord{
		Id:           types.NewUniqueID().String(),
		Vector:       encodeVector(10, testVector1, coordinatorpb.ScalarEncoding_FLOAT32),
		Operation:    coordinatorpb.Operation_ADD,
		CollectionId: collectionId,
	})
	recordsToSubmit = append(recordsToSubmit, &coordinatorpb.SubmitEmbeddingRecord{
		Id:           types.NewUniqueID().String(),
		Vector:       encodeVector(6, testVector2, coordinatorpb.ScalarEncoding_FLOAT32),
		Operation:    coordinatorpb.Operation_UPDATE,
		CollectionId: collectionId,
	})
	recordsToSubmit = append(recordsToSubmit, &coordinatorpb.SubmitEmbeddingRecord{
		Id:           types.NewUniqueID().String(),
		Vector:       encodeVector(10, testVector3, coordinatorpb.ScalarEncoding_FLOAT32),
		Operation:    coordinatorpb.Operation_ADD,
		CollectionId: collectionId,
	})
	return recordsToSubmit
}

func (suite *RecordLogServiceTestSuite) TestServer_PushLogs() {
	log.Info("test push logs")
	// push some records
	recordsToSubmit := GetTestEmbeddingRecords(suite.collectionId.String())
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: suite.collectionId.String(),
		Records:      recordsToSubmit,
	}
	response, err := suite.s.PushLogs(context.Background(), &pushRequest)
	assert.Nil(suite.t, err)
	assert.Equal(suite.t, int32(3), response.RecordCount)

	var recordLogs []*dbmodel.RecordLog
	suite.db.Where("collection_id = ?", types.FromUniqueID(suite.collectionId)).Find(&recordLogs)
	assert.Len(suite.t, recordLogs, 3)
	for index := range recordLogs {
		assert.Equal(suite.t, int64(index+1), recordLogs[index].ID)
		assert.Equal(suite.t, suite.collectionId.String(), *recordLogs[index].CollectionID)
		record := &coordinatorpb.SubmitEmbeddingRecord{}
		if err := proto.Unmarshal(*recordLogs[index].Record, record); err != nil {
			panic(err)
		}
		assert.Equal(suite.t, record.Id, recordsToSubmit[index].Id)
		assert.Equal(suite.t, record.Operation, recordsToSubmit[index].Operation)
		assert.Equal(suite.t, record.CollectionId, "")
		assert.Equal(suite.t, record.Metadata, recordsToSubmit[index].Metadata)
		assert.Equal(suite.t, record.Vector.Dimension, recordsToSubmit[index].Vector.Dimension)
		assert.Equal(suite.t, record.Vector.Encoding, recordsToSubmit[index].Vector.Encoding)
		assert.Equal(suite.t, record.Vector.Vector, recordsToSubmit[index].Vector.Vector)
	}
}

func (suite *RecordLogServiceTestSuite) TestServer_PullLogs() {
	// push some records
	recordsToSubmit := GetTestEmbeddingRecords(suite.collectionId.String())
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: suite.collectionId.String(),
		Records:      recordsToSubmit,
	}
	suite.s.PushLogs(context.Background(), &pushRequest)

	// pull the records
	pullRequest := logservicepb.PullLogsRequest{
		CollectionId: suite.collectionId.String(),
		StartFromId:  0,
		BatchSize:    10,
	}
	pullResponse, err := suite.s.PullLogs(context.Background(), &pullRequest)
	assert.Nil(suite.t, err)
	assert.Len(suite.t, pullResponse.Records, 3)
	for index := range pullResponse.Records {
		assert.Equal(suite.t, int64(index+1), pullResponse.Records[index].LogId)
		assert.Equal(suite.t, recordsToSubmit[index].Id, pullResponse.Records[index].Record.Id)
		assert.Equal(suite.t, recordsToSubmit[index].Operation, pullResponse.Records[index].Record.Operation)
		assert.Equal(suite.t, recordsToSubmit[index].CollectionId, pullResponse.Records[index].Record.CollectionId)
		assert.Equal(suite.t, recordsToSubmit[index].Metadata, pullResponse.Records[index].Record.Metadata)
		assert.Equal(suite.t, recordsToSubmit[index].Vector.Dimension, pullResponse.Records[index].Record.Vector.Dimension)
		assert.Equal(suite.t, recordsToSubmit[index].Vector.Encoding, pullResponse.Records[index].Record.Vector.Encoding)
		assert.Equal(suite.t, recordsToSubmit[index].Vector.Vector, pullResponse.Records[index].Record.Vector.Vector)
	}
}

func (suite *RecordLogServiceTestSuite) TestServer_Bad_CollectionId() {
	log.Info("test bad collectionId")
	// push some records
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: "badId",
		Records:      []*coordinatorpb.SubmitEmbeddingRecord{},
	}
	pushResponse, err := suite.s.PushLogs(context.Background(), &pushRequest)
	assert.Nil(suite.t, pushResponse)
	assert.NotNil(suite.t, err)
	st, ok := status.FromError(err)
	assert.True(suite.t, ok)
	assert.Equal(suite.T(), codes.InvalidArgument, st.Code())
	assert.Equal(suite.T(), "invalid collection_id", st.Message())

	// pull the records
	// pull the records
	pullRequest := logservicepb.PullLogsRequest{
		CollectionId: "badId",
		StartFromId:  0,
		BatchSize:    10,
	}
	pullResponse, err := suite.s.PullLogs(context.Background(), &pullRequest)
	assert.Nil(suite.t, pullResponse)
	assert.NotNil(suite.t, err)
	st, ok = status.FromError(err)
	assert.True(suite.t, ok)
	assert.Equal(suite.T(), codes.InvalidArgument, st.Code())
	assert.Equal(suite.T(), "invalid collection_id", st.Message())
}

func (suite *RecordLogServiceTestSuite) TestServer_GetAllCollectionInfoToCompact() {
	// push some records
	var startTime = time.Now().UnixNano()
	recordsToSubmit := GetTestEmbeddingRecords(suite.collectionId.String())
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: suite.collectionId.String(),
		Records:      recordsToSubmit,
	}
	suite.s.PushLogs(context.Background(), &pushRequest)

	// get collection info for compactor
	request := logservicepb.GetAllCollectionInfoToCompactRequest{}
	response, err := suite.s.GetAllCollectionInfoToCompact(context.Background(), &request)
	assert.Nil(suite.t, err)
	assert.Len(suite.t, response.AllCollectionInfo, 1)
	assert.Equal(suite.T(), suite.collectionId.String(), response.AllCollectionInfo[0].CollectionId)
	assert.Equal(suite.T(), int64(1), response.AllCollectionInfo[0].FirstLogId)
	assert.True(suite.T(), response.AllCollectionInfo[0].FirstLogIdTs > startTime)
	assert.True(suite.T(), response.AllCollectionInfo[0].FirstLogIdTs < time.Now().UnixNano())

	// move log position
	testutils.MoveLogPosition(suite.db, suite.collectionId, 2)

	// get collection info for compactor
	request = logservicepb.GetAllCollectionInfoToCompactRequest{}
	response, err = suite.s.GetAllCollectionInfoToCompact(context.Background(), &request)
	assert.Nil(suite.t, err)
	assert.Len(suite.t, response.AllCollectionInfo, 1)
	assert.Equal(suite.T(), suite.collectionId.String(), response.AllCollectionInfo[0].CollectionId)
	assert.Equal(suite.T(), int64(3), response.AllCollectionInfo[0].FirstLogId)
	assert.True(suite.T(), response.AllCollectionInfo[0].FirstLogIdTs > startTime)
	assert.True(suite.T(), response.AllCollectionInfo[0].FirstLogIdTs < time.Now().UnixNano())

}

func TestRecordLogServiceTestSuite(t *testing.T) {
	testSuite := new(RecordLogServiceTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
