package grpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/chroma-core/chroma/go/pkg/logservice/testutils"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
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
	suite.NoError(err)
	suite.Equal(int32(3), response.RecordCount)

	var recordLogs []*dbmodel.RecordLog
	suite.db.Where("collection_id = ?", types.FromUniqueID(suite.collectionId)).Find(&recordLogs)
	suite.Len(recordLogs, 3)
	for index := range recordLogs {
		suite.Equal(int64(index+1), recordLogs[index].ID)
		suite.Equal(suite.collectionId.String(), *recordLogs[index].CollectionID)
		record := &coordinatorpb.SubmitEmbeddingRecord{}
		if unmarshalErr := proto.Unmarshal(*recordLogs[index].Record, record); err != nil {
			suite.NoError(unmarshalErr)
		}
		suite.Equal(recordsToSubmit[index].Id, record.Id)
		suite.Equal(recordsToSubmit[index].Operation, record.Operation)
		suite.Equal("", record.CollectionId)
		suite.Equal(recordsToSubmit[index].Metadata, record.Metadata)
		suite.Equal(recordsToSubmit[index].Vector.Dimension, record.Vector.Dimension)
		suite.Equal(recordsToSubmit[index].Vector.Encoding, record.Vector.Encoding)
		suite.Equal(recordsToSubmit[index].Vector.Vector, record.Vector.Vector)
	}
}

func (suite *RecordLogServiceTestSuite) TestServer_PullLogs() {
	// push some records
	recordsToSubmit := GetTestEmbeddingRecords(suite.collectionId.String())
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: suite.collectionId.String(),
		Records:      recordsToSubmit,
	}
	_, err := suite.s.PushLogs(context.Background(), &pushRequest)
	suite.NoError(err)

	// pull the records
	pullRequest := logservicepb.PullLogsRequest{
		CollectionId: suite.collectionId.String(),
		StartFromId:  0,
		BatchSize:    10,
	}
	pullResponse, err := suite.s.PullLogs(context.Background(), &pullRequest)
	suite.NoError(err)
	suite.Len(pullResponse.Records, 3)
	for index := range pullResponse.Records {
		suite.Equal(int64(index+1), pullResponse.Records[index].LogId)
		suite.Equal(pullResponse.Records[index].Record.Id, recordsToSubmit[index].Id)
		suite.Equal(pullResponse.Records[index].Record.Operation, recordsToSubmit[index].Operation)
		suite.Equal(pullResponse.Records[index].Record.CollectionId, recordsToSubmit[index].CollectionId)
		suite.Equal(pullResponse.Records[index].Record.Metadata, recordsToSubmit[index].Metadata)
		suite.Equal(pullResponse.Records[index].Record.Vector.Dimension, recordsToSubmit[index].Vector.Dimension)
		suite.Equal(pullResponse.Records[index].Record.Vector.Encoding, recordsToSubmit[index].Vector.Encoding)
		suite.Equal(pullResponse.Records[index].Record.Vector.Vector, recordsToSubmit[index].Vector.Vector)
	}
}

func (suite *RecordLogServiceTestSuite) TestServer_Bad_CollectionId() {
	log.Info("test bad collectionId")
	// push some records
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: "badId",
		Records:      []*coordinatorpb.SubmitEmbeddingRecord{},
	}
	_, err := suite.s.PushLogs(context.Background(), &pushRequest)
	suite.Error(err)
	st, ok := status.FromError(err)
	suite.True(ok)
	suite.Equal(codes.InvalidArgument, st.Code())
	suite.Equal("invalid collection_id", st.Message())

	// pull the records
	// pull the records
	pullRequest := logservicepb.PullLogsRequest{
		CollectionId: "badId",
		StartFromId:  0,
		BatchSize:    10,
	}
	_, err = suite.s.PullLogs(context.Background(), &pullRequest)
	suite.Error(err)
	st, ok = status.FromError(err)
	suite.True(ok)
	suite.Equal(codes.InvalidArgument, st.Code())
	suite.Equal("invalid collection_id", st.Message())
}

func (suite *RecordLogServiceTestSuite) TestServer_GetAllCollectionInfoToCompact() {
	// push some records
	var startTime = time.Now().UnixNano()
	recordsToSubmit := GetTestEmbeddingRecords(suite.collectionId.String())
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: suite.collectionId.String(),
		Records:      recordsToSubmit,
	}
	_, err := suite.s.PushLogs(context.Background(), &pushRequest)
	suite.NoError(err)

	// get collection info for compactor
	request := logservicepb.GetAllCollectionInfoToCompactRequest{}
	response, err := suite.s.GetAllCollectionInfoToCompact(context.Background(), &request)
	suite.NoError(err)
	suite.Len(response.AllCollectionInfo, 1)
	suite.Equal(suite.collectionId.String(), response.AllCollectionInfo[0].CollectionId)
	suite.Equal(int64(1), response.AllCollectionInfo[0].FirstLogId)
	suite.True(response.AllCollectionInfo[0].FirstLogIdTs > startTime)
	suite.True(response.AllCollectionInfo[0].FirstLogIdTs < time.Now().UnixNano())

	// move log position
	testutils.MoveLogPosition(suite.db, suite.collectionId, 2)

	// get collection info for compactor
	request = logservicepb.GetAllCollectionInfoToCompactRequest{}
	response, err = suite.s.GetAllCollectionInfoToCompact(context.Background(), &request)
	suite.NoError(err)
	suite.Len(response.AllCollectionInfo, 1)
	suite.Equal(suite.collectionId.String(), response.AllCollectionInfo[0].CollectionId)
	suite.Equal(int64(3), response.AllCollectionInfo[0].FirstLogId)
	suite.True(response.AllCollectionInfo[0].FirstLogIdTs > startTime)
	suite.True(response.AllCollectionInfo[0].FirstLogIdTs < time.Now().UnixNano())
}

func TestRecordLogServiceTestSuite(t *testing.T) {
	testSuite := new(RecordLogServiceTestSuite)
	suite.Run(t, testSuite)
}
