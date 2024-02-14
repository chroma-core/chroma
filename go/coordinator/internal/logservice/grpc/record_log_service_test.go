package grpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"github.com/chroma/chroma-coordinator/internal/proto/logservicepb"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"testing"
)

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

func resetLogTable(db *gorm.DB) {
	db.Migrator().DropTable(&dbmodel.RecordLog{})
	db.Migrator().CreateTable(&dbmodel.RecordLog{})
}

func TestServer_PushLogs(t *testing.T) {
	// setup
	s, err := New(Config{
		DBProvider:   "aurora",
		AuroraRegion: "us-west-2",
		DBHost:       "test-instance-1.cd2rjkzioeat.us-west-2.rds.amazonaws.com",
		DBPort:       5432,
		DBUser:       "postgres",
		DBPassword:   "z7_UHv7f2_Pz9Js9BkHN",
		DBName:       "test",
		StartGrpc:    false,
	})
	if err != nil {
		t.Fatalf("error creating server: %v", err)
	}
	db := dbcore.GetDB(context.Background())
	resetLogTable(db)

	// push some records
	collectionId := types.NewUniqueID()
	recordsToSubmit := GetTestEmbeddingRecords(collectionId.String())
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: collectionId.String(),
		Records:      recordsToSubmit,
	}
	response, err := s.PushLogs(context.Background(), &pushRequest)
	assert.Nil(t, err)
	assert.Equal(t, int32(3), response.RecordCount)
	assert.Equal(t, int32(200), response.Status.Code)

	var recordLogs []*dbmodel.RecordLog
	db.Where("collection_id = ?", types.FromUniqueID(collectionId)).Find(&recordLogs)
	assert.Len(t, recordLogs, 3)
	for index := range recordLogs {
		assert.Equal(t, int64(index+1), recordLogs[index].ID)
		assert.Equal(t, collectionId.String(), *recordLogs[index].CollectionID)
		record := &coordinatorpb.SubmitEmbeddingRecord{}
		if err := proto.Unmarshal(*recordLogs[index].Record, record); err != nil {
			panic(err)
		}
		assert.Equal(t, record.Id, recordsToSubmit[index].Id)
		assert.Equal(t, record.Operation, recordsToSubmit[index].Operation)
		assert.Equal(t, record.CollectionId, "")
		assert.Equal(t, record.Metadata, recordsToSubmit[index].Metadata)
		assert.Equal(t, record.Vector.Dimension, recordsToSubmit[index].Vector.Dimension)
		assert.Equal(t, record.Vector.Encoding, recordsToSubmit[index].Vector.Encoding)
		assert.Equal(t, record.Vector.Vector, recordsToSubmit[index].Vector.Vector)
	}

	resetLogTable(db)
}

func TestServer_PullLogs(t *testing.T) {
	// setup
	s, err := New(Config{
		DBProvider:   "aurora",
		AuroraRegion: "us-west-2",
		DBHost:       "test-instance-1.cd2rjkzioeat.us-west-2.rds.amazonaws.com",
		DBPort:       5432,
		DBUser:       "postgres",
		DBPassword:   "z7_UHv7f2_Pz9Js9BkHN",
		DBName:       "test",
		StartGrpc:    false,
	})
	if err != nil {
		t.Fatalf("error creating server: %v", err)
	}
	db := dbcore.GetDB(context.Background())
	resetLogTable(db)

	// push some records
	collectionId := types.NewUniqueID()
	recordsToSubmit := GetTestEmbeddingRecords(collectionId.String())
	pushRequest := logservicepb.PushLogsRequest{
		CollectionId: collectionId.String(),
		Records:      recordsToSubmit,
	}
	s.PushLogs(context.Background(), &pushRequest)

	// pull the records
	pullRequest := logservicepb.PullLogsRequest{
		CollectionId: collectionId.String(),
		StartFromId:  0,
		BatchSize:    10,
	}
	pullResponse, err := s.PullLogs(context.Background(), &pullRequest)
	assert.Nil(t, err)
	assert.Len(t, pullResponse.Records, 3)
	assert.Equal(t, int32(200), pullResponse.Status.Code)
	for index := range pullResponse.Records {
		assert.Equal(t, recordsToSubmit[index].Id, pullResponse.Records[index].Id)
		assert.Equal(t, recordsToSubmit[index].Operation, pullResponse.Records[index].Operation)
		assert.Equal(t, recordsToSubmit[index].CollectionId, "")
		assert.Equal(t, recordsToSubmit[index].Metadata, pullResponse.Records[index].Metadata)
		assert.Equal(t, recordsToSubmit[index].Vector.Dimension, pullResponse.Records[index].Vector.Dimension)
		assert.Equal(t, recordsToSubmit[index].Vector.Encoding, pullResponse.Records[index].Vector.Encoding)
		assert.Equal(t, recordsToSubmit[index].Vector.Vector, pullResponse.Records[index].Vector.Vector)
	}

	resetLogTable(db)
}
