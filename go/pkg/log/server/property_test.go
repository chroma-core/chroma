package server

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/chroma-core/chroma/go/pkg/types"
	libs2 "github.com/chroma-core/chroma/go/shared/libs"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"pgregory.net/rapid"
	"testing"
)

type ModelState struct {
	CollectionEnumerationOffset map[types.UniqueID]int64
	CollectionData              map[types.UniqueID][]*coordinatorpb.OperationRecord
	CollectionCompactionOffset  map[types.UniqueID]int64
}

type LogServerTestSuite struct {
	suite.Suite
	logServer logservicepb.LogServiceServer
	model     ModelState
	t         *testing.T
}

func (suite *LogServerTestSuite) SetupSuite() {
	ctx := context.Background()
	connectionString, err := libs2.StartPgContainer(ctx)
	assert.NoError(suite.t, err)
	assert.NoError(suite.t, err)
	var conn *pgx.Conn
	conn, err = libs2.NewPgConnection(ctx, connectionString)
	assert.NoError(suite.t, err)
	err = libs2.RunMigration(ctx, connectionString)
	assert.NoError(suite.t, err)
	lr := repository.NewLogRepository(conn)
	suite.logServer = NewLogServer(lr)
	suite.model = ModelState{
		CollectionData:             map[types.UniqueID][]*coordinatorpb.OperationRecord{},
		CollectionCompactionOffset: map[types.UniqueID]int64{},
	}
}

func (suite *LogServerTestSuite) TestRecordLogDb_PushLogs() {
	ctx := context.Background()
	// Generate collection ids
	collections := make([]types.UniqueID, 10)
	for i := 0; i < len(collections); i++ {
		collections[i] = types.NewUniqueID()
	}

	collectionGen := rapid.Custom(func(t *rapid.T) types.UniqueID {
		return collections[rapid.IntRange(0, len(collections)-1).Draw(t, "collection_id")]
	})
	recordGen := rapid.SliceOf(rapid.Custom(func(t *rapid.T) *coordinatorpb.OperationRecord {
		data := rapid.SliceOf(rapid.Byte()).Draw(t, "record_data")
		id := rapid.String().Draw(t, "record_id")
		return &coordinatorpb.OperationRecord{
			Id: id,
			Vector: &coordinatorpb.Vector{
				Vector: data,
			},
		}
	}))
	rapid.Check(suite.t, func(t *rapid.T) {
		t.Repeat(map[string]func(*rapid.T){
			"pushLogs": func(t *rapid.T) {
				c := collectionGen.Draw(t, "collection")
				records := recordGen.Draw(t, "record")
				r, err := suite.logServer.PushLogs(ctx, &logservicepb.PushLogsRequest{
					CollectionId: c.String(),
					Records:      records,
				})
				if err != nil {
					t.Fatal(err)
				}
				if int32(len(records)) != r.RecordCount {
					t.Fatal("record count mismatch", len(records), r.RecordCount)
				}
				suite.model.CollectionData[c] = append(suite.model.CollectionData[c], records...)
			},
			"getAllCollectionsToCompact": func(t *rapid.T) {
				result, err := suite.logServer.GetAllCollectionInfoToCompact(ctx, &logservicepb.GetAllCollectionInfoToCompactRequest{})
				assert.NoError(suite.t, err)
				for _, collection := range result.AllCollectionInfo {
					id, err := types.Parse(collection.CollectionId)
					if err != nil {
						t.Fatal(err)
					}
					compactionOffset := rapid.Int64Range(suite.model.CollectionCompactionOffset[id], int64(len(suite.model.CollectionData))).Draw(t, "new_position")
					_, err = suite.logServer.UpdateCollectionLogOffset(ctx, &logservicepb.UpdateCollectionLogOffsetRequest{
						CollectionId: id.String(),
						LogOffset:    compactionOffset,
					})
					if err != nil {
						t.Fatal(err)
					}
					suite.model.CollectionCompactionOffset[id] = compactionOffset
				}
			},
			"pullLogs": func(t *rapid.T) {
				c := collectionGen.Draw(t, "collection")
				startOffset := rapid.Int64Range(suite.model.CollectionCompactionOffset[c], int64(len(suite.model.CollectionData))).Draw(t, "start_offset")
				// If start offset is 0, we need to set it to 1 as the offset is 1 based
				if startOffset == 0 {
					startOffset = 1
				}
				batchSize := rapid.Int32Range(1, 20).Draw(t, "batch_size")
				response, err := suite.logServer.PullLogs(ctx, &logservicepb.PullLogsRequest{
					CollectionId:    c.String(),
					StartFromOffset: startOffset,
					BatchSize:       batchSize,
				})
				if err != nil {
					t.Fatal(err)
				}
				// Verify that record returned is matching the expected record
				for _, record := range response.Records {
					expectedRecord := suite.model.CollectionData[c][record.LogOffset-1]
					if string(expectedRecord.Vector.Vector) != string(record.Record.Vector.Vector) {
						t.Fatalf("expect record vector %s, got %s", string(expectedRecord.Vector.Vector), string(record.Record.Vector.Vector))
					}
					if expectedRecord.Id != record.Record.Id {
						t.Fatalf("expect record id %s, got %s", expectedRecord.Id, record.Record.Id)
					}
				}

				// Verify that the first and last record offset is correct
				if len(response.Records) > 0 {
					lastRecord := response.Records[len(response.Records)-1]
					firstRecord := response.Records[0]
					//
					expectedLastOffset := startOffset + int64(batchSize) - 1
					if expectedLastOffset > int64(len(suite.model.CollectionData[c])) {
						expectedLastOffset = int64(len(suite.model.CollectionData[c]))
					}
					if lastRecord.LogOffset != expectedLastOffset {
						t.Fatalf("expect last record %d, got %d", lastRecord.LogOffset, expectedLastOffset)
					}
					if firstRecord.LogOffset != startOffset {
						t.Fatalf("expect first record %d, got %d", startOffset, firstRecord.LogOffset)
					}
				}
			},
		})
	})
}

func TestLogServerTestSuite(t *testing.T) {
	testSuite := new(LogServerTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
