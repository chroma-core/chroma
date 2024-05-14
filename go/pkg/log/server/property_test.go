package server

import (
	"context"
	log "github.com/chroma-core/chroma/go/database/log/db"
	"github.com/chroma-core/chroma/go/pkg/log/configuration"
	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/chroma-core/chroma/go/pkg/types"
	libs2 "github.com/chroma-core/chroma/go/shared/libs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"pgregory.net/rapid"
	"testing"
	"time"
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
	lr        *repository.LogRepository
}

func (suite *LogServerTestSuite) SetupSuite() {
	ctx := context.Background()
	config := configuration.NewLogServiceConfiguration()
	connectionString, err := libs2.StartPgContainer(ctx)
	config.DATABASE_URL = connectionString
	assert.NoError(suite.t, err, "Failed to start pg container")
	var conn *pgxpool.Pool
	conn, err = libs2.NewPgConnection(ctx, config)
	assert.NoError(suite.t, err, "Failed to create new pg connection")
	err = libs2.RunMigration(ctx, connectionString)
	assert.NoError(suite.t, err, "Failed to run migration")
	suite.lr = repository.NewLogRepository(conn)
	suite.logServer = NewLogServer(suite.lr)
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
					EndTimestamp:    time.Now().UnixNano(),
				})
				if err != nil {
					t.Fatal(err)
				}
				// Verify that the number of records returned is correct
				if len(suite.model.CollectionData[c]) > int(startOffset) {
					if len(suite.model.CollectionData[c])-int(startOffset) < int(batchSize) {
						suite.Equal(len(response.Records), len(suite.model.CollectionData[c])-int(startOffset)+1)
					} else {
						suite.Equal(len(response.Records), int(batchSize))
					}
				}
				// Verify that the first record offset is correct
				if len(response.Records) > 0 {
					suite.Equal(response.Records[0].LogOffset, startOffset)
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
			"purgeLogs": func(t *rapid.T) {
				err := suite.lr.PurgeRecords(ctx)
				suite.NoError(err)
				// Verify that all record logs are purged
				for id, offset := range suite.model.CollectionCompactionOffset {
					if offset != 0 {
						var records []log.RecordLog
						records, err = suite.lr.PullRecords(ctx, id.String(), 0, 1, time.Now().UnixNano())
						suite.NoError(err)
						if len(records) > 0 {
							suite.Equal(offset, records[0].Offset)
						}
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
