package server

import (
	"context"
	"math"
	"testing"
	"time"

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
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

type ModelState struct {
	// The current max offset for each collection
	CollectionEnumerationOffset map[types.UniqueID]uint64
	// The current non-purged log for each collection and its offset
	CollectionData map[types.UniqueID][]ModelLogRecord
	// The current compaction offset for each collection (the last offset that was compacted)
	CollectionCompactionOffset map[types.UniqueID]uint64
}

// A log entry in the model (for testing only)
type ModelLogRecord struct {
	offset uint64
	record *coordinatorpb.OperationRecord
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
		CollectionEnumerationOffset: map[types.UniqueID]uint64{},
		CollectionData:              map[types.UniqueID][]ModelLogRecord{},
		CollectionCompactionOffset:  map[types.UniqueID]uint64{},
	}
}

// Invariants

// Check that the correct set of collections are returned for compaction
// The set of collections returned for compaction should be the set of collections
// where the difference between the enumeration offset and the compaction offset
// is greater than the minimum compaction size (if specified)
// Additionally, we should never return a collection if it is not dirty
func (suite *LogServerTestSuite) invariantAllDirtyCollectionsAreReturnedForCompaction(ctx context.Context, t *rapid.T) {
	result, err := suite.logServer.GetAllCollectionInfoToCompact(ctx, &logservicepb.GetAllCollectionInfoToCompactRequest{})
	assert.NoError(suite.t, err)
	numCollectionsNeedingCompaction := 0
	// Iterate over collections with log data
	for collectionId, _ := range suite.model.CollectionData {
		compactionOffset, ok := suite.model.CollectionCompactionOffset[collectionId]
		if !ok {
			compactionOffset = 0
		}

		enumerationOffset, ok := suite.model.CollectionEnumerationOffset[collectionId]
		if !ok {
			t.Fatalf("State inconsistency: collection %s has no enumeration offset, yet has log data", collectionId)
		}

		if enumerationOffset-compactionOffset > 0 {
			numCollectionsNeedingCompaction++
			// Expect to find the collection in the result
			found := false
			for _, collection := range result.AllCollectionInfo {
				id, err := types.Parse(collection.CollectionId)
				if err != nil {
					t.Fatal(err)
				}
				if id == collectionId {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("collection %s not found in result", collectionId)
			}
		}
	}
	if numCollectionsNeedingCompaction != len(result.AllCollectionInfo) {
		t.Fatalf("expected %d collections needing compaction, got %d", numCollectionsNeedingCompaction, len(result.AllCollectionInfo))
	}
}

func compareModelLogRecordToRecordLog(t *rapid.T, modelLogRecord ModelLogRecord, recordLog log.RecordLog) {
	record := &coordinatorpb.OperationRecord{}
	if err := proto.Unmarshal(recordLog.Record, record); err != nil {
		t.Fatal(err)
	}
	if int64(modelLogRecord.offset) != recordLog.Offset {
		t.Fatalf("expected offset %d, got %d", modelLogRecord.offset, recordLog.Offset)
	}
	if modelLogRecord.record.Id != record.Id {
		t.Fatalf("expected record id %s, got %s", modelLogRecord.record.Id, record.Id)
	}
	if string(modelLogRecord.record.Vector.Vector) != string(record.Vector.Vector) {
		t.Fatalf("expected record vector %s, got %s", string(modelLogRecord.record.Vector.Vector), string(record.Vector.Vector))
	}
	if modelLogRecord.record.Vector.Encoding != record.Vector.Encoding {
		t.Fatalf("expected record encoding %s, got %s", modelLogRecord.record.Vector.Encoding, record.Vector.Encoding)
	}
	if modelLogRecord.record.Vector.Dimension != record.Vector.Dimension {
		t.Fatalf("expected record dimension %d, got %d", modelLogRecord.record.Vector.Dimension, record.Vector.Dimension)
	}
	if modelLogRecord.record.Operation != record.Operation {
		t.Fatalf("expected record operation %s, got %s", modelLogRecord.record.Operation, record.Operation)
	}
	if modelLogRecord.record.Metadata != record.Metadata {
		t.Fatalf("expected record metadata %s, got %s", modelLogRecord.record.Metadata, record.Metadata)
	}
}

func compareModelLogRecordToLogRecord(t *rapid.T, modelLogRecord ModelLogRecord, recordLog *logservicepb.LogRecord) {
	if int64(modelLogRecord.offset) != recordLog.LogOffset {
		t.Fatalf("expected offset %d, got %d", modelLogRecord.offset, recordLog.LogOffset)
	}
	if modelLogRecord.record.Id != recordLog.Record.Id {
		t.Fatalf("expected record id %s, got %s", modelLogRecord.record.Id, recordLog.Record.Id)
	}
	if string(modelLogRecord.record.Vector.Vector) != string(recordLog.Record.Vector.Vector) {
		t.Fatalf("expected record vector %s, got %s", string(modelLogRecord.record.Vector.Vector), string(recordLog.Record.Vector.Vector))
	}
	if modelLogRecord.record.Vector.Encoding != recordLog.Record.Vector.Encoding {
		t.Fatalf("expected record encoding %s, got %s", modelLogRecord.record.Vector.Encoding, recordLog.Record.Vector.Encoding)
	}
	if modelLogRecord.record.Vector.Dimension != recordLog.Record.Vector.Dimension {
		t.Fatalf("expected record dimension %d, got %d", modelLogRecord.record.Vector.Dimension, recordLog.Record.Vector.Dimension)
	}
	if modelLogRecord.record.Operation != recordLog.Record.Operation {
		t.Fatalf("expected record operation %s, got %s", modelLogRecord.record.Operation, recordLog.Record.Operation)
	}
	if modelLogRecord.record.Metadata != recordLog.Record.Metadata {
		t.Fatalf("expected record metadata %s, got %s", modelLogRecord.record.Metadata, recordLog.Record.Metadata)
	}
}

// Check that the set of logs from the compaction offset onwards
// is the same in both the model and the SUT
func (suite *LogServerTestSuite) invariantLogsAreTheSame(ctx context.Context, t *rapid.T) {
	for id, model_log := range suite.model.CollectionData {
		pulled_log, err := suite.lr.PullRecords(ctx, id.String(), 0, len(model_log), time.Now().UnixNano())
		if err != nil {
			t.Fatal(err)
		}
		// Length of log should be the same
		if len(model_log) != len(pulled_log) {
			t.Fatalf("expected log length %d, got %d", len(model_log), len(pulled_log))
		}

		// Each record should be the same
		for i, modelLogRecord := range model_log {
			// Compare the record
			compareModelLogRecordToRecordLog(t, modelLogRecord, pulled_log[i])
		}
	}
}

func (suite *LogServerTestSuite) modelPushLogs(ctx context.Context, t *rapid.T, collectionId types.UniqueID, recordsToPush []*coordinatorpb.OperationRecord) {
	// Update the model
	startEnumerationOffset, ok := suite.model.CollectionEnumerationOffset[collectionId]
	if !ok {
		startEnumerationOffset = 0
	}
	// Enumeration offset is 1 based and should always be
	// 1 greater than the last offset
	startEnumerationOffset++

	for i, record := range recordsToPush {
		modelRecord := ModelLogRecord{
			offset: startEnumerationOffset + uint64(i),
			record: record,
		}
		suite.model.CollectionData[collectionId] = append(suite.model.CollectionData[collectionId], modelRecord)
		suite.model.CollectionEnumerationOffset[collectionId] = startEnumerationOffset + uint64(i)
	}

}

func (suite *LogServerTestSuite) modelPullLogs(ctx context.Context, t *rapid.T, c types.UniqueID) ([]ModelLogRecord, uint64, uint32) {
	startOffset := rapid.Uint64Range(suite.model.CollectionCompactionOffset[c], suite.model.CollectionEnumerationOffset[c]).Draw(t, "start_offset")
	// If start offset is 0, we need to set it to 1 as the offset is 1 based
	if startOffset == 0 {
		startOffset = 1
	}
	batchSize := rapid.Uint32Range(1, 20).Draw(t, "batch_size")

	// Pull logs from the model
	modelLogs := suite.model.CollectionData[c]
	// Find start offset in the model, which is the first offset that is greater than or equal to the start offset
	startIndex := -1
	for i, record := range modelLogs {
		if record.offset >= startOffset {
			startIndex = i
			break
		}
	}
	if startIndex == -1 {
		t.Fatalf("start offset %d not found in model", startOffset)
	}
	endIndex := startIndex + int(batchSize)
	if endIndex > len(modelLogs) {
		endIndex = len(modelLogs)
	}
	expectedRecords := modelLogs[startIndex:endIndex]
	return expectedRecords, startOffset, batchSize
}

func (suite *LogServerTestSuite) modelPurgeLogs(ctx context.Context, t *rapid.T) {
	for id, log := range suite.model.CollectionData {
		compactionOffset, ok := suite.model.CollectionCompactionOffset[id]
		if !ok {
			// No compaction has occurred yet, so we can't purge
			continue
		}

		new_log := []ModelLogRecord{}
		for _, record := range log {
			// Purge by adding everything after the compaction offset
			if record.offset > compactionOffset {
				new_log = append(new_log, record)
			}
		}
		suite.model.CollectionData[id] = new_log
	}
}

func (suite *LogServerTestSuite) modelGetAllCollectionInfoToCompact(ctx context.Context, t *rapid.T) (uint64, uint64, map[types.UniqueID]uint64, bool) {
	minCompactionSize := uint64(math.MaxUint64)
	maxCompactionSize := uint64(0)
	actualCompactionSizes := make(map[types.UniqueID]uint64)
	allEmpty := true
	for id, log := range suite.model.CollectionData {
		if len(log) > 0 {
			allEmpty = false
		}

		enumerationOffset := suite.model.CollectionEnumerationOffset[id]
		compactionOffset, ok := suite.model.CollectionCompactionOffset[id]
		if !ok {
			compactionOffset = 0
		}
		delta := enumerationOffset - compactionOffset
		actualCompactionSizes[id] = delta
		if delta < minCompactionSize {
			minCompactionSize = delta
		}
		if delta > maxCompactionSize {
			maxCompactionSize = delta
		}
	}
	return minCompactionSize, maxCompactionSize, actualCompactionSizes, allEmpty
}

// State machine
func (suite *LogServerTestSuite) TestRecordLogDb_PushLogs() {
	ctx := context.Background()
	maxCollections := 100
	collections := make(map[int]types.UniqueID)

	collectionGen := rapid.Custom(func(t *rapid.T) types.UniqueID {
		position := rapid.IntRange(0, maxCollections-1).Draw(t, "collection_position")
		if _, ok := collections[position]; !ok {
			collections[position] = types.NewUniqueID()
		}
		return collections[position]
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
				// Generate data
				c := collectionGen.Draw(t, "collection")
				records := recordGen.Draw(t, "record")

				// Update the model
				suite.modelPushLogs(ctx, t, c, records)

				// Update the SUT
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
			},
			"compaction": func(t *rapid.T) {
				// Query the SUT for all collections to compact (We could query the model too
				// it doesn't really matter since we just want to know that if compacted
				// the output is the same in getallcollections, which we have another
				// transition for)
				result, err := suite.logServer.GetAllCollectionInfoToCompact(ctx, &logservicepb.GetAllCollectionInfoToCompactRequest{})
				assert.NoError(suite.t, err)

				// For reach collection the SUT wants to compact, perform a compaction
				for _, collection := range result.AllCollectionInfo {
					id, err := types.Parse(collection.CollectionId)
					if err != nil {
						t.Fatal(err)
					}
					enumerationOffset := suite.model.CollectionEnumerationOffset[id]

					// Update the SUT
					compactionOffset := rapid.Uint64Range(suite.model.CollectionCompactionOffset[id], enumerationOffset).Draw(t, "new_position")
					_, err = suite.logServer.UpdateCollectionLogOffset(ctx, &logservicepb.UpdateCollectionLogOffsetRequest{
						CollectionId: id.String(),
						LogOffset:    int64(compactionOffset),
					})
					if err != nil {
						t.Fatal(err)
					}

					// Update the model
					suite.model.CollectionCompactionOffset[id] = compactionOffset
				}
			},
			"getAllCollectionsToCompactWithMinCompactionSize": func(t *rapid.T) {
				if len(suite.model.CollectionData) == 0 {
					// Nothing to do if no data
					return
				}

				// Query the model
				minCompactionSize, maxCompactionSize, actualCompactionSizes, allEmpty := suite.modelGetAllCollectionInfoToCompact(ctx, t)
				if allEmpty {
					// Nothing to do if no data
					return
				}

				// Query the SUT
				requestMinCompactionSize := rapid.Uint64Range(minCompactionSize, maxCompactionSize).Draw(t, "min_compaction_size")
				result, err := suite.logServer.GetAllCollectionInfoToCompact(ctx, &logservicepb.GetAllCollectionInfoToCompactRequest{
					MinCompactionSize: requestMinCompactionSize,
				})
				assert.NoError(suite.t, err)

				// Verify that the result is correct
				for _, collection := range result.AllCollectionInfo {
					id, err := types.Parse(collection.CollectionId)
					if err != nil {
						t.Fatal(err)
					}

					actualCompactionSize := actualCompactionSizes[id]
					if actualCompactionSize < requestMinCompactionSize {
						t.Fatalf("compaction size %d is less than request min compaction size %d", actualCompactionSize, requestMinCompactionSize)
					}
				}

				// Verify that the length of the results is correct
				model_expects := 0
				for _, size := range actualCompactionSizes {
					if size >= requestMinCompactionSize && size > 0 {
						model_expects++
					}
				}
				if model_expects != len(result.AllCollectionInfo) {
					t.Fatalf("expected %d collections, got %d", model_expects, len(result.AllCollectionInfo))
				}
			},
			"pullLogs": func(t *rapid.T) {
				c := collectionGen.Draw(t, "collection")

				// Pull logs from the model
				// If the collection has no data, we can't pull logs
				if len(suite.model.CollectionData[c]) == 0 {
					return
				}

				expectedRecords, startOffset, batchSize := suite.modelPullLogs(ctx, t, c)

				// Pull logs from the SUT
				response, err := suite.logServer.PullLogs(ctx, &logservicepb.PullLogsRequest{
					CollectionId:    c.String(),
					StartFromOffset: int64(startOffset),
					BatchSize:       int32(batchSize),
					EndTimestamp:    time.Now().UnixNano(),
				})
				if err != nil {
					t.Fatal(err)
				}

				// Verify that the number of records returned is correct
				if int64(len(response.Records)) != int64(len(expectedRecords)) {
					t.Fatalf("expected %d records, got %d", len(expectedRecords), len(response.Records))
				}

				// Verify the record data is the same
				for i, logRecord := range response.Records {
					expectedLogRecord := expectedRecords[i]
					compareModelLogRecordToLogRecord(t, expectedLogRecord, logRecord)
				}
			},
			"purgeLogs": func(t *rapid.T) {
				// Purge the model
				suite.modelPurgeLogs(ctx, t)

				// Purge the SUT
				err := suite.lr.PurgeRecords(ctx)
				suite.NoError(err)

				// Verify that all record logs are purged
				for id, offset := range suite.model.CollectionCompactionOffset {
					if offset != 0 {
						var records []log.RecordLog
						records, err = suite.lr.PullRecords(ctx, id.String(), 0, 1, time.Now().UnixNano())
						suite.NoError(err)
						if len(records) > 0 {
							suite.Equal(int64(offset)+1, records[0].Offset)
						}
					}
				}
			},
			"": func(t *rapid.T) {
				// "" is the invariant check function in rapid
				suite.invariantAllDirtyCollectionsAreReturnedForCompaction(ctx, t)
				suite.invariantLogsAreTheSame(ctx, t)
			},
		})
	})
}

func TestLogServerTestSuite(t *testing.T) {
	testSuite := new(LogServerTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
