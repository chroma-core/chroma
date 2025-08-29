package server

import (
	"context"
	"math"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/log/configuration"
	"github.com/chroma-core/chroma/go/pkg/log/repository"
	log "github.com/chroma-core/chroma/go/pkg/log/store/db"
	"github.com/chroma-core/chroma/go/pkg/log/sysdb"
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
	// Offset upto which the log records have been purged.
	CollectionPurgedOffset map[types.UniqueID]uint64
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
	sysDb     sysdb.ISysDB
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
	suite.sysDb = sysdb.NewMockSysDB()
	suite.lr = repository.NewLogRepository(conn, suite.sysDb)
	suite.logServer = NewLogServer(suite.lr)
	suite.model = ModelState{
		CollectionEnumerationOffset: map[types.UniqueID]uint64{},
		CollectionData:              map[types.UniqueID][]ModelLogRecord{},
		CollectionCompactionOffset:  map[types.UniqueID]uint64{},
		CollectionPurgedOffset:      map[types.UniqueID]uint64{},
	}
}

func compareModelLogRecordToRecordLog(t *rapid.T, modelLogRecord ModelLogRecord, recordLog log.RecordLog) {
	record := &coordinatorpb.OperationRecord{}
	if err := proto.Unmarshal(recordLog.Record, record); err != nil {
		t.Fatal(err)
	}
	if int64(modelLogRecord.offset) != recordLog.Offset {
		t.Fatalf("expected offset %d, got %d for collection id %s", modelLogRecord.offset, recordLog.Offset, recordLog.CollectionID)
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
	var startOffset uint64
	// CollectionCompactionOffset is the last offset that was compacted.
	// CollectionCompactionOffset + 1 is the first valid offset for a pull.
	// Log is empty so return empty data.
	if suite.model.CollectionCompactionOffset[c] == suite.model.CollectionEnumerationOffset[c] {
		startOffset = suite.model.CollectionCompactionOffset[c] + 1
		batchSize := rapid.Uint32Range(1, 20).Draw(t, "batch_size")
		return []ModelLogRecord{}, startOffset, batchSize
	} else {
		startOffset = rapid.Uint64Range(suite.model.CollectionCompactionOffset[c]+1, suite.model.CollectionEnumerationOffset[c]).Draw(t, "start_offset")
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
		suite.model.CollectionPurgedOffset[id] = compactionOffset
	}
}

func (suite *LogServerTestSuite) modelGarbageCollection(ctx context.Context, t *rapid.T) {
	for id := range suite.model.CollectionData {
		exists, err := suite.sysDb.CheckCollections(ctx, []string{id.String()})
		if err != nil {
			t.Fatal(err)
		}
		exist := exists[0]
		if !exist {
			// Collection does not exist, so we can delete it
			delete(suite.model.CollectionData, id)
			delete(suite.model.CollectionEnumerationOffset, id)
			delete(suite.model.CollectionCompactionOffset, id)
			delete(suite.model.CollectionPurgedOffset, id)
		}
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

func TestLogServerTestSuite(t *testing.T) {
	testSuite := new(LogServerTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
