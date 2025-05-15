package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/log/configuration"
	log "github.com/chroma-core/chroma/go/pkg/log/store/db"
	"github.com/chroma-core/chroma/go/pkg/log/sysdb"
	"github.com/chroma-core/chroma/go/pkg/types"
	libs2 "github.com/chroma-core/chroma/go/shared/libs"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type LogTestSuite struct {
	suite.Suite
	t     *testing.T
	lr    *LogRepository
	sysDb sysdb.ISysDB
}

func (suite *LogTestSuite) SetupSuite() {
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
	suite.lr = NewLogRepository(conn, suite.sysDb)
}

func (suite *LogTestSuite) TestGarbageCollection() {
	ctx := context.Background()
	collectionID1 := types.NewUniqueID()
	collectionID2 := types.NewUniqueID()

	// Add records to collection 1
	count, isSealed, err := suite.lr.InsertRecords(ctx, collectionID1.String(), [][]byte{{1, 2, 3}})
	assert.NoError(suite.t, err, "Failed to insert records")
	assert.False(suite.t, isSealed, count, "Log sealed")
	assert.Equal(suite.t, int64(1), count, "Failed to insert records")

	// Add records to collection 2
	count, isSealed, err = suite.lr.InsertRecords(ctx, collectionID2.String(), [][]byte{{1, 2, 3}})
	assert.NoError(suite.t, err, "Failed to insert records")
	assert.False(suite.t, isSealed, count, "Log sealed")
	assert.Equal(suite.t, int64(1), count, "Failed to insert records")

	// Add collection 1 to sysdb
	err = suite.sysDb.AddCollection(ctx, collectionID1.String())
	assert.NoError(suite.t, err, "Failed to add collection")

	err = suite.lr.GarbageCollection(ctx)
	assert.NoError(suite.t, err, "Failed to run garbage collection")

	records, err := suite.lr.PullRecords(ctx, collectionID1.String(), 1, 1, time.Now().UnixNano())
	assert.NoError(suite.t, err, "Failed to pull records")
	assert.Equal(suite.t, 1, len(records), "Failed to run garbage collection")
	assert.Equal(suite.t, []byte{1, 2, 3}, records[0].Record, "Failed to run garbage collection")
	assert.Equal(suite.t, int64(1), records[0].Offset, "Failed to run garbage collection")

	records, err = suite.lr.PullRecords(ctx, collectionID2.String(), 1, 1, time.Now().UnixNano())
	assert.NoError(suite.t, err, "Failed to pull records")
	assert.Equal(suite.t, 0, len(records), "Failed to run garbage collection")

	// Add records to collection 2, expect offset to reset
	count, isSealed, err = suite.lr.InsertRecords(ctx, collectionID2.String(), [][]byte{{4, 5, 6}})
	assert.NoError(suite.t, err, "Failed to insert records")
	assert.False(suite.t, isSealed, count, "Log sealed")
	assert.Equal(suite.t, int64(1), count, "Failed to insert records")
	records, err = suite.lr.PullRecords(ctx, collectionID2.String(), 1, 1, time.Now().UnixNano())
	assert.NoError(suite.t, err, "Failed to pull records")
	assert.Equal(suite.t, 1, len(records), "Failed to run garbage collection")
	assert.Equal(suite.t, []byte{4, 5, 6}, records[0].Record, "Failed to run garbage collection")
	assert.Equal(suite.t, int64(1), records[0].Offset, "Failed to run garbage collection")
}

func (suite *LogTestSuite) TestUniqueConstraintPushLogs() {
	ctx := context.Background()
	collectionId := types.NewUniqueID()

	records := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
	}
	params := make([]log.InsertRecordParams, 2)
	for i, record := range records {
		offset := 1
		params[i] = log.InsertRecordParams{
			CollectionID: collectionId.String(),
			Record:       record,
			Offset:       int64(offset),
			Timestamp:    time.Now().UnixNano(),
		}
	}
	_, err := suite.lr.queries.InsertRecord(ctx, params)
	assert.Error(suite.t, err, "Failed to insert records")
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		assert.Equal(suite.t, "23505", pgErr.Code, "Expected SQLSTATE 23505 for duplicate key")
	} else {
		assert.Fail(suite.t, "Expected pgconn.PgError but got different error", err)
	}
}

func (suite *LogTestSuite) TestSealedLogWontPush() {
	ctx := context.Background()
	collectionId := types.NewUniqueID()
	params := log.InsertCollectionParams {
		ID: collectionId.String(),
		RecordEnumerationOffsetPosition: 1,
		RecordCompactionOffsetPosition: 0,
	}
	_, err := suite.lr.queries.InsertCollection(ctx, params)
	assert.NoError(suite.t, err, "Initializing log should not fail.")
	_, err = suite.lr.queries.SealLog(ctx, collectionId.String())
	assert.NoError(suite.t, err, "Sealing log should not fail.")
	var isSealed bool
	_, isSealed, err = suite.lr.InsertRecords(ctx, collectionId.String(), [][]byte{{1,2,3}})
	assert.NoError(suite.t, err, "Failed to push logs")
	assert.True(suite.t, isSealed, "Did not report was sealed")
}

func TestLogTestSuite(t *testing.T) {
	testSuite := new(LogTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
