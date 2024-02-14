package dao

import (
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRecordLogDb_PushLogs(t *testing.T) {
	db := dbcore.ConfigDatabaseForTesting()
	db.Migrator().DropTable(&dbmodel.RecordLog{})
	db.Migrator().CreateTable(&dbmodel.RecordLog{})
	Db := &recordLogDb{
		db: db,
	}

	collection_id := types.NewUniqueID()
	records := make([][]byte, 0, 5)
	records = append(records, []byte("test1"), []byte("test2"),
		[]byte("test3"), []byte("test4"), []byte("test5"))

	// run push logs in transaction
	// id: 0,
	// offset: 0, 1, 2
	// records: test1, test2, test3
	count, err := Db.PushLogs(collection_id, records[:3])
	assert.NoError(t, err)
	assert.Equal(t, 3, count)

	// verify logs are pushed
	var recordLogs []*dbmodel.RecordLog
	db.Where("collection_id = ?", types.FromUniqueID(collection_id)).Find(&recordLogs)
	assert.Len(t, recordLogs, 3)
	for index := range recordLogs {
		assert.Equal(t, int64(index+1), recordLogs[index].ID)
		assert.Equal(t, records[index], *recordLogs[index].Record)
	}

	// run push logs in transaction
	// id: 1,
	// offset: 0, 1
	// records: test4, test5
	count, err = Db.PushLogs(collection_id, records[3:])
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	// verify logs are pushed
	db.Where("collection_id = ?", types.FromUniqueID(collection_id)).Find(&recordLogs)
	assert.Len(t, recordLogs, 5)
	for index := range recordLogs {
		assert.Equal(t, int64(index+1), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(t, records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	db.Migrator().DropTable(&dbmodel.RecordLog{})
}

func TestRecordLogDb_PullLogsFromID(t *testing.T) {
	db := dbcore.ConfigDatabaseForTesting()
	db.Migrator().DropTable(&dbmodel.RecordLog{})
	db.Migrator().CreateTable(&dbmodel.RecordLog{})
	Db := &recordLogDb{
		db: db,
	}

	collectionId := types.NewUniqueID()
	records := make([][]byte, 0, 5)
	records = append(records, []byte("test1"), []byte("test2"),
		[]byte("test3"), []byte("test4"), []byte("test5"))

	// push some logs
	count, err := Db.PushLogs(collectionId, records[:3])
	assert.NoError(t, err)
	assert.Equal(t, 3, count)
	count, err = Db.PushLogs(collectionId, records[3:])
	assert.NoError(t, err)
	assert.Equal(t, 2, count)

	// pull logs from id 0 batch_size 3
	var recordLogs []*dbmodel.RecordLog
	recordLogs, err = Db.PullLogs(collectionId, 0, 3)
	assert.NoError(t, err)
	assert.Len(t, recordLogs, 3)
	for index := range recordLogs {
		assert.Equal(t, int64(index+1), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(t, records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	// pull logs from id 0 batch_size 6
	recordLogs, err = Db.PullLogs(collectionId, 0, 6)
	assert.NoError(t, err)
	assert.Len(t, recordLogs, 5)

	for index := range recordLogs {
		assert.Equal(t, int64(index+1), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(t, records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	// pull logs from id 3 batch_size 4
	recordLogs, err = Db.PullLogs(collectionId, 3, 4)
	assert.NoError(t, err)
	assert.Len(t, recordLogs, 3)
	for index := range recordLogs {
		assert.Equal(t, int64(index+3), recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(t, records[index+2], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	db.Migrator().DropTable(&dbmodel.RecordLog{})
}
