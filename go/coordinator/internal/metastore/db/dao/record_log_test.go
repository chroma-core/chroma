package dao

import (
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"testing"
)

func GetTiDBConfig() dbcore.DBConfig {
	dBConfig := dbcore.DBConfig{
		Username: "root",
		Password: "emuY1ktyq5Tq4nGx",
		Address:  "tidb.ootbbu125szh.clusters.tidb-cloud.com",
		Port:     4000,
		DBName:   "test",
	}
	return dBConfig
}

func TestRecordLogDb_PushLogs(t *testing.T) {
	db := dbcore.ConnectTiDB(GetTiDBConfig())
	db.Migrator().DropTable(&dbmodel.RecordLog{})
	db.Migrator().CreateTable(&dbmodel.RecordLog{})

	collection_id := types.NewUniqueID()
	records := make([]string, 0, 5)
	records = append(records, "test1", "test2", "test3", "test4", "test5")

	// run push logs in transaction
	// id: 0,
	// offset: 0, 1, 2
	// records: test1, test2, test3
	err := db.Transaction(func(tx *gorm.DB) error {
		Db := &recordLogDb{
			db: tx,
		}
		return Db.PushLogs(collection_id, records[:3])
	})
	assert.NoError(t, err)

	// verify logs are pushed
	var recordLogs []*dbmodel.RecordLog
	db.Where("collection_id = ?", types.FromUniqueID(collection_id)).Find(&recordLogs)
	assert.Len(t, recordLogs, 3)
	var id int64
	for index := range recordLogs {
		id = recordLogs[index].ID
		assert.Equal(t, index, recordLogs[index].Offset)
		assert.Equal(t, records[index], *recordLogs[index].Record)
	}

	// run push logs in transaction
	// id: 1,
	// offset: 0, 1
	// records: test4, test5
	err = db.Transaction(func(tx *gorm.DB) error {
		Db := &recordLogDb{
			db: tx,
		}
		return Db.PushLogs(collection_id, records[3:])
	})
	assert.NoError(t, err)

	// verify logs are pushed
	db.Where("collection_id = ?", types.FromUniqueID(collection_id)).Find(&recordLogs)
	assert.Len(t, recordLogs, 5)
	for index := range recordLogs {
		if index < 3 {
			assert.Equal(t, id, recordLogs[index].ID, "id mismatch for index %d", index)
			assert.Equal(t, index, recordLogs[index].Offset, "offset mismatch for index %d", index)
		} else {
			assert.NotEqual(t, id, recordLogs[index].ID, "id mismatch for index %d", index)
			assert.Equal(t, index, recordLogs[index].Offset+3, "offset mismatch for index %d", index)
		}
		assert.Equal(t, records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	db.Migrator().DropTable(&dbmodel.RecordLog{})
}

func TestRecordLogDb_PullLogsFromID(t *testing.T) {
	db := dbcore.ConnectTiDB(GetTiDBConfig())
	db.Migrator().DropTable(&dbmodel.RecordLog{})
	db.Migrator().CreateTable(&dbmodel.RecordLog{})
	Db := &recordLogDb{
		db: db,
	}

	collection_id := types.NewUniqueID()
	records := make([]string, 0, 5)
	records = append(records, "test1", "test2", "test3", "test4", "test5")
	// push some logs
	err := db.Transaction(func(tx *gorm.DB) error {
		Db := &recordLogDb{
			db: tx,
		}
		return Db.PushLogs(collection_id, records[:3])
	})
	assert.NoError(t, err)
	err = db.Transaction(func(tx *gorm.DB) error {
		Db := &recordLogDb{
			db: tx,
		}
		return Db.PushLogs(collection_id, records[3:])
	})
	assert.NoError(t, err)

	// pull logs from id 0 batch_size 3
	var recordLogs []*dbmodel.RecordLog
	recordLogs, err = Db.PullLogsFromID(collection_id, 0, 3)
	assert.NoError(t, err)
	assert.Len(t, recordLogs, 3)
	var id int64
	for index := range recordLogs {
		id = recordLogs[index].ID
		assert.Equal(t, index, recordLogs[index].Offset)
		assert.Equal(t, records[index], *recordLogs[index].Record)
	}

	// pull logs from id 0 batch_size 5
	recordLogs, err = Db.PullLogsFromID(collection_id, 0, 5)
	assert.NoError(t, err)
	assert.Len(t, recordLogs, 5)
	var id2 int64
	for index := range recordLogs {
		if index < 3 {
			assert.Equal(t, id, recordLogs[index].ID, "id mismatch for index %d", index)
			assert.Equal(t, index, recordLogs[index].Offset, "offset mismatch for index %d", index)
		} else {
			id2 = recordLogs[index].ID
			assert.NotEqual(t, id, recordLogs[index].ID, "id mismatch for index %d", index)
			assert.Equal(t, index, recordLogs[index].Offset+3, "offset mismatch for index %d", index)
		}
		assert.Equal(t, records[index], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	// pull logs from id 1 batch_size 3
	recordLogs, err = Db.PullLogsFromID(collection_id, id+1, 3)
	assert.NoError(t, err)
	assert.Len(t, recordLogs, 2)
	for index := range recordLogs {
		assert.Equal(t, id2, recordLogs[index].ID, "id mismatch for index %d", index)
		assert.Equal(t, index, recordLogs[index].Offset, "offset mismatch for index %d", index)
		assert.Equal(t, records[index+3], *recordLogs[index].Record, "record mismatch for index %d", index)
	}

	// pull logs from id 0 batch_size 2
	recordLogs, err = Db.PullLogsFromID(collection_id, id, 2)
	assert.NoError(t, err)
	assert.Len(t, recordLogs, 2)
	for index := range recordLogs {
		id = recordLogs[index].ID
		assert.Equal(t, index, recordLogs[index].Offset)
		assert.Equal(t, records[index], *recordLogs[index].Record)
	}

	db.Migrator().DropTable(&dbmodel.RecordLog{})
}
