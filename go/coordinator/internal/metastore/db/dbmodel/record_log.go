package dbmodel

import "github.com/chroma/chroma-coordinator/internal/types"

type RecordLog struct {
	CollectionID *string `gorm:"collection_id;primaryKey;autoIncrement:false"`
	ID           int64   `gorm:"id;primaryKey;autoIncrement:false"` // timestamp
	Offset       int     `gorm:"offset;primaryKey;autoIncrement:false"`
	Record       *string `gorm:"record"`
}

func (v RecordLog) TableName() string {
	return "record_logs"
}

//go:generate mockery --name=IRecordLogDb
type IRecordLogDb interface {
	PushLogs(collectionID types.UniqueID, recordContent []string) error
}
