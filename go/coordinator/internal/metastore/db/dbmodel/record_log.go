package dbmodel

type RecordLog struct {
	CollectionID *string `gorm:"collection_id;primaryKey;autoIncrement:false"`
	ID           int64   `gorm:"id;primaryKey;"` // auto_increment id
	Timestamp    int64   `gorm:"timestamp;"`
	Record       *[]byte `gorm:"record;type:bytea"`
}

func (v RecordLog) TableName() string {
	return "record_logs"
}

//go:generate mockery --name=IRecordLogDb
type IRecordLogDb interface {
}
