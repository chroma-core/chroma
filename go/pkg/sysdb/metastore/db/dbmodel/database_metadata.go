package dbmodel

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/types"
)

type DatabaseMetadata struct {
	DatabaseID string          `gorm:"database_id;primaryKey"`
	Key        *string         `gorm:"key;primaryKey"`
	StrValue   *string         `gorm:"str_value"`
	IntValue   *int64          `gorm:"int_value"`
	FloatValue *float64        `gorm:"float_value"`
	BoolValue  *bool           `gorm:"bool_value"`
	Ts         types.Timestamp `gorm:"ts;type:bigint;default:0"`
	CreatedAt  time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt  time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
}

func (v DatabaseMetadata) TableName() string {
	return "database_metadata"
}

//go:generate mockery --name=IDatabaseMetadataDb
type IDatabaseMetadataDb interface {
	GetByDatabaseID(databaseID string) ([]*DatabaseMetadata, error)
	GetByDatabaseIDs(databaseIDs []string) ([]*DatabaseMetadata, error)
	DeleteByDatabaseID(databaseID string) (int, error)
	Insert(in []*DatabaseMetadata) error
	DeleteAll() error
}
