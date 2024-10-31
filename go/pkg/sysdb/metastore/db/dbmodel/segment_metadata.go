package dbmodel

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/types"
)

type SegmentMetadata struct {
	SegmentID  string          `gorm:"segment_id;primaryKey"`
	Key        *string         `gorm:"key;primaryKey"`
	StrValue   *string         `gorm:"str_value"`
	IntValue   *int64          `gorm:"int_value"`
	FloatValue *float64        `gorm:"float_value"`
	Ts         types.Timestamp `gorm:"ts;type:bigint;default:0"`
	CreatedAt  time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt  time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
	BoolValue  *bool           `gorm:"bool_value"`
}

func (SegmentMetadata) TableName() string {
	return "segment_metadata"
}

//go:generate mockery --name=ISegmentMetadataDb
type ISegmentMetadataDb interface {
	DeleteBySegmentID(segmentID string) error
	DeleteBySegmentIDAndKeys(segmentID string, keys []string) error
	Insert(in []*SegmentMetadata) error
	DeleteAll() error
}
