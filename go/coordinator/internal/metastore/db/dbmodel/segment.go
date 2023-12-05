package dbmodel

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type Segment struct {
	ID           string          `gorm:"id;primaryKey"`
	Type         string          `gorm:"type;type:string;not null"`
	Scope        string          `gorm:"scope"`
	Topic        *string         `gorm:"topic"`
	Ts           types.Timestamp `gorm:"ts;type:bigint;default:0"`
	IsDeleted    bool            `gorm:"is_deleted;type:bool;default:false"`
	CreatedAt    time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt    time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
	CollectionID *string         `gorm:"collection_id"`
}

func (s Segment) TableName() string {
	return "segments"
}

type SegmentAndMetadata struct {
	Segment         *Segment
	SegmentMetadata []*SegmentMetadata
}

type UpdateSegment struct {
	ID              string
	Topic           *string
	ResetTopic      bool
	Collection      *string
	ResetCollection bool
}

//go:generate mockery --name=ISegmentDb
type ISegmentDb interface {
	GetSegments(id types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*SegmentAndMetadata, error)
	DeleteSegmentByID(id string) error
	Insert(*Segment) error
	Update(*UpdateSegment) error
	DeleteAll() error
}
