package dbmodel

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/model"

	"github.com/chroma-core/chroma/go/pkg/types"
)

type Segment struct {
	/* Making CollectionID the primary key allows fast search when we have CollectionID.
	   This requires us to push down CollectionID from the caller. We don't think there is
	   need to modify CollectionID in the near future. Each Segment should always have a
	   collection as a parent and cannot be modified. */
	CollectionID      *string             `gorm:"collection_id;primaryKey"`
	ID                string              `gorm:"id;primaryKey"`
	Type              string              `gorm:"type;type:string;not null"`
	Scope             string              `gorm:"scope"`
	Ts                types.Timestamp     `gorm:"ts;type:bigint;default:0"`
	IsDeleted         bool                `gorm:"is_deleted;type:bool;default:false"`
	CreatedAt         time.Time           `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt         time.Time           `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
	FilePaths         map[string][]string `gorm:"file_paths;serializer:json;default:'{}'"`
	LogPosition       int64               `gorm:"log_position;default:0"`
	CollectionVersion int32               `gorm:"collection_version;default:0"`
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
	Collection      *string
	ResetCollection bool
}

//go:generate mockery --name=ISegmentDb
type ISegmentDb interface {
	GetSegments(id types.UniqueID, segmentType *string, scope *string, collectionID types.UniqueID) ([]*SegmentAndMetadata, error)
	DeleteSegmentByID(id string) error
	Insert(*Segment) error
	Update(*UpdateSegment) error
	DeleteAll() error
	RegisterFilePaths(flushSegmentCompactions []*model.FlushSegmentCompaction) error
}
