package dbmodel

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/types"
)

type CollectionMetadata struct {
	CollectionID string          `gorm:"collection_id;primaryKey"`
	Key          *string         `gorm:"key;primaryKey"`
	StrValue     *string         `gorm:"str_value"`
	IntValue     *int64          `gorm:"int_value"`
	FloatValue   *float64        `gorm:"float_value"`
	Ts           types.Timestamp `gorm:"ts;type:bigint;default:0"`
	CreatedAt    time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt    time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
	BoolValue    *bool           `gorm:"bool_value"`
}

func (v CollectionMetadata) TableName() string {
	return "collection_metadata"
}

//go:generate mockery --name=ICollectionMetadataDb
type ICollectionMetadataDb interface {
	// GetForCollection(collectionID string) ([]*CollectionMetadata, error)
	DeleteByCollectionID(collectionID string) (int, error)
	Insert(in []*CollectionMetadata) error
	DeleteAll() error
}
