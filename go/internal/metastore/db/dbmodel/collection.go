package dbmodel

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/types"
)

type Collection struct {
	ID         string          `gorm:"id;primaryKey"`
	Name       *string         `gorm:"name;unique"`
	Topic      *string         `gorm:"topic"`
	Dimension  *int32          `gorm:"dimension"`
	DatabaseID string          `gorm:"database_id"`
	Ts         types.Timestamp `gorm:"ts;type:bigint;default:0"`
	IsDeleted  bool            `gorm:"is_deleted;type:bool;default:false"`
	CreatedAt  time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt  time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
}

func (v Collection) TableName() string {
	return "collections"
}

type CollectionAndMetadata struct {
	Collection         *Collection
	CollectionMetadata []*CollectionMetadata
	TenantID           string
	DatabaseName       string
}

//go:generate mockery --name=ICollectionDb
type ICollectionDb interface {
	GetCollections(collectionID *string, collectionName *string, collectionTopic *string, tenantID string, databaseName string) ([]*CollectionAndMetadata, error)
	DeleteCollectionByID(collectionID string) error
	Insert(in *Collection) error
	Update(in *Collection) error
	DeleteAll() error
}
