package dbmodel

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/types"
)

type Collection struct {
	ID                         string          `gorm:"id;primaryKey"`
	Name                       *string         `gorm:"name;not null;index:idx_name,unique;"`
	ConfigurationJsonStr       *string         `gorm:"configuration_json_str"`
	Dimension                  *int32          `gorm:"dimension"`
	DatabaseID                 string          `gorm:"database_id;not null;index:idx_name,unique;"`
	Ts                         types.Timestamp `gorm:"ts;type:bigint;default:0"`
	IsDeleted                  bool            `gorm:"is_deleted;type:bool;default:false"`
	CreatedAt                  time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt                  time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
	LogPosition                int64           `gorm:"log_position;default:0"`
	Version                    int32           `gorm:"version;default:0"`
	VersionFileName            string          `gorm:"version_file_name"`
	TotalRecordsPostCompaction uint64          `gorm:"total_records_post_compaction;default:0"`
}

type CollectionToGc struct {
	ID      string `gorm:"id;primaryKey"`
	Name    string `gorm:"name;not null;index:idx_name,unique;"`
	Version int32  `gorm:"version;default:0"`
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
	GetCollections(collectionID *string, collectionName *string, tenantID string, databaseName string, limit *int32, offset *int32) ([]*CollectionAndMetadata, error)
	DeleteCollectionByID(collectionID string) (int, error)
	GetSoftDeletedCollections(collectionID *string, tenantID string, databaseName string, limit int32) ([]*CollectionAndMetadata, error)
	Insert(in *Collection) error
	Update(in *Collection) error
	DeleteAll() error
	UpdateLogPositionVersionAndTotalRecords(collectionID string, logPosition int64, currentCollectionVersion int32, totalRecordsPostCompaction uint64) (int32, error)
	UpdateLogPositionAndVersionInfo(collectionID string, logPosition int64, currentCollectionVersion int32, currentVersionFileName string, newCollectionVersion int32, newVersionFileName string) (int64, error)
	GetCollectionEntry(collectionID *string, databaseName *string) (*Collection, error)
	GetCollectionSize(collectionID string) (uint64, error)
	ListCollectionsToGc() ([]*CollectionToGc, error)
}
