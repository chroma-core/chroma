package dbmodel

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/types"
)

type Collection struct {
	ID                         string          `gorm:"id;primaryKey"`
	Name                       *string         `gorm:"name;not null;index:idx_name,unique;"`
	ConfigurationJsonStr       *string         `gorm:"configuration_json_str"`
	SchemaStr                  *string         `gorm:"schema_str"`
	Dimension                  *int32          `gorm:"dimension"`
	DatabaseID                 string          `gorm:"database_id;not null;index:idx_name,unique;"`
	Ts                         types.Timestamp `gorm:"ts;type:bigint;default:0"`
	IsDeleted                  bool            `gorm:"is_deleted;type:bool;default:false"`
	CreatedAt                  time.Time       `gorm:"created_at;type:timestamp;not null;default:current_timestamp"`
	UpdatedAt                  time.Time       `gorm:"updated_at;type:timestamp;not null;default:current_timestamp"`
	LogPosition                int64           `gorm:"log_position;default:0"`
	Version                    int32           `gorm:"version;default:0"`
	VersionFileName            string          `gorm:"version_file_name"`
	RootCollectionId           *string         `gorm:"column:root_collection_id"`
	LineageFileName            *string         `gorm:"column:lineage_file_name"`
	TotalRecordsPostCompaction uint64          `gorm:"total_records_post_compaction;default:0"`
	SizeBytesPostCompaction    uint64          `gorm:"size_bytes_post_compaction;default:0"`
	LastCompactionTimeSecs     uint64          `gorm:"last_compaction_time_secs;default:0"`
	NumVersions                uint32          `gorm:"num_versions;type:integer;default:0"`
	OldestVersionTs            time.Time       `gorm:"oldest_version_ts;type:timestamp"`
	Tenant                     string          `gorm:"tenant"`
}

type CollectionToGc struct {
	ID              string    `gorm:"id;primaryKey"`
	TenantID        string    `gorm:"tenant_id;not null;index:idx_tenant_id"`
	Name            string    `gorm:"name;not null;index:idx_name,unique;"`
	VersionFileName string    `gorm:"version_file_name"`
	OldestVersionTs time.Time `gorm:"oldest_version_ts;type:timestamp"`
	LineageFileName *string   `gorm:"lineage_file_name"`
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
	GetCollections(collectionIDs []string, collectionName *string, tenantID string, databaseName string, limit *int32, offset *int32, includeSoftDeleted bool) ([]*CollectionAndMetadata, error)
	GetCollectionEntries(id *string, name *string, tenantID string, databaseName string, limit *int32, offset *int32) ([]*CollectionAndMetadata, error)
	GetCollectionByResourceName(tenantResourceName string, databaseName string, collectionName string) (*CollectionAndMetadata, error)
	CountCollections(tenantID string, databaseName *string) (uint64, error)
	DeleteCollectionByID(collectionID string) (int, error)
	GetSoftDeletedCollections(collectionID *string, tenantID string, databaseName string, limit int32) ([]*CollectionAndMetadata, error)
	Insert(in *Collection) error
	InsertOnConflictDoNothing(in *Collection) (didInsert bool, err error)
	Update(in *Collection) error
	DeleteAll() error
	UpdateLogPositionVersionTotalRecordsAndLogicalSize(collectionID string, logPosition int64, currentCollectionVersion int32, totalRecordsPostCompaction uint64, sizeBytesPostCompaction uint64, lastCompactionTimeSecs uint64, tenant string, schemaStr *string) (int32, error)
	UpdateLogPositionAndVersionInfo(collectionID string, logPosition int64, currentCollectionVersion int32, currentVersionFilePath string, newCollectionVersion int32, newVersionFilePath string, totalRecordsPostCompaction uint64,
		sizeBytesPostCompaction uint64, lastCompactionTimeSecs uint64, numVersions uint64, schemaStr *string) (int64, error)
	GetCollectionWithoutMetadata(collectionID *string, databaseName *string, softDeletedFlag *bool) (*Collection, error)
	GetCollectionSize(collectionID string) (uint64, error)
	ListCollectionsToGc(cutoffTimeSecs *uint64, limit *uint64, tenantID *string, minVersionsIfAlive *uint64) ([]*CollectionToGc, error)
	UpdateVersionRelatedFields(collectionID, existingVersionFileName, newVersionFileName string, oldestVersionTs *time.Time, numActiveVersions *int) (int64, error)
	LockCollection(collectionID string) (*bool, error)
	UpdateCollectionLineageFilePath(collectionID string, currentLineageFilePath *string, newLineageFilePath string) error
	BatchGetCollectionVersionFilePaths(collectionIDs []string) (map[string]string, error)
	BatchGetCollectionSoftDeleteStatus(collectionIDs []string) (map[string]bool, error)
}
