package model

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
)

type Collection struct {
	ID                         types.UniqueID
	Name                       string
	ConfigurationJsonStr       string
	SchemaStr                  *string
	Dimension                  *int32
	Metadata                   *CollectionMetadata[CollectionMetadataValueType]
	TenantID                   string
	DatabaseName               string
	Ts                         types.Timestamp
	LogPosition                int64
	Version                    int32
	RootCollectionID           *types.UniqueID
	LineageFileName            *string
	UpdatedAt                  types.Timestamp
	TotalRecordsPostCompaction uint64
	SizeBytesPostCompaction    uint64 // Note: This represents the size of the records off the log
	LastCompactionTimeSecs     uint64
	IsDeleted                  bool
	VersionFileName            string
	CreatedAt                  time.Time
	DatabaseId                 types.UniqueID
}

type CollectionToGc struct {
	ID              types.UniqueID
	TenantID        string
	Name            string
	VersionFilePath string
	LineageFilePath *string
}

type CreateCollection struct {
	ID                         types.UniqueID
	Name                       string
	ConfigurationJsonStr       string
	SchemaStr                  *string
	Dimension                  *int32
	Metadata                   *CollectionMetadata[CollectionMetadataValueType]
	GetOrCreate                bool
	TenantID                   string
	DatabaseName               string
	Ts                         types.Timestamp
	LogPosition                int64
	RootCollectionId           *string
	TotalRecordsPostCompaction uint64
	SizeBytesPostCompaction    uint64 // Note: This represents the size of the records off the log
	LastCompactionTimeSecs     uint64
}

type DeleteCollection struct {
	ID           types.UniqueID
	TenantID     string
	DatabaseName string
	Ts           types.Timestamp
}

type UpdateCollection struct {
	ID                      types.UniqueID
	Name                    *string
	Dimension               *int32
	Metadata                *CollectionMetadata[CollectionMetadataValueType]
	ResetMetadata           bool
	NewConfigurationJsonStr *string
	TenantID                string
	DatabaseName            string
	Ts                      types.Timestamp
}

type ForkCollection struct {
	SourceCollectionID                   types.UniqueID
	SourceCollectionLogCompactionOffset  uint64
	SourceCollectionLogEnumerationOffset uint64
	TargetCollectionID                   types.UniqueID
	TargetCollectionName                 string
}

type FlushCollectionCompaction struct {
	ID                         types.UniqueID
	TenantID                   string
	LogPosition                int64
	CurrentCollectionVersion   int32
	FlushSegmentCompactions    []*FlushSegmentCompaction
	TotalRecordsPostCompaction uint64
	SizeBytesPostCompaction    uint64
	SchemaStr                  *string
}

type FlushCollectionInfo struct {
	ID                       string
	CollectionVersion        int32
	TenantLastCompactionTime int64
	// Optional attached function fields (only populated for attached-function-based compactions)
	AttachedFunctionNextNonce        *uuid.UUID
	AttachedFunctionNextRun          *time.Time
	AttachedFunctionCompletionOffset *int64
}

func FilterCollection(collection *Collection, collectionID types.UniqueID, collectionName *string) bool {
	if collectionID != types.NilUniqueID() && collectionID != collection.ID {
		return false
	}
	if collectionName != nil && *collectionName != collection.Name {
		return false
	}
	return true
}
