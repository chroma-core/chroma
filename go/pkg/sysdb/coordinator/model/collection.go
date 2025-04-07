package model

import (
	"github.com/chroma-core/chroma/go/pkg/types"
)

type Collection struct {
	ID                         types.UniqueID
	Name                       string
	ConfigurationJsonStr       string
	Dimension                  *int32
	Metadata                   *CollectionMetadata[CollectionMetadataValueType]
	TenantID                   string
	DatabaseName               string
	Ts                         types.Timestamp
	LogPosition                int64
	Version                    int32
	UpdatedAt                  types.Timestamp
	TotalRecordsPostCompaction uint64
	SizeBytesPostCompaction    uint64 // Note: This represents the size of the records off the log
	LastCompactionTimeSecs     uint64
}

type CollectionToGc struct {
	ID              types.UniqueID
	TenantID        string
	Name            string
	VersionFilePath string
	LatestVersion   int64
}

type CreateCollection struct {
	ID                   types.UniqueID
	Name                 string
	ConfigurationJsonStr string
	Dimension            *int32
	Metadata             *CollectionMetadata[CollectionMetadataValueType]
	GetOrCreate          bool
	TenantID             string
	DatabaseName         string
	Ts                   types.Timestamp
}

type DeleteCollection struct {
	ID           types.UniqueID
	TenantID     string
	DatabaseName string
	Ts           types.Timestamp
}

type UpdateCollection struct {
	ID            types.UniqueID
	Name          *string
	Dimension     *int32
	Metadata      *CollectionMetadata[CollectionMetadataValueType]
	ResetMetadata bool
	TenantID      string
	DatabaseName  string
	Ts            types.Timestamp
}

type FlushCollectionCompaction struct {
	ID                         types.UniqueID
	TenantID                   string
	LogPosition                int64
	CurrentCollectionVersion   int32
	FlushSegmentCompactions    []*FlushSegmentCompaction
	TotalRecordsPostCompaction uint64
	SizeBytesPostCompaction    uint64
}

type FlushCollectionInfo struct {
	ID                       string
	CollectionVersion        int32
	TenantLastCompactionTime int64
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
