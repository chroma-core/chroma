package model

import (
	"github.com/chroma-core/chroma/go/pkg/types"
)

type Collection struct {
	ID           types.UniqueID
	Name         string
	Topic        string
	Dimension    *int32
	Metadata     *CollectionMetadata[CollectionMetadataValueType]
	TenantID     string
	DatabaseName string
	Ts           types.Timestamp
}

type CreateCollection struct {
	ID           types.UniqueID
	Name         string
	Topic        string
	Dimension    *int32
	Metadata     *CollectionMetadata[CollectionMetadataValueType]
	GetOrCreate  bool
	TenantID     string
	DatabaseName string
	Ts           types.Timestamp
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
	Topic         *string
	Dimension     *int32
	Metadata      *CollectionMetadata[CollectionMetadataValueType]
	ResetMetadata bool
	TenantID      string
	DatabaseName  string
	Ts            types.Timestamp
}

func FilterCollection(collection *Collection, collectionID types.UniqueID, collectionName *string, collectionTopic *string) bool {
	if collectionID != types.NilUniqueID() && collectionID != collection.ID {
		return false
	}
	if collectionName != nil && *collectionName != collection.Name {
		return false
	}
	if collectionTopic != nil && *collectionTopic != collection.Topic {
		return false
	}
	return true
}
