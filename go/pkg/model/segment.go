package model

import (
	"github.com/chroma-core/chroma/go/pkg/types"
)

type Segment struct {
	ID           types.UniqueID
	Type         string
	Scope        string
	Topic        *string
	CollectionID types.UniqueID
	Metadata     *SegmentMetadata[SegmentMetadataValueType]
	Ts           types.Timestamp
	FilePaths    map[string][]string
}

type CreateSegment struct {
	ID           types.UniqueID
	Type         string
	Scope        string
	Topic        *string
	CollectionID types.UniqueID
	Metadata     *SegmentMetadata[SegmentMetadataValueType]
	Ts           types.Timestamp
}

type UpdateSegment struct {
	ID              types.UniqueID
	Topic           *string
	ResetTopic      bool
	Collection      *string
	ResetCollection bool
	Metadata        *SegmentMetadata[SegmentMetadataValueType]
	ResetMetadata   bool
	Ts              types.Timestamp
}

type GetSegments struct {
	ID           types.UniqueID
	Type         *string
	Scope        *string
	Topic        *string
	CollectionID types.UniqueID
}

type FlushSegmentCompaction struct {
	ID        types.UniqueID
	FilePaths map[string][]string
}

func FilterSegments(segment *Segment, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) bool {
	if segmentID != types.NilUniqueID() && segment.ID != segmentID {
		return false
	}
	if segmentType != nil && segment.Type != *segmentType {
		return false
	}

	if scope != nil && segment.Scope != *scope {
		return false
	}

	if topic != nil && *segment.Topic != *topic {
		return false
	}

	if collectionID != types.NilUniqueID() && segment.CollectionID != collectionID {
		return false
	}
	return true
}
