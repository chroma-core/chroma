package metastore

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
)

//go:generate mockery --name=Catalog
type Catalog interface {
	ResetState(ctx context.Context) error
	CreateCollection(ctx context.Context, collectionInfo *model.CreateCollection, ts types.Timestamp) (*model.Collection, error)
	GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error)
	DeleteCollection(ctx context.Context, collectionID types.UniqueID) error
	UpdateCollection(ctx context.Context, collectionInfo *model.UpdateCollection, ts types.Timestamp) (*model.Collection, error)
	CreateSegment(ctx context.Context, segmentInfo *model.CreateSegment, ts types.Timestamp) (*model.Segment, error)
	GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID, ts types.Timestamp) ([]*model.Segment, error)
	DeleteSegment(ctx context.Context, segmentID types.UniqueID) error
	UpdateSegment(ctx context.Context, segmentInfo *model.UpdateSegment, ts types.Timestamp) (*model.Segment, error)
}
