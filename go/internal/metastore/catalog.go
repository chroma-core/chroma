package metastore

import (
	"context"

	"github.com/chroma-core/chroma/go/internal/model"
	"github.com/chroma-core/chroma/go/internal/types"
)

// Catalog defines methods for system catalog
//
//go:generate mockery --name=Catalog
type Catalog interface {
	ResetState(ctx context.Context) error
	CreateCollection(ctx context.Context, createCollection *model.CreateCollection, ts types.Timestamp) (*model.Collection, error)
	GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string, tenantID string, databaseName string) ([]*model.Collection, error)
	DeleteCollection(ctx context.Context, deleteCollection *model.DeleteCollection) error
	UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection, ts types.Timestamp) (*model.Collection, error)
	CreateSegment(ctx context.Context, createSegment *model.CreateSegment, ts types.Timestamp) (*model.Segment, error)
	GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID, ts types.Timestamp) ([]*model.Segment, error)
	DeleteSegment(ctx context.Context, segmentID types.UniqueID) error
	UpdateSegment(ctx context.Context, segmentInfo *model.UpdateSegment, ts types.Timestamp) (*model.Segment, error)
	CreateDatabase(ctx context.Context, createDatabase *model.CreateDatabase, ts types.Timestamp) (*model.Database, error)
	GetDatabases(ctx context.Context, getDatabase *model.GetDatabase, ts types.Timestamp) (*model.Database, error)
	GetAllDatabases(ctx context.Context, ts types.Timestamp) ([]*model.Database, error)
	CreateTenant(ctx context.Context, createTenant *model.CreateTenant, ts types.Timestamp) (*model.Tenant, error)
	GetTenants(ctx context.Context, getTenant *model.GetTenant, ts types.Timestamp) (*model.Tenant, error)
	GetAllTenants(ctx context.Context, ts types.Timestamp) ([]*model.Tenant, error)
}
