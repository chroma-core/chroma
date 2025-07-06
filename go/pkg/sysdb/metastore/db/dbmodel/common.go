package dbmodel

import (
	"context"

	_ "ariga.io/atlas-provider-gorm/gormschema"
)

//go:generate mockery --name=IMetaDomain
type IMetaDomain interface {
	DatabaseDb(ctx context.Context) IDatabaseDb
	TenantDb(ctx context.Context) ITenantDb
	CollectionDb(ctx context.Context) ICollectionDb
	CollectionMetadataDb(ctx context.Context) ICollectionMetadataDb
	SegmentDb(ctx context.Context) ISegmentDb
	SegmentMetadataDb(ctx context.Context) ISegmentMetadataDb
}

//go:generate mockery --name=ITransaction
type ITransaction interface {
	Transaction(ctx context.Context, fn func(txCtx context.Context) error) error
}
