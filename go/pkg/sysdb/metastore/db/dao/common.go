package dao

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
)

type MetaDomain struct{}

func NewMetaDomain() *MetaDomain {
	return &MetaDomain{}
}

func (*MetaDomain) DatabaseDb(ctx context.Context) dbmodel.IDatabaseDb {
	return &databaseDb{dbcore.GetDB(ctx)}
}

func (*MetaDomain) TenantDb(ctx context.Context) dbmodel.ITenantDb {
	return &tenantDb{dbcore.GetDB(ctx)}
}

func (*MetaDomain) CollectionDb(ctx context.Context) dbmodel.ICollectionDb {
	return &collectionDb{dbcore.GetDB(ctx)}
}

func (*MetaDomain) CollectionMetadataDb(ctx context.Context) dbmodel.ICollectionMetadataDb {
	return &collectionMetadataDb{dbcore.GetDB(ctx)}
}

func (*MetaDomain) SegmentDb(ctx context.Context) dbmodel.ISegmentDb {
	return &segmentDb{dbcore.GetDB(ctx)}
}

func (*MetaDomain) SegmentMetadataDb(ctx context.Context) dbmodel.ISegmentMetadataDb {
	return &segmentMetadataDb{dbcore.GetDB(ctx)}
}
