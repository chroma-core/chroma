package dao

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbcore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
)

type metaDomain struct{}

func NewMetaDomain() *metaDomain {
	return &metaDomain{}
}

func (*metaDomain) DatabaseDb(ctx context.Context) dbmodel.IDatabaseDb {
	return &databaseDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) TenantDb(ctx context.Context) dbmodel.ITenantDb {
	return &tenantDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) CollectionDb(ctx context.Context) dbmodel.ICollectionDb {
	return &collectionDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) CollectionMetadataDb(ctx context.Context) dbmodel.ICollectionMetadataDb {
	return &collectionMetadataDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) SegmentDb(ctx context.Context) dbmodel.ISegmentDb {
	return &segmentDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) SegmentMetadataDb(ctx context.Context) dbmodel.ISegmentMetadataDb {
	return &segmentMetadataDb{dbcore.GetDB(ctx)}
}

func (*metaDomain) NotificationDb(ctx context.Context) dbmodel.INotificationDb {
	return &notificationDb{dbcore.GetDB(ctx)}
}
