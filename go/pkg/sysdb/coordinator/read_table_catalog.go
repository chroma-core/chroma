package coordinator

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/chroma-core/chroma/go/shared/otel"
)

type ReadCatalog struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
}

func NewReadTableCatalog(txImpl dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain) *ReadCatalog {
	return &ReadCatalog{
		txImpl:     txImpl,
		metaDomain: metaDomain,
	}
}

func (tc *ReadCatalog) GetCollectionsRead(ctx context.Context, collectionID types.UniqueID, collectionName *string, tenantID string, databaseName string) ([]*model.Collection, error) {
	tracer := otel.Tracer
	if tracer != nil {
		_, span := tracer.Start(ctx, "Catalog.GetCollectionsRead")
		defer span.End()
	}

	collectionAndMetadataList, err := tc.metaDomain.CollectionDb(ctx).GetCollections(types.FromUniqueID(collectionID), collectionName, tenantID, databaseName, nil, nil)
	if err != nil {
		return nil, err
	}
	collections := convertCollectionToModel(collectionAndMetadataList)
	return collections, nil
}
