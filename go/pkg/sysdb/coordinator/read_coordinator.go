package coordinator

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/types"
	"gorm.io/gorm"
)

type ReadCoordinator struct {
	ctx     context.Context
	catalog ReadCatalog
}

func NewReadCoordinator(ctx context.Context, db *gorm.DB) (*ReadCoordinator, error) {
	s := &ReadCoordinator{
		ctx: ctx,
	}

	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	s.catalog = *NewReadTableCatalog(txnImpl, metaDomain)
	return s, nil
}

func (s *ReadCoordinator) GetCollectionsRead(ctx context.Context, collectionID types.UniqueID, collectionName *string, tenantID string, databaseName string) ([]*model.Collection, error) {
	return s.catalog.GetCollectionsRead(ctx, collectionID, collectionName, tenantID, databaseName)
}
