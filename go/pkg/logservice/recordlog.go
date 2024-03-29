package logservice

import (
	"context"
	dao2 "github.com/chroma-core/chroma/go/pkg/logservice/db/dao"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dao"
	"github.com/pingcap/log"
)

var _ IRecordLog = (*RecordLog)(nil)

type RecordLog struct {
	ctx           context.Context
	recordLogDb   dao2.IRecordLogDb
	collectionLog dao2.ICollectionPositionDb
}

func NewLogService(ctx context.Context) (*RecordLog, error) {
	s := &RecordLog{
		ctx:         ctx,
		recordLogDb: dao.NewMetaDomain().RecordLogDb(ctx),
	}
	return s, nil
}

func (s *RecordLog) Start() error {
	log.Info("RecordLog start")
	return nil
}

func (s *RecordLog) Stop() error {
	log.Info("RecordLog stop")
	return nil
}
