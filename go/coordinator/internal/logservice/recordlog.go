package logservice

import (
	"context"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dao"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/pingcap/log"
)

var _ IRecordLog = (*RecordLog)(nil)

type RecordLog struct {
	ctx          context.Context
	recordLogDb  dbmodel.IRecordLogDb
	arrowPool    memory.Allocator
	recordSchema *arrow.Schema
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
	s.arrowPool = memory.NewGoAllocator()
	s.recordSchema = arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.BinaryTypes.String},
			{Name: "vector", Type: arrow.StructOf(
				[]arrow.Field{
					{Name: "dimension", Type: arrow.PrimitiveTypes.Int32},
					{Name: "vector", Type: arrow.BinaryTypes.String},
					{Name: "scalarEncoding", Type: arrow.PrimitiveTypes.Int32},
				}...)},
			{Name: "updateMetadata", Type: arrow.PrimitiveTypes.Float64},
			{Name: "operation", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)
	return nil
}

func (s *RecordLog) Stop() error {
	log.Info("RecordLog stop")
	return nil
}
