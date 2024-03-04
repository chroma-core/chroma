package logservice

import (
	"context"
	"github.com/chroma-core/chroma/go/internal/common"
	"github.com/chroma-core/chroma/go/internal/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/internal/types"
)

type (
	IRecordLog interface {
		common.Component
		PushLogs(ctx context.Context, collectionID types.UniqueID, recordContent [][]byte) (int, error)
		PullLogs(ctx context.Context, collectionID types.UniqueID, id int64, batchSize int) ([]*dbmodel.RecordLog, error)
		GetAllCollectionIDsToCompact() ([]*dbmodel.RecordLog, error)
	}
)

func (s *RecordLog) PushLogs(ctx context.Context, collectionID types.UniqueID, recordsContent [][]byte) (int, error) {
	return s.recordLogDb.PushLogs(collectionID, recordsContent)
}

func (s *RecordLog) PullLogs(ctx context.Context, collectionID types.UniqueID, id int64, batchSize int) ([]*dbmodel.RecordLog, error) {
	return s.recordLogDb.PullLogs(collectionID, id, batchSize)
}

func (s *RecordLog) GetAllCollectionIDsToCompact() ([]*dbmodel.RecordLog, error) {
	return s.recordLogDb.GetAllCollectionsToCompact()
}
