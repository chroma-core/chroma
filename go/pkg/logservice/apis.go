package logservice

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
)

type (
	IRecordLog interface {
		common.Component
		PushLogs(ctx context.Context, collectionID types.UniqueID, recordContent [][]byte) (int, error)
		PullLogs(ctx context.Context, collectionID types.UniqueID, id int64, batchSize int, endTimestamp int64) ([]*dbmodel.RecordLog, error)
		GetAllCollectionIDsToCompact() ([]*dbmodel.RecordLog, error)
	}
)

var _ IRecordLog = &RecordLog{}

func (s *RecordLog) PushLogs(ctx context.Context, collectionID types.UniqueID, recordsContent [][]byte) (int, error) {
	return s.recordLogDb.PushLogs(collectionID, recordsContent)
}

func (s *RecordLog) PullLogs(ctx context.Context, collectionID types.UniqueID, id int64, batchSize int, endTimestamp int64) ([]*dbmodel.RecordLog, error) {
	return s.recordLogDb.PullLogs(collectionID, id, batchSize, endTimestamp)
}

func (s *RecordLog) GetAllCollectionIDsToCompact() ([]*dbmodel.RecordLog, error) {
	return s.recordLogDb.GetAllCollectionsToCompact()
}
