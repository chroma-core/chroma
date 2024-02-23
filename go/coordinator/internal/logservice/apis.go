package logservice

import (
	"context"
	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/types"
)

type (
	IRecordLog interface {
		common.Component
		PushLogs(ctx context.Context, collectionID types.UniqueID, recordContent [][]byte) (int, error)
	}
)

func (s *RecordLog) PushLogs(ctx context.Context, collectionID types.UniqueID, recordsContent [][]byte) (int, error) {
	return s.recordLogDb.PushLogs(collectionID, recordsContent)
}
