package server

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/log/repository"
	log "github.com/chroma-core/chroma/go/pkg/log/store/db"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/chroma-core/chroma/go/pkg/types"
	"google.golang.org/protobuf/proto"
)

type logServer struct {
	logservicepb.UnimplementedLogServiceServer
	lr *repository.LogRepository
}

func (s *logServer) PushLogs(ctx context.Context, req *logservicepb.PushLogsRequest) (res *logservicepb.PushLogsResponse, err error) {
	var collectionID types.UniqueID
	collectionID, err = types.ToUniqueID(&req.CollectionId)
	if err != nil {
		// TODO HANDLE ERROR
		return
	}
	var recordsContent [][]byte
	for _, record := range req.Records {
		var data []byte
		data, err = proto.Marshal(record)
		if err != nil {
			// TODO HANDLE ERROR
			return
		}
		recordsContent = append(recordsContent, data)
	}
	var recordCount int64
	recordCount, err = s.lr.InsertRecords(ctx, collectionID.String(), recordsContent)
	if err != nil {
		return
	}
	res = &logservicepb.PushLogsResponse{
		RecordCount: int32(recordCount),
	}
	return
}

func (s *logServer) ScoutLogs(ctx context.Context, req *logservicepb.ScoutLogsRequest) (res *logservicepb.ScoutLogsResponse, err error) {
	var collectionID types.UniqueID
	collectionID, err = types.ToUniqueID(&req.CollectionId)
	if err != nil {
		return
	}
	var limit int64
	_, limit, err = s.lr.GetBoundsForCollection(ctx, collectionID.String())
	if err != nil {
		return
	}
	// +1 to convert from the (] bound to a [) bound.
	res = &logservicepb.ScoutLogsResponse{
		FirstUninsertedRecordOffset: int64(limit + 1),
	}
	return
}

func (s *logServer) PullLogs(ctx context.Context, req *logservicepb.PullLogsRequest) (res *logservicepb.PullLogsResponse, err error) {
	var collectionID types.UniqueID
	collectionID, err = types.ToUniqueID(&req.CollectionId)
	if err != nil {
		return
	}
	var records []log.RecordLog
	records, err = s.lr.PullRecords(ctx, collectionID.String(), req.StartFromOffset, int(req.BatchSize), req.EndTimestamp)
	if err != nil {
		return
	}
	res = &logservicepb.PullLogsResponse{
		Records: make([]*logservicepb.LogRecord, len(records)),
	}

	for index := range records {
		record := &coordinatorpb.OperationRecord{}
		if err = proto.Unmarshal(records[index].Record, record); err != nil {
			return
		}
		res.Records[index] = &logservicepb.LogRecord{
			LogOffset: records[index].Offset,
			Record:    record,
		}
	}
	return
}

func (s *logServer) ForkLogs(ctx context.Context, req *logservicepb.ForkLogsRequest) (res *logservicepb.ForkLogsResponse, err error) {
	var sourceCollectionID types.UniqueID
	var targetCollectionID types.UniqueID
	sourceCollectionID, err = types.ToUniqueID(&req.SourceCollectionId)
	if err != nil {
		return
	}
	targetCollectionID, err = types.ToUniqueID(&req.TargetCollectionId)
	if err != nil {
		return
	}

	compactionOffset, enumerationOffset, err := s.lr.ForkRecords(ctx, sourceCollectionID.String(), targetCollectionID.String())
	if err != nil {
		return
	}

	res = &logservicepb.ForkLogsResponse{
		CompactionOffset:  compactionOffset,
		EnumerationOffset: enumerationOffset,
	}
	return
}

func (s *logServer) GetAllCollectionInfoToCompact(ctx context.Context, req *logservicepb.GetAllCollectionInfoToCompactRequest) (res *logservicepb.GetAllCollectionInfoToCompactResponse, err error) {
	var collectionToCompact []log.GetAllCollectionsToCompactRow
	collectionToCompact, err = s.lr.GetAllCollectionInfoToCompact(ctx, req.MinCompactionSize)
	if err != nil {
		return
	}
	res = &logservicepb.GetAllCollectionInfoToCompactResponse{
		AllCollectionInfo: make([]*logservicepb.CollectionInfo, len(collectionToCompact)),
	}
	for index := range collectionToCompact {
		res.AllCollectionInfo[index] = &logservicepb.CollectionInfo{
			CollectionId:   collectionToCompact[index].CollectionID,
			FirstLogOffset: collectionToCompact[index].Offset,
			FirstLogTs:     int64(collectionToCompact[index].Timestamp),
		}
	}
	return
}

func (s *logServer) UpdateCollectionLogOffset(ctx context.Context, req *logservicepb.UpdateCollectionLogOffsetRequest) (res *logservicepb.UpdateCollectionLogOffsetResponse, err error) {
	var collectionID types.UniqueID
	collectionID, err = types.ToUniqueID(&req.CollectionId)
	if err != nil {
		return
	}
	err = s.lr.UpdateCollectionCompactionOffsetPosition(ctx, collectionID.String(), req.LogOffset)
	if err != nil {
		return
	}
	res = &logservicepb.UpdateCollectionLogOffsetResponse{}
	return
}

func (s *logServer) PurgeDirtyForCollection(ctx context.Context, req *logservicepb.PurgeDirtyForCollectionRequest) (res *logservicepb.PurgeDirtyForCollectionResponse, err error) {
	// no-op for now
	return
}

func NewLogServer(lr *repository.LogRepository) logservicepb.LogServiceServer {
	return &logServer{
		lr: lr,
	}
}
