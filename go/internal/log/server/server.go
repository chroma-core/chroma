package server

import (
	"context"
	log "github.com/chroma-core/chroma/go/database/log/db"
	"github.com/chroma-core/chroma/go/internal/log/repository"
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
		// TODO WHY SET COLLECTION ID TO EMPTY STRING?
		record.CollectionId = ""
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

func (s *logServer) PullLogs(ctx context.Context, req *logservicepb.PullLogsRequest) (res *logservicepb.PullLogsResponse, err error) {
	var collectionID types.UniqueID
	collectionID, err = types.ToUniqueID(&req.CollectionId)
	if err != nil {
		return
	}
	records, err := s.lr.PullRecords(ctx, collectionID.String(), req.StartFromId, int(req.BatchSize))
	if err != nil {
		return
	}
	res = &logservicepb.PullLogsResponse{
		Records: make([]*logservicepb.RecordLog, len(records)),
	}

	for index := range records {
		record := &coordinatorpb.SubmitEmbeddingRecord{}
		if err = proto.Unmarshal(records[index].Record, record); err != nil {
			return
		}
		res.Records[index] = &logservicepb.RecordLog{
			LogId:  records[index].ID,
			Record: record,
		}
	}
	return
}

func (s *logServer) GetAllCollectionInfoToCompact(ctx context.Context, req *logservicepb.GetAllCollectionInfoToCompactRequest) (res *logservicepb.GetAllCollectionInfoToCompactResponse, err error) {
	var collectionToCompact []log.GetAllCollectionsToCompactRow
	collectionToCompact, err = s.lr.GetAllCollectionInfoToCompact(ctx)
	if err != nil {
		return
	}
	res = &logservicepb.GetAllCollectionInfoToCompactResponse{
		AllCollectionInfo: make([]*logservicepb.CollectionInfo, len(collectionToCompact)),
	}
	for index := range collectionToCompact {
		res.AllCollectionInfo[index] = &logservicepb.CollectionInfo{
			CollectionId: collectionToCompact[index].CollectionID,
			FirstLogId:   collectionToCompact[index].ID,
			FirstLogIdTs: int64(collectionToCompact[index].Timestamp),
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
	err = s.lr.UpdateCollectionPosition(ctx, collectionID.String(), req.LogId)
	if err != nil {
		return
	}
	res = &logservicepb.UpdateCollectionLogOffsetResponse{}
	return
}

func NewLogServer(lr *repository.LogRepository) logservicepb.LogServiceServer {
	return &logServer{
		lr: lr,
	}
}
