package grpccoordinator

import (
	"context"
	"errors"
	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func (s *Server) PushLogs(ctx context.Context, req *coordinatorpb.PushLogsRequest) (*coordinatorpb.PushLogsResponse, error) {
	res := &coordinatorpb.PushLogsResponse{}
	collectionID, err := types.ToUniqueID(&req.CollectionId)
	if err != nil {
		log.Error("collection id format error", zap.String("collection.id", req.CollectionId))
		return nil, common.ErrCollectionIDFormat
	}
	var recordsContent [][]byte
	for _, record := range req.Records {
		record.CollectionId = ""
		data, err := proto.Marshal(record)
		if err != nil {
			log.Error("marshaling error", zap.Error(err))
			return nil, common.ErrPushLogs
		}
		recordsContent = append(recordsContent, data)
	}
	recordCount, err := s.coordinator.PushLogs(ctx, collectionID, recordsContent)
	if err != nil {
		log.Error("error pushing logs", zap.Error(err))
		if errors.Is(err, common.ErrPushLogs) {
			res.Status = failResponseWithError(err, errorCode)
			return res, nil
		}
	}
	res.RecordCount = int32(recordCount)
	res.Status = setResponseStatus(successCode)
	log.Info("PushLogs success", zap.String("collectionID", req.CollectionId), zap.Int("recordCount", recordCount))
	return res, nil
}

func (s *Server) PullLogs(ctx context.Context, req *coordinatorpb.PullLogsRequest) (*coordinatorpb.PullLogsResponse, error) {
	res := &coordinatorpb.PullLogsResponse{}
	collectionID, err := types.ToUniqueID(&req.CollectionId)
	records := make([]*coordinatorpb.SubmitEmbeddingRecord, 0)
	if err != nil {
		log.Error("collection id format error", zap.String("collection.id", req.CollectionId))
		return nil, common.ErrCollectionIDFormat
	}
	recordLogs, err := s.coordinator.PullLogs(ctx, collectionID, req.GetStartFromId(), int(req.BatchSize))
	for index := range recordLogs {
		record := &coordinatorpb.SubmitEmbeddingRecord{}
		if err := proto.Unmarshal(*recordLogs[index].Record, record); err != nil {
			res.Status = failResponseWithError(err, errorCode)
			return res, nil
		}
		records = append(records, record)
	}
	res.Records = records
	res.Status = setResponseStatus(successCode)
	log.Info("PullLogs success", zap.String("collectionID", req.CollectionId), zap.Int("recordCount", len(records)))
	return res, nil
}
