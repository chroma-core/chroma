package grpc

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type CollectionInfo struct {
	CollectionId string
	FirstLogId   int64
	FirstLogTs   int64
}

func (s *Server) PushLogs(ctx context.Context, req *logservicepb.PushLogsRequest) (*logservicepb.PushLogsResponse, error) {
	res := &logservicepb.PushLogsResponse{}
	collectionID, err := types.ToUniqueID(&req.CollectionId)
	err = grpcutils.BuildErrorForUUID(collectionID, "collection", err)
	if err != nil {
		return nil, err
	}
	var recordsContent [][]byte
	for _, record := range req.Records {
		data, err := proto.Marshal(record)
		if err != nil {
			log.Error("marshaling error", zap.Error(err))
			grpcError, err := grpcutils.BuildInvalidArgumentGrpcError("records", "marshaling error")
			if err != nil {
				return nil, err
			}
			return nil, grpcError
		}
		recordsContent = append(recordsContent, data)
	}
	recordCount, err := s.logService.PushLogs(ctx, collectionID, recordsContent)
	if err != nil {
		log.Error("error pushing logs", zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.RecordCount = int32(recordCount)
	log.Info("PushLogs success", zap.String("collectionID", req.CollectionId), zap.Int("recordCount", recordCount))
	return res, nil
}

func (s *Server) PullLogs(ctx context.Context, req *logservicepb.PullLogsRequest) (*logservicepb.PullLogsResponse, error) {
	res := &logservicepb.PullLogsResponse{}
	collectionID, err := types.ToUniqueID(&req.CollectionId)
	err = grpcutils.BuildErrorForUUID(collectionID, "collection", err)
	if err != nil {
		return nil, err
	}
	records := make([]*logservicepb.LogRecord, 0)
	recordLogs, err := s.logService.PullLogs(ctx, collectionID, req.GetStartFromOffset(), int(req.BatchSize), req.GetEndTimestamp())
	if err != nil {
		log.Error("error pulling logs", zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	for index := range recordLogs {
		record := &coordinatorpb.OperationRecord{}
		if err := proto.Unmarshal(*recordLogs[index].Record, record); err != nil {
			log.Error("Unmarshal error", zap.Error(err))
			grpcError, err := grpcutils.BuildInvalidArgumentGrpcError("records", "marshaling error")
			if err != nil {
				return nil, err
			}
			return nil, grpcError
		}
		recordLog := &logservicepb.LogRecord{
			LogOffset: recordLogs[index].LogOffset,
			Record:    record,
		}
		records = append(records, recordLog)
	}
	res.Records = records
	log.Info("PullLogs success", zap.String("collectionID", req.CollectionId), zap.Int("recordCount", len(records)))
	return res, nil
}

func (s *Server) GetAllCollectionInfoToCompact(ctx context.Context, req *logservicepb.GetAllCollectionInfoToCompactRequest) (*logservicepb.GetAllCollectionInfoToCompactResponse, error) {
	res := &logservicepb.GetAllCollectionInfoToCompactResponse{}
	res.AllCollectionInfo = make([]*logservicepb.CollectionInfo, 0)
	var recordLogs []*dbmodel.RecordLog
	recordLogs, err := s.logService.GetAllCollectionIDsToCompact()
	if err != nil {
		log.Error("error getting collection info", zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	for _, recordLog := range recordLogs {
		collectionInfo := &logservicepb.CollectionInfo{
			CollectionId:   *recordLog.CollectionID,
			FirstLogOffset: recordLog.LogOffset,
			FirstLogTs:     recordLog.Timestamp,
		}
		res.AllCollectionInfo = append(res.AllCollectionInfo, collectionInfo)
	}
	// print everything for now, we can make this smaller once
	log.Info("GetAllCollectionInfoToCompact success", zap.Any("collectionInfo", res.AllCollectionInfo))
	return res, nil
}
