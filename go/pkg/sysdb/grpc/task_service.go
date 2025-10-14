package grpc

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func (s *Server) CreateTask(ctx context.Context, req *coordinatorpb.CreateTaskRequest) (*coordinatorpb.CreateTaskResponse, error) {
	log.Info("CreateTask", zap.String("name", req.Name), zap.String("operator_name", req.OperatorName))

	res, err := s.coordinator.CreateTask(ctx, req)
	if err != nil {
		log.Error("CreateTask failed", zap.Error(err))
		if err == common.ErrTaskAlreadyExists {
			return nil, grpcutils.BuildAlreadyExistsGrpcError(err.Error())
		}
		return nil, err
	}

	return res, nil
}

func (s *Server) GetTaskByName(ctx context.Context, req *coordinatorpb.GetTaskByNameRequest) (*coordinatorpb.GetTaskByNameResponse, error) {
	log.Info("GetTaskByName", zap.String("input_collection_id", req.InputCollectionId), zap.String("task_name", req.TaskName))

	res, err := s.coordinator.GetTaskByName(ctx, req)
	if err != nil {
		log.Error("GetTaskByName failed", zap.Error(err))
		if err == common.ErrTaskNotFound {
			return nil, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return nil, err
	}

	return res, nil
}

func (s *Server) DeleteTask(ctx context.Context, req *coordinatorpb.DeleteTaskRequest) (*coordinatorpb.DeleteTaskResponse, error) {
	log.Info("DeleteTask", zap.String("input_collection_id", req.InputCollectionId), zap.String("task_name", req.TaskName))

	res, err := s.coordinator.DeleteTask(ctx, req)
	if err != nil {
		log.Error("DeleteTask failed", zap.Error(err))
		return nil, err
	}

	return res, nil
}

func (s *Server) GetOperators(ctx context.Context, req *coordinatorpb.GetOperatorsRequest) (*coordinatorpb.GetOperatorsResponse, error) {
	log.Info("GetOperators")

	res, err := s.coordinator.GetOperators(ctx, req)
	if err != nil {
		log.Error("GetOperators failed", zap.Error(err))
		return nil, err
	}

	return res, nil
}
