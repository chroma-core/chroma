package grpc

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func (s *Server) AttachFunction(ctx context.Context, req *coordinatorpb.AttachFunctionRequest) (*coordinatorpb.AttachFunctionResponse, error) {
	log.Info("AttachFunction", zap.String("name", req.Name), zap.String("function_name", req.FunctionName))

	res, err := s.coordinator.AttachFunction(ctx, req)
	if err != nil {
		log.Error("AttachFunction failed", zap.Error(err))
		if err == common.ErrAttachedFunctionAlreadyExists {
			return nil, grpcutils.BuildAlreadyExistsGrpcError(err.Error())
		}
		return nil, err
	}

	return res, nil
}

func (s *Server) GetAttachedFunctionByName(ctx context.Context, req *coordinatorpb.GetAttachedFunctionByNameRequest) (*coordinatorpb.GetAttachedFunctionByNameResponse, error) {
	log.Info("GetAttachedFunctionByName", zap.String("input_collection_id", req.InputCollectionId), zap.String("name", req.Name))

	res, err := s.coordinator.GetAttachedFunctionByName(ctx, req)
	if err != nil {
		log.Error("GetAttachedFunctionByName failed", zap.Error(err))
		if err == common.ErrAttachedFunctionNotFound {
			return nil, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return nil, err
	}

	return res, nil
}

func (s *Server) ListAttachedFunctions(ctx context.Context, req *coordinatorpb.ListAttachedFunctionsRequest) (*coordinatorpb.ListAttachedFunctionsResponse, error) {
	log.Info("ListAttachedFunctions", zap.String("input_collection_id", req.InputCollectionId))

	res, err := s.coordinator.ListAttachedFunctions(ctx, req)
	if err != nil {
		log.Error("ListAttachedFunctions failed", zap.Error(err))
		return nil, err
	}

	return res, nil
}

func (s *Server) GetAttachedFunctionByUuid(ctx context.Context, req *coordinatorpb.GetAttachedFunctionByUuidRequest) (*coordinatorpb.GetAttachedFunctionByUuidResponse, error) {
	log.Info("GetAttachedFunctionByUuid", zap.String("id", req.Id))

	res, err := s.coordinator.GetAttachedFunctionByUuid(ctx, req)
	if err != nil {
		log.Error("GetAttachedFunctionByUuid failed", zap.Error(err))
		if err == common.ErrAttachedFunctionNotFound {
			return nil, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return nil, err
	}

	return res, nil
}

func (s *Server) DetachFunction(ctx context.Context, req *coordinatorpb.DetachFunctionRequest) (*coordinatorpb.DetachFunctionResponse, error) {
	log.Info("DetachFunction", zap.String("attached_function_id", req.AttachedFunctionId))

	res, err := s.coordinator.DetachFunction(ctx, req)
	if err != nil {
		log.Error("DetachFunction failed", zap.Error(err))
		return nil, err
	}

	return res, nil
}

func (s *Server) GetFunctions(ctx context.Context, req *coordinatorpb.GetFunctionsRequest) (*coordinatorpb.GetFunctionsResponse, error) {
	log.Info("GetFunctions")

	res, err := s.coordinator.GetFunctions(ctx, req)
	if err != nil {
		log.Error("GetFunctions failed", zap.Error(err))
		return nil, err
	}

	return res, nil
}

func (s *Server) CleanupExpiredPartialAttachedFunctions(ctx context.Context, req *coordinatorpb.CleanupExpiredPartialAttachedFunctionsRequest) (*coordinatorpb.CleanupExpiredPartialAttachedFunctionsResponse, error) {
	log.Info("CleanupExpiredPartialAttachedFunctions", zap.Uint64("max_age_seconds", req.MaxAgeSeconds))

	res, err := s.coordinator.CleanupExpiredPartialAttachedFunctions(ctx, req)
	if err != nil {
		log.Error("CleanupExpiredPartialAttachedFunctions failed", zap.Error(err))
		return nil, err
	}

	log.Info("CleanupExpiredPartialAttachedFunctions succeeded", zap.Uint64("cleaned_up_count", res.CleanedUpCount))
	return res, nil
}

func (s *Server) GetSoftDeletedAttachedFunctions(ctx context.Context, req *coordinatorpb.GetSoftDeletedAttachedFunctionsRequest) (*coordinatorpb.GetSoftDeletedAttachedFunctionsResponse, error) {
	log.Info("GetSoftDeletedAttachedFunctions", zap.Time("cutoff_time", req.CutoffTime.AsTime()), zap.Int32("limit", req.Limit))

	res, err := s.coordinator.GetSoftDeletedAttachedFunctions(ctx, req)
	if err != nil {
		log.Error("GetSoftDeletedAttachedFunctions failed", zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}

	log.Info("GetSoftDeletedAttachedFunctions succeeded", zap.Int("count", len(res.AttachedFunctions)))
	return res, nil
}

func (s *Server) FinishCreateAttachedFunction(ctx context.Context, req *coordinatorpb.FinishCreateAttachedFunctionRequest) (*coordinatorpb.FinishCreateAttachedFunctionResponse, error) {
	log.Info("FinishCreateAttachedFunction", zap.String("id", req.Id))

	res, err := s.coordinator.FinishCreateAttachedFunction(ctx, req)
	if err != nil {
		log.Error("FinishCreateAttachedFunction failed", zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}

	log.Info("FinishCreateAttachedFunction succeeded", zap.String("id", req.Id))
	return res, nil
}

func (s *Server) FinishAttachedFunctionDeletion(ctx context.Context, req *coordinatorpb.FinishAttachedFunctionDeletionRequest) (*coordinatorpb.FinishAttachedFunctionDeletionResponse, error) {
	log.Info("FinishAttachedFunctionDeletion", zap.String("id", req.AttachedFunctionId))

	res, err := s.coordinator.FinishAttachedFunctionDeletion(ctx, req)
	if err != nil {
		log.Error("FinishAttachedFunctionDeletion failed", zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}

	log.Info("FinishAttachedFunctionDeletion succeeded", zap.String("id", req.AttachedFunctionId))
	return res, nil
}
