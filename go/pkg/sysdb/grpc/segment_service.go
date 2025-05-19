package grpc

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func (s *Server) CreateSegment(ctx context.Context, req *coordinatorpb.CreateSegmentRequest) (*coordinatorpb.CreateSegmentResponse, error) {
	segmentpb := req.GetSegment()

	res := &coordinatorpb.CreateSegmentResponse{}

	segment, err := convertProtoSegment(segmentpb)
	if err != nil {
		log.Error("CreateSegment failed. convert segment to model error", zap.Error(err), zap.String("request", segmentpb.String()))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	err = s.coordinator.CreateSegment(ctx, segment)
	if err != nil {
		log.Error("CreateSegment failed", zap.Error(err), zap.String("request", segmentpb.String()))
		if err == common.ErrSegmentUniqueConstraintViolation {
			return res, grpcutils.BuildAlreadyExistsGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	return res, nil
}

func (s *Server) GetSegments(ctx context.Context, req *coordinatorpb.GetSegmentsRequest) (*coordinatorpb.GetSegmentsResponse, error) {
	segmentID := req.Id
	segmentType := req.Type
	scope := req.Scope
	collectionID := req.Collection
	res := &coordinatorpb.GetSegmentsResponse{}

	parsedSegmentID, err := types.ToUniqueID(segmentID)
	if err != nil {
		log.Error("GetSegments failed. segment id format error", zap.Error(err), zap.String("request", req.String()))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	parsedCollectionID, err := types.ToUniqueID(&collectionID)
	if err != nil {
		log.Error("GetSegments failed. collection id format error", zap.Error(err), zap.String("request", req.String()))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	var scopeValue *string
	if scope == nil {
		scopeValue = nil
	} else {
		scopeString := scope.String()
		scopeValue = &scopeString
	}
	segments, err := s.coordinator.GetSegments(ctx, parsedSegmentID, segmentType, scopeValue, parsedCollectionID)
	if err != nil {
		log.Error("GetSegments failed.", zap.Error(err), zap.String("request", req.String()))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	segmentpbList := make([]*coordinatorpb.Segment, 0, len(segments))
	for _, segment := range segments {
		segmentpb := convertSegmentToProto(segment)
		segmentpbList = append(segmentpbList, segmentpb)
	}
	res.Segments = segmentpbList
	return res, nil
}

func (s *Server) DeleteSegment(ctx context.Context, req *coordinatorpb.DeleteSegmentRequest) (*coordinatorpb.DeleteSegmentResponse, error) {
	segmentID := req.GetId()
	res := &coordinatorpb.DeleteSegmentResponse{}
	parsedSegmentID, err := types.Parse(segmentID)
	if err != nil {
		log.Error("DeleteSegment failed. segment id format error", zap.Error(err), zap.String("request", req.String()))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	collectionID := req.GetCollection()
	parsedCollectionID, err := types.Parse(collectionID)
	if err != nil {
		log.Error("DeleteSegment failed. collection id format error", zap.Error(err), zap.String("request", req.String()))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	err = s.coordinator.DeleteSegment(ctx, parsedSegmentID, parsedCollectionID)
	if err != nil {
		log.Error("DeleteSegment failed", zap.Error(err), zap.String("request", req.String()))
		if err == common.ErrSegmentDeleteNonExistingSegment {
			return res, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	log.Info("DeleteSegment success", zap.String("request", req.String()))
	return res, nil
}

func (s *Server) UpdateSegment(ctx context.Context, req *coordinatorpb.UpdateSegmentRequest) (*coordinatorpb.UpdateSegmentResponse, error) {
	res := &coordinatorpb.UpdateSegmentResponse{}
	updateSegment := &model.UpdateSegment{
		ID:            types.MustParse(req.Id),
		ResetMetadata: req.GetResetMetadata(),
	}

	collection := req.GetCollection()
	if collection == "" {
		updateSegment.Collection = nil
	} else {
		updateSegment.Collection = &collection
	}
	metadata := req.GetMetadata()
	if metadata == nil {
		updateSegment.Metadata = nil
	} else {
		modelMetadata, err := convertSegmentMetadataToModel(metadata)
		if err != nil {
			log.Error("UpdateSegment failed", zap.Error(err), zap.String("request", req.String()))
			return res, grpcutils.BuildInternalGrpcError(err.Error())
		}
		updateSegment.Metadata = modelMetadata
	}
	_, err := s.coordinator.UpdateSegment(ctx, updateSegment)
	if err != nil {
		log.Error("UpdateSegment failed", zap.Error(err), zap.String("request", req.String()))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	return res, nil
}
