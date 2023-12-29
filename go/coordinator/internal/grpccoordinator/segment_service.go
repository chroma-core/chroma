package grpccoordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/proto/coordinatorpb"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func (s *Server) CreateSegment(ctx context.Context, req *coordinatorpb.CreateSegmentRequest) (*coordinatorpb.ChromaResponse, error) {
	segmentpb := req.GetSegment()

	res := &coordinatorpb.ChromaResponse{}

	segment, err := convertSegmentToModel(segmentpb)
	if err != nil {
		log.Error("convert segment to model error", zap.Error(err))
		res.Status = failResponseWithError(common.ErrSegmentIDFormat, errorCode)
		return res, nil
	}

	err = s.coordinator.CreateSegment(ctx, segment)
	if err != nil {
		if err == common.ErrSegmentUniqueConstraintViolation {
			log.Error("segment id already exist", zap.Error(err))
			res.Status = failResponseWithError(err, 409)
			return res, nil
		}
		log.Error("create segment error", zap.Error(err))
		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}
	res.Status = setResponseStatus(successCode)

	return res, nil
}

func (s *Server) GetSegments(ctx context.Context, req *coordinatorpb.GetSegmentsRequest) (*coordinatorpb.GetSegmentsResponse, error) {
	segmentID := req.Id
	segmentType := req.Type
	scope := req.Scope
	topic := req.Topic
	collectionID := req.Collection
	res := &coordinatorpb.GetSegmentsResponse{}

	parsedSegmentID, err := types.ToUniqueID(segmentID)
	if err != nil {
		log.Error("segment id format error", zap.String("segment.id", *segmentID))
		res.Status = failResponseWithError(common.ErrSegmentIDFormat, errorCode)
		return res, nil
	}

	parsedCollectionID, err := types.ToUniqueID(collectionID)
	if err != nil {
		log.Error("collection id format error", zap.String("collectionpd.id", *collectionID))
		res.Status = failResponseWithError(common.ErrCollectionIDFormat, errorCode)
		return res, nil
	}
	var scopeValue *string
	if scope == nil {
		scopeValue = nil
	} else {
		scopeString := scope.String()
		scopeValue = &scopeString
	}
	segments, err := s.coordinator.GetSegments(ctx, parsedSegmentID, segmentType, scopeValue, topic, parsedCollectionID)
	if err != nil {
		log.Error("get segments error", zap.Error(err))
		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}

	segmentpbList := make([]*coordinatorpb.Segment, 0, len(segments))
	for _, segment := range segments {
		segmentpb := convertSegmentToProto(segment)
		segmentpbList = append(segmentpbList, segmentpb)
	}
	res.Segments = segmentpbList
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) DeleteSegment(ctx context.Context, req *coordinatorpb.DeleteSegmentRequest) (*coordinatorpb.ChromaResponse, error) {
	segmentID := req.GetId()
	res := &coordinatorpb.ChromaResponse{}
	parsedSegmentID, err := types.Parse(segmentID)
	if err != nil {
		log.Error(err.Error(), zap.String("segment.id", segmentID))
		res.Status = failResponseWithError(common.ErrSegmentIDFormat, errorCode)
		return res, nil
	}
	err = s.coordinator.DeleteSegment(ctx, parsedSegmentID)
	if err != nil {
		if err == common.ErrSegmentDeleteNonExistingSegment {
			log.Error(err.Error(), zap.String("segment.id", segmentID))
			res.Status = failResponseWithError(err, 404)
			return res, nil
		}
		log.Error(err.Error(), zap.String("segment.id", segmentID))
		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) UpdateSegment(ctx context.Context, req *coordinatorpb.UpdateSegmentRequest) (*coordinatorpb.ChromaResponse, error) {
	res := &coordinatorpb.ChromaResponse{}
	updateSegment := &model.UpdateSegment{
		ID:              types.MustParse(req.Id),
		ResetTopic:      req.GetResetTopic(),
		ResetCollection: req.GetResetCollection(),
		ResetMetadata:   req.GetResetMetadata(),
	}
	topic := req.GetTopic()
	if topic == "" {
		updateSegment.Topic = nil
	} else {
		updateSegment.Topic = &topic
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
			log.Error("convert segment metadata to model error", zap.Error(err))
			res.Status = failResponseWithError(err, errorCode)
			return res, nil
		}
		updateSegment.Metadata = modelMetadata
	}
	_, err := s.coordinator.UpdateSegment(ctx, updateSegment)
	if err != nil {
		log.Error("update segment error", zap.Error(err))
		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}
	res.Status = setResponseStatus(successCode)
	return res, nil
}
