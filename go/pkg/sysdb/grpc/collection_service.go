package grpc

import (
	"context"
	"encoding/json"
	"math"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *Server) ResetState(context.Context, *emptypb.Empty) (*coordinatorpb.ResetStateResponse, error) {
	log.Info("reset state")
	res := &coordinatorpb.ResetStateResponse{}
	err := s.coordinator.ResetState(context.Background())
	if err != nil {
		log.Error("error resetting state", zap.Error(err))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	return res, nil
}

// Cases for get_or_create

// Case 0
// new_metadata is none, coll is an existing collection
// get_or_create should return the existing collection with existing metadata
// Essentially - an update with none is a no-op

// Case 1
// new_metadata is none, coll is a new collection
// get_or_create should create a new collection with the metadata of None

// Case 2
// new_metadata is not none, coll is an existing collection
// get_or_create should return the existing collection with updated metadata

// Case 3
// new_metadata is not none, coll is a new collection
// get_or_create should create a new collection with the new metadata, ignoring
// the metdata of in the input coll.

// The fact that we ignore the metadata of the generated collections is a
// bit weird, but it is the easiest way to excercise all cases

// NOTE: In current implementation, we do not support updating the metadata of an existing collection via this RPC.
//
// The call will fail if the collection already exists. Leaving the comments about cases 0,1,2,3 above for future reference.
// Refer to these issues for more context:
// https://github.com/chroma-core/chroma/issues/2390
// https://github.com/chroma-core/chroma/pull/2810
func (s *Server) CreateCollection(ctx context.Context, req *coordinatorpb.CreateCollectionRequest) (*coordinatorpb.CreateCollectionResponse, error) {
	res := &coordinatorpb.CreateCollectionResponse{}

	log.Info("CreateCollectionRequest", zap.Any("request", req))

	createCollection, err := convertToCreateCollectionModel(req)
	if err != nil {
		log.Error("CreateCollection failed. error converting to create collection model", zap.Error(err), zap.String("collection_id", req.Id), zap.String("collection_name", req.Name))
		res.Collection = &coordinatorpb.Collection{
			Id:                   req.Id,
			Name:                 req.Name,
			ConfigurationJsonStr: req.ConfigurationJsonStr,
			SchemaStr:            req.SchemaStr,
			Dimension:            req.Dimension,
			Metadata:             req.Metadata,
			Tenant:               req.Tenant,
			Database:             req.Database,
		}
		res.Created = false
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	// Convert the request segments to create segment models
	createSegments := []*model.Segment{}
	for _, segment := range req.Segments {
		createSegment, err := convertProtoSegment(segment)
		if err != nil {
			log.Error("Error in creating segments for the collection", zap.Error(err))
			res.Collection = nil // We don't need to set the collection in case of error
			res.Created = false
			if err == common.ErrSegmentUniqueConstraintViolation {
				log.Error("segment id already exist", zap.Error(err))
				return res, grpcutils.BuildAlreadyExistsGrpcError(err.Error())
			}
			return res, grpcutils.BuildInternalGrpcError(err.Error())
		}
		filePaths := make(map[string][]string)
		for key, filePath := range segment.FilePaths {
			filePaths[key] = filePath.Paths
		}
		createSegment.FilePaths = filePaths

		createSegments = append(createSegments, createSegment)
	}

	// Create the collection and segments
	collection, created, err := s.coordinator.CreateCollectionAndSegments(ctx, createCollection, createSegments)
	if err != nil {
		log.Error("CreateCollection failed. error creating collection", zap.Error(err), zap.String("collection_id", req.Id), zap.String("collection_name", req.Name))
		res.Collection = &coordinatorpb.Collection{
			Id:                   req.Id,
			Name:                 req.Name,
			ConfigurationJsonStr: req.ConfigurationJsonStr,
			SchemaStr:            req.SchemaStr,
			Dimension:            req.Dimension,
			Metadata:             req.Metadata,
			Tenant:               req.Tenant,
			Database:             req.Database,
		}
		res.Created = false
		if err == common.ErrCollectionUniqueConstraintViolation {
			return res, grpcutils.BuildAlreadyExistsGrpcError(err.Error())
		}
		if err == common.ErrDatabaseNotFound {
			return res, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		if err == common.ErrConcurrentDeleteCollection {
			return res, grpcutils.BuildAbortedGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.Collection = convertCollectionToProto(collection)
	res.Created = created
	log.Info("CreateCollection finished.", zap.String("collection_id", req.Id), zap.String("collection_name", req.Name), zap.Bool("created", created))
	return res, nil
}

func (s *Server) GetCollection(ctx context.Context, req *coordinatorpb.GetCollectionRequest) (*coordinatorpb.GetCollectionResponse, error) {
	collectionID := req.Id
	tenantID := req.Tenant
	databaseName := req.Database

	res := &coordinatorpb.GetCollectionResponse{}

	parsedCollectionID, err := types.ToUniqueID(&collectionID)
	if err != nil {
		log.Error("GetCollection failed. collection id format error", zap.Error(err), zap.Stringp("collection_id", &collectionID), zap.Stringp("collection_name", req.Name))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	collection, err := s.coordinator.GetCollection(ctx, parsedCollectionID, req.Name, *tenantID, *databaseName)
	if err != nil {
		if err == common.ErrCollectionSoftDeleted {
			return res, grpcutils.BuildFailedPreconditionGrpcError(err.Error())
		}

		log.Error("GetCollection failed. ", zap.Error(err), zap.Stringp("collection_id", &collectionID), zap.Stringp("collection_name", req.Name))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	res.Collection = convertCollectionToProto(collection)
	return res, nil
}

func (s *Server) GetCollections(ctx context.Context, req *coordinatorpb.GetCollectionsRequest) (*coordinatorpb.GetCollectionsResponse, error) {
	collectionID := req.Id
	collectionName := req.Name
	tenantID := req.Tenant
	databaseName := req.Database
	limit := req.Limit
	offset := req.Offset

	res := &coordinatorpb.GetCollectionsResponse{}

	collectionIDs := ([]types.UniqueID)(nil)
	parsedCollectionID, err := types.ToUniqueID(collectionID)
	if err != nil {
		log.Error("GetCollections failed. collection id format error", zap.Error(err), zap.Stringp("collection_id", collectionID), zap.Stringp("collection_name", collectionName))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	if parsedCollectionID != types.NilUniqueID() {
		collectionIDs = []types.UniqueID{parsedCollectionID}
	}

	if req.IdsFilter != nil {
		if collectionIDs == nil {
			collectionIDs = make([]types.UniqueID, 0, len(req.IdsFilter.Ids))
		}

		for _, id := range req.IdsFilter.Ids {
			parsedCollectionID, err := types.ToUniqueID(&id)
			if err != nil {
				log.Error("GetCollections failed. collection id format error", zap.Error(err), zap.Stringp("collection_id", &id), zap.Stringp("collection_name", collectionName))
				return res, grpcutils.BuildInternalGrpcError(err.Error())
			}
			if parsedCollectionID != types.NilUniqueID() {
				collectionIDs = append(collectionIDs, parsedCollectionID)
			}
		}
	}

	includeSoftDeleted := false
	if req.IncludeSoftDeleted != nil {
		includeSoftDeleted = *req.IncludeSoftDeleted
	}

	collections, err := s.coordinator.GetCollections(ctx, collectionIDs, collectionName, tenantID, databaseName, limit, offset, includeSoftDeleted)
	if err != nil {
		log.Error("GetCollections failed. ", zap.Error(err), zap.Stringp("collection_id", collectionID), zap.Stringp("collection_name", collectionName))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.Collections = make([]*coordinatorpb.Collection, 0, len(collections))
	for _, collection := range collections {
		collectionpb := convertCollectionToProto(collection)
		res.Collections = append(res.Collections, collectionpb)
	}
	return res, nil
}

func (s *Server) GetCollectionByResourceName(ctx context.Context, req *coordinatorpb.GetCollectionByResourceNameRequest) (*coordinatorpb.GetCollectionResponse, error) {
	tenantResourceName := req.TenantResourceName
	databaseName := req.Database
	collectionName := req.Name

	res := &coordinatorpb.GetCollectionResponse{}

	collection, err := s.coordinator.GetCollectionByResourceName(ctx, tenantResourceName, databaseName, collectionName)
	if err != nil {
		log.Error("GetCollectionByResourceName failed. ", zap.Error(err), zap.String("tenant_resource_name", tenantResourceName), zap.String("database_name", databaseName), zap.String("collection_name", collectionName))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	res.Collection = convertCollectionToProto(collection)
	return res, nil
}

func (s *Server) CountCollections(ctx context.Context, req *coordinatorpb.CountCollectionsRequest) (*coordinatorpb.CountCollectionsResponse, error) {
	res := &coordinatorpb.CountCollectionsResponse{}
	collection_count, err := s.coordinator.CountCollections(ctx, req.Tenant, req.Database)
	if err != nil {
		log.Error("CountCollections failed. ", zap.Error(err), zap.String("tenant", req.Tenant), zap.Stringp("database", req.Database))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.Count = collection_count
	return res, nil
}

func (s *Server) GetCollectionSize(ctx context.Context, req *coordinatorpb.GetCollectionSizeRequest) (*coordinatorpb.GetCollectionSizeResponse, error) {
	collectionID := req.Id

	res := &coordinatorpb.GetCollectionSizeResponse{}

	parsedCollectionID, err := types.ToUniqueID(&collectionID)
	if err != nil {
		log.Error("GetCollectionSize failed. collection id format error", zap.Error(err), zap.Stringp("collection_id", &collectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	total_records_post_compaction, err := s.coordinator.GetCollectionSize(ctx, parsedCollectionID)
	if err != nil {
		log.Error("GetCollectionSize failed. ", zap.Error(err), zap.Stringp("collection_id", &collectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.TotalRecordsPostCompaction = total_records_post_compaction
	return res, nil
}

func (s *Server) CheckCollections(ctx context.Context, req *coordinatorpb.CheckCollectionsRequest) (*coordinatorpb.CheckCollectionsResponse, error) {
	res := &coordinatorpb.CheckCollectionsResponse{}
	res.Deleted = make([]bool, len(req.CollectionIds))
	res.LogPosition = make([]int64, len(req.CollectionIds))

	for i, collectionID := range req.CollectionIds {
		parsedId, err := types.ToUniqueID(&collectionID)
		if err != nil {
			log.Error("CheckCollection failed. collection id format error", zap.Error(err), zap.String("collection_id", collectionID))
			return nil, grpcutils.BuildInternalGrpcError(err.Error())
		}
		deleted, logPosition, err := s.coordinator.CheckCollection(ctx, parsedId)

		if err != nil {
			log.Error("CheckCollection failed", zap.Error(err), zap.String("collection_id", collectionID))
			return nil, grpcutils.BuildInternalGrpcError(err.Error())
		}

		res.Deleted[i] = deleted
		res.LogPosition[i] = logPosition
	}
	return res, nil
}

func (s *Server) GetCollectionWithSegments(ctx context.Context, req *coordinatorpb.GetCollectionWithSegmentsRequest) (*coordinatorpb.GetCollectionWithSegmentsResponse, error) {
	collectionID := req.Id

	res := &coordinatorpb.GetCollectionWithSegmentsResponse{}

	parsedCollectionID, err := types.ToUniqueID(&collectionID)
	if err != nil {
		log.Error("GetCollectionWithSegments failed. collection id format error", zap.Error(err), zap.String("collection_id", collectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	collection, segments, err := s.coordinator.GetCollectionWithSegments(ctx, parsedCollectionID)
	if err != nil {
		log.Error("GetCollectionWithSegments failed. ", zap.Error(err), zap.String("collection_id", collectionID))
		if err == common.ErrCollectionNotFound || err == common.ErrCollectionSoftDeleted {
			return res, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.Collection = convertCollectionToProto(collection)

	segmentpbList := make([]*coordinatorpb.Segment, 0, len(segments))
	for _, segment := range segments {
		segmentpb := convertSegmentToProto(segment)
		segmentpbList = append(segmentpbList, segmentpb)
	}
	res.Segments = segmentpbList

	return res, nil
}

func (s *Server) DeleteCollection(ctx context.Context, req *coordinatorpb.DeleteCollectionRequest) (*coordinatorpb.DeleteCollectionResponse, error) {
	collectionID := req.GetId()
	res := &coordinatorpb.DeleteCollectionResponse{}
	parsedCollectionID, err := types.Parse(collectionID)
	if err != nil {
		log.Error("DeleteCollection failed", zap.Error(err), zap.String("collection_id", collectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	deleteCollection := &model.DeleteCollection{
		ID:           parsedCollectionID,
		TenantID:     req.GetTenant(),
		DatabaseName: req.GetDatabase(),
	}
	err = s.coordinator.SoftDeleteCollection(ctx, deleteCollection)
	if err != nil {
		log.Error("DeleteCollection failed", zap.Error(err), zap.String("collection_id", collectionID))
		if err == common.ErrCollectionDeleteNonExistingCollection {
			return res, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	log.Info("DeleteCollection succeeded", zap.String("collection_id", collectionID))
	return res, nil
}

func (s *Server) FinishCollectionDeletion(ctx context.Context, req *coordinatorpb.FinishCollectionDeletionRequest) (*coordinatorpb.FinishCollectionDeletionResponse, error) {
	res := &coordinatorpb.FinishCollectionDeletionResponse{}
	collectionID := req.GetId()
	parsedCollectionID, err := types.ToUniqueID(&collectionID)
	if err != nil {
		log.Error("FinishCollectionDeletion failed", zap.Error(err), zap.String("collection_id", collectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	deleteCollection := &model.DeleteCollection{
		ID:           parsedCollectionID,
		TenantID:     req.GetTenant(),
		DatabaseName: req.GetDatabase(),
	}
	err = s.coordinator.FinishCollectionDeletion(ctx, deleteCollection)
	if err != nil {
		log.Error("FinishCollectionDeletion failed", zap.Error(err), zap.String("collection_id", collectionID))
		if err == common.ErrCollectionNotFound {
			return res, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	log.Info("FinishCollectionDeletion succeeded", zap.String("collection_id", collectionID))
	return res, nil
}

func (s *Server) UpdateCollection(ctx context.Context, req *coordinatorpb.UpdateCollectionRequest) (*coordinatorpb.UpdateCollectionResponse, error) {
	res := &coordinatorpb.UpdateCollectionResponse{}

	collectionID := req.Id
	parsedCollectionID, err := types.ToUniqueID(&collectionID)
	if err != nil {
		log.Error("UpdateCollection failed. collection id format error", zap.Error(err), zap.String("collection_id", collectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	updateCollection := &model.UpdateCollection{
		ID:                      parsedCollectionID,
		Name:                    req.Name,
		Dimension:               req.Dimension,
		NewConfigurationJsonStr: req.ConfigurationJsonStr,
	}

	resetMetadata := req.GetResetMetadata()
	updateCollection.ResetMetadata = resetMetadata
	metadata := req.GetMetadata()
	// Case 1: if resetMetadata is true, then delete all metadata for the collection
	// Case 2: if resetMetadata is true and metadata is not nil -> THIS SHOULD NEVER HAPPEN
	// Case 3: if resetMetadata is false, and the metadata is not nil - set the metadata to the value in metadata
	// Case 4: if resetMetadata is false and metadata is nil, then leave the metadata as is
	if resetMetadata {
		if metadata != nil {
			log.Error("UpdateCollection failed. reset metadata is true and metadata is not nil", zap.Any("metadata", metadata), zap.String("collection_id", collectionID))
			return res, grpcutils.BuildInternalGrpcError(common.ErrInvalidMetadataUpdate.Error())
		} else {
			updateCollection.Metadata = nil
		}
	} else {
		if metadata != nil {
			modelMetadata, err := convertCollectionMetadataToModel(metadata)
			if err != nil {
				log.Error("UpdateCollection failed. error converting collection metadata to model", zap.Error(err), zap.String("collection_id", collectionID))
				return res, grpcutils.BuildInternalGrpcError(err.Error())
			}
			updateCollection.Metadata = modelMetadata
		} else {
			updateCollection.Metadata = nil
		}
	}

	_, err = s.coordinator.UpdateCollection(ctx, updateCollection)

	if err != nil {
		log.Error("UpdateCollection failed. error updating collection", zap.Error(err), zap.String("collection_id", collectionID))
		if err == common.ErrCollectionUniqueConstraintViolation {
			return res, grpcutils.BuildAlreadyExistsGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	return res, nil
}

func (s *Server) ForkCollection(ctx context.Context, req *coordinatorpb.ForkCollectionRequest) (*coordinatorpb.ForkCollectionResponse, error) {
	res := &coordinatorpb.ForkCollectionResponse{}

	sourceCollectionID := req.SourceCollectionId
	parsedSourceCollectionID, err := types.ToUniqueID(&sourceCollectionID)
	if err != nil {
		log.Error("ForkCollection failed. Failed to parse source collection id", zap.Error(err), zap.String("collection_id", sourceCollectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	targetCollectionID := req.TargetCollectionId
	parsedTargetCollectionID, err := types.ToUniqueID(&targetCollectionID)
	if err != nil {
		log.Error("ForkCollection failed. Failed to parse target collection id", zap.Error(err), zap.String("collection_id", targetCollectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	forkCollection := &model.ForkCollection{
		SourceCollectionID:                   parsedSourceCollectionID,
		SourceCollectionLogCompactionOffset:  req.SourceCollectionLogCompactionOffset,
		SourceCollectionLogEnumerationOffset: req.SourceCollectionLogEnumerationOffset,
		TargetCollectionID:                   parsedTargetCollectionID,
		TargetCollectionName:                 req.TargetCollectionName,
	}
	collection, segments, err := s.coordinator.ForkCollection(ctx, forkCollection)
	if err != nil {
		log.Error("ForkCollection failed. ", zap.Error(err), zap.String("collection_id", sourceCollectionID))
		if err == common.ErrCollectionNotFound || err == common.ErrCollectionSoftDeleted {
			return res, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		if err == common.ErrCollectionLogPositionStale {
			return res, grpcutils.BuildFailedPreconditionGrpcError(err.Error())
		}
		if err == common.ErrCollectionUniqueConstraintViolation {
			return res, grpcutils.BuildAlreadyExistsGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.Collection = convertCollectionToProto(collection)

	segmentpbList := make([]*coordinatorpb.Segment, 0, len(segments))
	for _, segment := range segments {
		segmentpb := convertSegmentToProto(segment)
		segmentpbList = append(segmentpbList, segmentpb)
	}
	res.Segments = segmentpbList

	return res, nil
}

func (s *Server) CountForks(ctx context.Context, req *coordinatorpb.CountForksRequest) (*coordinatorpb.CountForksResponse, error) {
	res := &coordinatorpb.CountForksResponse{}

	sourceCollectionID := req.SourceCollectionId
	parsedSourceCollectionID, err := types.ToUniqueID(&sourceCollectionID)
	if err != nil {
		log.Error("CountForks failed. Failed to parse source collection id", zap.Error(err), zap.String("collection_id", sourceCollectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	count, err := s.coordinator.CountForks(ctx, parsedSourceCollectionID)
	if err != nil {
		if err == common.ErrCollectionNotFound {
			return res, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.Count = count
	return res, nil
}

func (s *Server) ListCollectionVersions(ctx context.Context, req *coordinatorpb.ListCollectionVersionsRequest) (*coordinatorpb.ListCollectionVersionsResponse, error) {
	collectionID, err := types.ToUniqueID(&req.CollectionId)
	if err != nil {
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}

	markedForDeletion := false
	if req.IncludeMarkedForDeletion != nil {
		markedForDeletion = *req.IncludeMarkedForDeletion
	}

	versions, err := s.coordinator.ListCollectionVersions(ctx, collectionID, req.TenantId, req.MaxCount, req.VersionsBefore, req.VersionsAtOrAfter, markedForDeletion)
	if err != nil {
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	return &coordinatorpb.ListCollectionVersionsResponse{
		Versions: versions,
	}, nil
}

func (s *Server) FlushCollectionCompaction(ctx context.Context, req *coordinatorpb.FlushCollectionCompactionRequest) (*coordinatorpb.FlushCollectionCompactionResponse, error) {
	_, err := json.Marshal(req)
	if err != nil {
		log.Error("FlushCollectionCompaction failed. error marshalling request", zap.Error(err), zap.String("collection_id", req.CollectionId), zap.Int32("collection_version", req.CollectionVersion), zap.Int64("log_position", req.LogPosition))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	collectionID, err := types.ToUniqueID(&req.CollectionId)
	err = grpcutils.BuildErrorForUUID(collectionID, "collection", err)
	if err != nil {
		log.Error("FlushCollectionCompaction failed. error parsing collection id", zap.Error(err), zap.String("collection_id", req.CollectionId), zap.Int32("collection_version", req.CollectionVersion), zap.Int64("log_position", req.LogPosition))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	segmentCompactionInfo := make([]*model.FlushSegmentCompaction, 0, len(req.SegmentCompactionInfo))
	for _, flushSegmentCompaction := range req.SegmentCompactionInfo {
		segmentID, err := types.ToUniqueID(&flushSegmentCompaction.SegmentId)
		err = grpcutils.BuildErrorForUUID(segmentID, "segment", err)
		if err != nil {
			log.Error("FlushCollectionCompaction failed. error parsing segment id", zap.Error(err), zap.String("collection_id", req.CollectionId), zap.Int32("collection_version", req.CollectionVersion), zap.Int64("log_position", req.LogPosition))
			return nil, grpcutils.BuildInternalGrpcError(err.Error())
		}
		filePaths := make(map[string][]string)
		for key, filePath := range flushSegmentCompaction.FilePaths {
			filePaths[key] = filePath.Paths
		}
		segmentCompactionInfo = append(segmentCompactionInfo, &model.FlushSegmentCompaction{
			ID:        segmentID,
			FilePaths: filePaths,
		})
	}
	FlushCollectionCompaction := &model.FlushCollectionCompaction{
		ID:                         collectionID,
		TenantID:                   req.TenantId,
		LogPosition:                req.LogPosition,
		CurrentCollectionVersion:   req.CollectionVersion,
		FlushSegmentCompactions:    segmentCompactionInfo,
		TotalRecordsPostCompaction: req.TotalRecordsPostCompaction,
		SizeBytesPostCompaction:    req.SizeBytesPostCompaction,
		SchemaStr:                  req.SchemaStr,
	}
	flushCollectionInfo, err := s.coordinator.FlushCollectionCompaction(ctx, FlushCollectionCompaction)
	if err != nil {
		log.Error("FlushCollectionCompaction failed", zap.Error(err), zap.String("collection_id", req.CollectionId), zap.Int32("collection_version", req.CollectionVersion), zap.Int64("log_position", req.LogPosition))
		if err == common.ErrCollectionSoftDeleted {
			return nil, grpcutils.BuildFailedPreconditionGrpcError(err.Error())
		}
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res := &coordinatorpb.FlushCollectionCompactionResponse{
		CollectionId:       flushCollectionInfo.ID,
		CollectionVersion:  flushCollectionInfo.CollectionVersion,
		LastCompactionTime: flushCollectionInfo.TenantLastCompactionTime,
	}
	return res, nil
}

func (s *Server) FlushCollectionCompactionAndAttachedFunction(ctx context.Context, req *coordinatorpb.FlushCollectionCompactionAndAttachedFunctionRequest) (*coordinatorpb.FlushCollectionCompactionAndAttachedFunctionResponse, error) {
	// Parse the flush compaction request (nested message)
	flushReq := req.GetFlushCompaction()
	if flushReq == nil {
		log.Error("FlushCollectionCompactionAndAttachedFunction failed. flush_compaction is nil")
		return nil, grpcutils.BuildInternalGrpcError("flush_compaction is required")
	}

	// Parse attached function update info
	attachedFunctionUpdate := req.GetAttachedFunctionUpdate()
	if attachedFunctionUpdate == nil {
		log.Error("FlushCollectionCompactionAndAttachedFunction failed. attached_function_update is nil")
		return nil, grpcutils.BuildInternalGrpcError("attached_function_update is required")
	}

	attachedFunctionID, err := uuid.Parse(attachedFunctionUpdate.Id)
	if err != nil {
		log.Error("FlushCollectionCompactionAndAttachedFunction failed. error parsing attached_function_id", zap.Error(err), zap.String("attached_function_id", attachedFunctionUpdate.Id))
		return nil, grpcutils.BuildInternalGrpcError("invalid attached_function_id: " + err.Error())
	}

	runNonce, err := uuid.Parse(attachedFunctionUpdate.RunNonce)
	if err != nil {
		log.Error("FlushCollectionCompactionAndAttachedFunction failed. error parsing run_nonce", zap.Error(err), zap.String("run_nonce", attachedFunctionUpdate.RunNonce))
		return nil, grpcutils.BuildInternalGrpcError("invalid run_nonce: " + err.Error())
	}

	// Parse collection and segment info (reuse logic from FlushCollectionCompaction)
	collectionID, err := types.ToUniqueID(&flushReq.CollectionId)
	err = grpcutils.BuildErrorForUUID(collectionID, "collection", err)
	if err != nil {
		log.Error("FlushCollectionCompactionAndAttachedFunction failed. error parsing collection id", zap.Error(err), zap.String("collection_id", flushReq.CollectionId))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}

	// Validate completion_offset fits in int64 before storing in database
	if attachedFunctionUpdate.CompletionOffset > uint64(math.MaxInt64) {
		log.Error("FlushCollectionCompactionAndAttachedFunction: completion_offset too large",
			zap.Uint64("completion_offset", attachedFunctionUpdate.CompletionOffset))
		return nil, grpcutils.BuildInternalGrpcError("completion_offset too large")
	}
	completionOffsetSigned := int64(attachedFunctionUpdate.CompletionOffset)

	segmentCompactionInfo := make([]*model.FlushSegmentCompaction, 0, len(flushReq.SegmentCompactionInfo))
	for _, flushSegmentCompaction := range flushReq.SegmentCompactionInfo {
		segmentID, err := types.ToUniqueID(&flushSegmentCompaction.SegmentId)
		err = grpcutils.BuildErrorForUUID(segmentID, "segment", err)
		if err != nil {
			log.Error("FlushCollectionCompactionAndAttachedFunction failed. error parsing segment id", zap.Error(err), zap.String("collection_id", flushReq.CollectionId))
			return nil, grpcutils.BuildInternalGrpcError(err.Error())
		}
		filePaths := make(map[string][]string)
		for key, filePath := range flushSegmentCompaction.FilePaths {
			filePaths[key] = filePath.Paths
		}
		segmentCompactionInfo = append(segmentCompactionInfo, &model.FlushSegmentCompaction{
			ID:        segmentID,
			FilePaths: filePaths,
		})
	}

	flushCollectionCompaction := &model.FlushCollectionCompaction{
		ID:                         collectionID,
		TenantID:                   flushReq.TenantId,
		LogPosition:                flushReq.LogPosition,
		CurrentCollectionVersion:   flushReq.CollectionVersion,
		FlushSegmentCompactions:    segmentCompactionInfo,
		TotalRecordsPostCompaction: flushReq.TotalRecordsPostCompaction,
		SizeBytesPostCompaction:    flushReq.SizeBytesPostCompaction,
	}

	flushCollectionInfo, err := s.coordinator.FlushCollectionCompactionAndAttachedFunction(
		ctx,
		flushCollectionCompaction,
		attachedFunctionID,
		runNonce,
		completionOffsetSigned,
	)
	if err != nil {
		log.Error("FlushCollectionCompactionAndAttachedFunction failed", zap.Error(err), zap.String("collection_id", flushReq.CollectionId), zap.String("attached_function_id", attachedFunctionUpdate.Id))
		if err == common.ErrCollectionSoftDeleted {
			return nil, grpcutils.BuildFailedPreconditionGrpcError(err.Error())
		}
		if err == common.ErrAttachedFunctionNotFound {
			return nil, grpcutils.BuildNotFoundGrpcError(err.Error())
		}
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}

	res := &coordinatorpb.FlushCollectionCompactionAndAttachedFunctionResponse{
		CollectionId:       flushCollectionInfo.ID,
		CollectionVersion:  flushCollectionInfo.CollectionVersion,
		LastCompactionTime: flushCollectionInfo.TenantLastCompactionTime,
	}

	// Populate attached function fields with authoritative values from database
	if flushCollectionInfo.AttachedFunctionNextNonce != nil {
		res.NextNonce = flushCollectionInfo.AttachedFunctionNextNonce.String()
	}
	if flushCollectionInfo.AttachedFunctionNextRun != nil {
		res.NextRun = timestamppb.New(*flushCollectionInfo.AttachedFunctionNextRun)
	}
	if flushCollectionInfo.AttachedFunctionCompletionOffset != nil {
		// Validate completion_offset is non-negative before converting to uint64
		if *flushCollectionInfo.AttachedFunctionCompletionOffset < 0 {
			log.Error("FlushCollectionCompactionAndAttachedFunction: invalid completion_offset",
				zap.Int64("completion_offset", *flushCollectionInfo.AttachedFunctionCompletionOffset))
			return nil, grpcutils.BuildInternalGrpcError("attached function has invalid completion_offset")
		}
		res.CompletionOffset = uint64(*flushCollectionInfo.AttachedFunctionCompletionOffset)
	}

	return res, nil
}

func (s *Server) ListCollectionsToGc(ctx context.Context, req *coordinatorpb.ListCollectionsToGcRequest) (*coordinatorpb.ListCollectionsToGcResponse, error) {
	absoluteCutoffTimeSecs := (*uint64)(nil)
	if req.CutoffTime != nil {
		cutoffTime := uint64(req.CutoffTime.Seconds)
		absoluteCutoffTimeSecs = &cutoffTime
	}

	collectionsToGc, err := s.coordinator.ListCollectionsToGc(ctx, absoluteCutoffTimeSecs, req.Limit, req.TenantId, req.MinVersionsIfAlive)
	if err != nil {
		log.Error("ListCollectionsToGc failed", zap.Error(err))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res := &coordinatorpb.ListCollectionsToGcResponse{}
	for _, collectionToGc := range collectionsToGc {
		res.Collections = append(res.Collections, &coordinatorpb.CollectionToGcInfo{
			Id:              collectionToGc.ID.String(),
			Name:            collectionToGc.Name,
			VersionFilePath: collectionToGc.VersionFilePath,
			TenantId:        collectionToGc.TenantID,
			LineageFilePath: collectionToGc.LineageFilePath,
		})
	}
	return res, nil
}

// Mark the versions for deletion.
// GC minics a 2PC protocol.
// 1. Mark the versions for deletion by calling MarkVersionForDeletion.
// 2. Compute the diffs and delete the files from S3.
// 3. Delete the versions from the version file by calling DeleteCollectionVersion.
//
// NOTE about concurrency:
// This method updates the version file which can concurrently with FlushCollectionCompaction.
func (s *Server) MarkVersionForDeletion(ctx context.Context, req *coordinatorpb.MarkVersionForDeletionRequest) (*coordinatorpb.MarkVersionForDeletionResponse, error) {
	res, err := s.coordinator.MarkVersionForDeletion(ctx, req)
	if err != nil {
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	return res, nil
}

// Delete the versions from the version file. Refer to comments in MarkVersionForDeletion.
// NOTE about concurrency:
// This method updates the version file which can concurrently with FlushCollectionCompaction.
func (s *Server) DeleteCollectionVersion(ctx context.Context, req *coordinatorpb.DeleteCollectionVersionRequest) (*coordinatorpb.DeleteCollectionVersionResponse, error) {
	res, err := s.coordinator.DeleteCollectionVersion(ctx, req)
	if err != nil {
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	return res, nil
}

func (s *Server) BatchGetCollectionVersionFilePaths(ctx context.Context, req *coordinatorpb.BatchGetCollectionVersionFilePathsRequest) (*coordinatorpb.BatchGetCollectionVersionFilePathsResponse, error) {
	res, err := s.coordinator.BatchGetCollectionVersionFilePaths(ctx, req)
	if err != nil {
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	return res, nil
}

func (s *Server) BatchGetCollectionSoftDeleteStatus(ctx context.Context, req *coordinatorpb.BatchGetCollectionSoftDeleteStatusRequest) (*coordinatorpb.BatchGetCollectionSoftDeleteStatusResponse, error) {
	res, err := s.coordinator.BatchGetCollectionSoftDeleteStatus(ctx, req)
	if err != nil {
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	return res, nil
}
