package grpc

import (
	"context"
	"encoding/json"

	"github.com/chroma-core/chroma/go/pkg/grpcutils"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
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
func (s *Server) CreateCollection(ctx context.Context, req *coordinatorpb.CreateCollectionRequest) (*coordinatorpb.CreateCollectionResponse, error) {
	res := &coordinatorpb.CreateCollectionResponse{}
	createCollection, err := convertToCreateCollectionModel(req)
	if err != nil {
		log.Error("CreateCollection failed. error converting to create collection model", zap.Error(err), zap.String("collection_id", req.Id), zap.String("collection_name", req.Name))
		res.Collection = &coordinatorpb.Collection{
			Id:                   req.Id,
			Name:                 req.Name,
			ConfigurationJsonStr: req.ConfigurationJsonStr,
			Dimension:            req.Dimension,
			Metadata:             req.Metadata,
			Tenant:               req.Tenant,
			Database:             req.Database,
		}
		res.Created = false
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	collection, created, err := s.coordinator.CreateCollection(ctx, createCollection)
	if err != nil {
		log.Error("CreateCollection failed. error creating collection", zap.Error(err), zap.String("collection_id", req.Id), zap.String("collection_name", req.Name))
		res.Collection = &coordinatorpb.Collection{
			Id:                   req.Id,
			Name:                 req.Name,
			ConfigurationJsonStr: req.ConfigurationJsonStr,
			Dimension:            req.Dimension,
			Metadata:             req.Metadata,
			Tenant:               req.Tenant,
			Database:             req.Database,
		}
		res.Created = false
		if err == common.ErrCollectionUniqueConstraintViolation {
			return res, grpcutils.BuildAlreadyExistsGrpcError(err.Error())
		}
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.Collection = convertCollectionToProto(collection)
	res.Created = created
	log.Info("CreateCollection finished.", zap.String("collection_id", req.Id), zap.String("collection_name", req.Name), zap.Bool("created", created))
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

	parsedCollectionID, err := types.ToUniqueID(collectionID)
	if err != nil {
		log.Error("GetCollections failed. collection id format error", zap.Error(err), zap.Stringp("collection_id", collectionID), zap.Stringp("collection_name", collectionName))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	collections, err := s.coordinator.GetCollections(ctx, parsedCollectionID, collectionName, tenantID, databaseName, limit, offset)
	if err != nil {
		log.Error("GetCollections failed. ", zap.Error(err), zap.Stringp("collection_id", collectionID), zap.Stringp("collection_name", collectionName))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res.Collections = make([]*coordinatorpb.Collection, 0, len(collections))
	for _, collection := range collections {
		collectionpb := convertCollectionToProto(collection)
		res.Collections = append(res.Collections, collectionpb)
	}
	log.Info("GetCollections succeeded", zap.Any("response", res.Collections), zap.Stringp("collection_id", collectionID), zap.Stringp("collection_name", collectionName))
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
	err = s.coordinator.DeleteCollection(ctx, deleteCollection)
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

func (s *Server) UpdateCollection(ctx context.Context, req *coordinatorpb.UpdateCollectionRequest) (*coordinatorpb.UpdateCollectionResponse, error) {
	res := &coordinatorpb.UpdateCollectionResponse{}

	collectionID := req.Id
	parsedCollectionID, err := types.ToUniqueID(&collectionID)
	if err != nil {
		log.Error("UpdateCollection failed. collection id format error", zap.Error(err), zap.String("collection_id", collectionID))
		return res, grpcutils.BuildInternalGrpcError(err.Error())
	}

	updateCollection := &model.UpdateCollection{
		ID:        parsedCollectionID,
		Name:      req.Name,
		Dimension: req.Dimension,
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

	log.Info("UpdateCollection succeeded", zap.String("collection_id", collectionID))
	return res, nil
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
		ID:                       collectionID,
		TenantID:                 req.TenantId,
		LogPosition:              req.LogPosition,
		CurrentCollectionVersion: req.CollectionVersion,
		FlushSegmentCompactions:  segmentCompactionInfo,
	}
	flushCollectionInfo, err := s.coordinator.FlushCollectionCompaction(ctx, FlushCollectionCompaction)
	if err != nil {
		log.Error("FlushCollectionCompaction failed", zap.Error(err), zap.String("collection_id", req.CollectionId), zap.Int32("collection_version", req.CollectionVersion), zap.Int64("log_position", req.LogPosition))
		return nil, grpcutils.BuildInternalGrpcError(err.Error())
	}
	res := &coordinatorpb.FlushCollectionCompactionResponse{
		CollectionId:       flushCollectionInfo.ID,
		CollectionVersion:  flushCollectionInfo.CollectionVersion,
		LastCompactionTime: flushCollectionInfo.TenantLastCompactionTime,
	}
	log.Info("FlushCollectionCompaction succeeded", zap.String("collection_id", req.CollectionId), zap.Int32("collection_version", req.CollectionVersion), zap.Int64("log_position", req.LogPosition))
	return res, nil
}
