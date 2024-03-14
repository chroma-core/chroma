package grpc

import (
	"context"
	"errors"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/emptypb"
)

const errorCode = 500
const successCode = 200
const success = "ok"

func (s *Server) ResetState(context.Context, *emptypb.Empty) (*coordinatorpb.ResetStateResponse, error) {
	log.Info("reset state")
	res := &coordinatorpb.ResetStateResponse{}
	err := s.coordinator.ResetState(context.Background())
	if err != nil {
		res.Status = failResponseWithError(err, errorCode)
		return res, err
	}
	setResponseStatus(successCode)
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
		log.Error("error converting to create collection model", zap.Error(err))
		res.Collection = &coordinatorpb.Collection{
			Id:        req.Id,
			Name:      req.Name,
			Dimension: req.Dimension,
			Metadata:  req.Metadata,
			Tenant:    req.Tenant,
			Database:  req.Database,
		}
		res.Created = false
		res.Status = failResponseWithError(err, successCode)
		return res, nil
	}
	collection, err := s.coordinator.CreateCollection(ctx, createCollection)
	if err != nil {
		log.Error("error creating collection", zap.Error(err))
		res.Collection = &coordinatorpb.Collection{
			Id:        req.Id,
			Name:      req.Name,
			Dimension: req.Dimension,
			Metadata:  req.Metadata,
			Tenant:    req.Tenant,
			Database:  req.Database,
		}
		res.Created = false
		if err == common.ErrCollectionUniqueConstraintViolation {
			res.Status = failResponseWithError(err, 409)
		} else {
			res.Status = failResponseWithError(err, errorCode)
		}
		return res, nil
	}
	res.Collection = convertCollectionToProto(collection)
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) GetCollections(ctx context.Context, req *coordinatorpb.GetCollectionsRequest) (*coordinatorpb.GetCollectionsResponse, error) {
	collectionID := req.Id
	collectionName := req.Name
	collectionTopic := req.Topic
	tenantID := req.Tenant
	databaseName := req.Database

	res := &coordinatorpb.GetCollectionsResponse{}

	parsedCollectionID, err := types.ToUniqueID(collectionID)
	if err != nil {
		log.Error("collection id format error", zap.String("collectionpd.id", *collectionID))
		res.Status = failResponseWithError(common.ErrCollectionIDFormat, errorCode)
		return res, nil
	}

	collections, err := s.coordinator.GetCollections(ctx, parsedCollectionID, collectionName, collectionTopic, tenantID, databaseName)
	if err != nil {
		log.Error("error getting collections", zap.Error(err))
		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}
	res.Collections = make([]*coordinatorpb.Collection, 0, len(collections))
	for _, collection := range collections {
		collectionpb := convertCollectionToProto(collection)
		res.Collections = append(res.Collections, collectionpb)
	}
	log.Info("collection service collections", zap.Any("collections", res.Collections))
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) DeleteCollection(ctx context.Context, req *coordinatorpb.DeleteCollectionRequest) (*coordinatorpb.DeleteCollectionResponse, error) {
	collectionID := req.GetId()
	res := &coordinatorpb.DeleteCollectionResponse{}
	parsedCollectionID, err := types.Parse(collectionID)
	if err != nil {
		log.Error(err.Error(), zap.String("collectionpd.id", collectionID))
		res.Status = failResponseWithError(common.ErrCollectionIDFormat, errorCode)
		return res, nil
	}
	deleteCollection := &model.DeleteCollection{
		ID:           parsedCollectionID,
		TenantID:     req.GetTenant(),
		DatabaseName: req.GetDatabase(),
	}
	err = s.coordinator.DeleteCollection(ctx, deleteCollection)
	if err != nil {
		if errors.Is(err, common.ErrCollectionDeleteNonExistingCollection) {
			log.Error("ErrCollectionDeleteNonExistingCollection", zap.String("collectionpd.id", collectionID))
			res.Status = failResponseWithError(err, 404)
		} else {
			log.Error(err.Error(), zap.String("collectionpd.id", collectionID))
			res.Status = failResponseWithError(err, errorCode)
		}
		return res, nil
	}
	res.Status = setResponseStatus(successCode)
	return res, nil
}

func (s *Server) UpdateCollection(ctx context.Context, req *coordinatorpb.UpdateCollectionRequest) (*coordinatorpb.UpdateCollectionResponse, error) {
	res := &coordinatorpb.UpdateCollectionResponse{}

	collectionID := req.Id
	parsedCollectionID, err := types.ToUniqueID(&collectionID)
	if err != nil {
		log.Error("collection id format error", zap.String("collectionpd.id", collectionID))
		res.Status = failResponseWithError(common.ErrCollectionIDFormat, errorCode)
		return res, nil
	}

	updateCollection := &model.UpdateCollection{
		ID:        parsedCollectionID,
		Name:      req.Name,
		Topic:     req.Topic,
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
			log.Error("reset metadata is true and metadata is not nil", zap.Any("metadata", metadata))
			res.Status = failResponseWithError(common.ErrInvalidMetadataUpdate, errorCode)
			return res, nil
		} else {
			updateCollection.Metadata = nil
		}
	} else {
		if metadata != nil {
			modelMetadata, err := convertCollectionMetadataToModel(metadata)
			if err != nil {
				log.Error("error converting collection metadata to model", zap.Error(err))
				res.Status = failResponseWithError(err, errorCode)
				return res, nil
			}
			updateCollection.Metadata = modelMetadata
		} else {
			updateCollection.Metadata = nil
		}
	}

	_, err = s.coordinator.UpdateCollection(ctx, updateCollection)
	if err != nil {
		log.Error("error updating collection", zap.Error(err))
		res.Status = failResponseWithError(err, errorCode)
		return res, nil
	}

	res.Status = setResponseStatus(successCode)
	return res, nil
}

func failResponseWithError(err error, code int32) *coordinatorpb.Status {
	return &coordinatorpb.Status{
		Reason: err.Error(),
		Code:   code,
	}
}

func setResponseStatus(code int32) *coordinatorpb.Status {
	return &coordinatorpb.Status{
		Reason: success,
		Code:   code,
	}
}
