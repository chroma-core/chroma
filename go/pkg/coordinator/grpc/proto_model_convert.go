package grpc

import (
	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func convertCollectionMetadataToModel(collectionMetadata *coordinatorpb.UpdateMetadata) (*model.CollectionMetadata[model.CollectionMetadataValueType], error) {
	if collectionMetadata == nil {
		return nil, nil
	}

	metadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	for key, value := range collectionMetadata.Metadata {
		switch v := (value.Value).(type) {
		case *coordinatorpb.UpdateMetadataValue_StringValue:
			metadata.Add(key, &model.CollectionMetadataValueStringType{Value: v.StringValue})
		case *coordinatorpb.UpdateMetadataValue_IntValue:
			metadata.Add(key, &model.CollectionMetadataValueInt64Type{Value: v.IntValue})
		case *coordinatorpb.UpdateMetadataValue_FloatValue:
			metadata.Add(key, &model.CollectionMetadataValueFloat64Type{Value: v.FloatValue})
		default:
			log.Error("collection metadata value type not supported", zap.Any("metadata value", value))
			return nil, common.ErrUnknownCollectionMetadataType
		}
	}
	log.Debug("collection metadata in model", zap.Any("metadata", metadata))
	return metadata, nil
}

func convertCollectionToProto(collection *model.Collection) *coordinatorpb.Collection {
	if collection == nil {
		return nil
	}

	collectionpb := &coordinatorpb.Collection{
		Id:          collection.ID.String(),
		Name:        collection.Name,
		Dimension:   collection.Dimension,
		Tenant:      collection.TenantID,
		Database:    collection.DatabaseName,
		LogPosition: collection.LogPosition,
		Version:     collection.Version,
	}
	if collection.Metadata == nil {
		return collectionpb
	}

	metadatapb := convertCollectionMetadataToProto(collection.Metadata)
	collectionpb.Metadata = metadatapb
	return collectionpb
}

func convertCollectionMetadataToProto(collectionMetadata *model.CollectionMetadata[model.CollectionMetadataValueType]) *coordinatorpb.UpdateMetadata {
	if collectionMetadata == nil {
		return nil
	}
	metadatapb := &coordinatorpb.UpdateMetadata{
		Metadata: make(map[string]*coordinatorpb.UpdateMetadataValue),
	}
	for key, value := range collectionMetadata.Metadata {
		switch v := (value).(type) {
		case *model.CollectionMetadataValueStringType:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_StringValue{
					StringValue: v.Value,
				},
			}
		case *model.CollectionMetadataValueInt64Type:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_IntValue{
					IntValue: v.Value,
				},
			}
		case *model.CollectionMetadataValueFloat64Type:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
					FloatValue: v.Value,
				},
			}
		default:
			log.Error("collection metadata value type not supported", zap.Any("metadata value", value))
		}
	}
	return metadatapb
}

func convertToCreateCollectionModel(req *coordinatorpb.CreateCollectionRequest) (*model.CreateCollection, error) {
	collectionID, err := types.ToUniqueID(&req.Id)
	if err != nil {
		log.Error("collection id format error", zap.String("collectionpd.id", req.Id))
		return nil, common.ErrCollectionIDFormat
	}

	metadatapb := req.Metadata
	metadata, err := convertCollectionMetadataToModel(metadatapb)
	if err != nil {
		return nil, err
	}

	return &model.CreateCollection{
		ID:           collectionID,
		Name:         req.Name,
		Dimension:    req.Dimension,
		Metadata:     metadata,
		GetOrCreate:  req.GetGetOrCreate(),
		TenantID:     req.GetTenant(),
		DatabaseName: req.GetDatabase(),
	}, nil
}

func convertSegmentMetadataToModel(segmentMetadata *coordinatorpb.UpdateMetadata) (*model.SegmentMetadata[model.SegmentMetadataValueType], error) {
	if segmentMetadata == nil {
		return nil, nil
	}

	metadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	for key, value := range segmentMetadata.Metadata {
		if value.Value == nil {
			log.Info("segment metadata value is nil", zap.String("key", key))
			metadata.Set(key, nil)
			continue
		}
		switch v := (value.Value).(type) {
		case *coordinatorpb.UpdateMetadataValue_StringValue:
			metadata.Set(key, &model.SegmentMetadataValueStringType{Value: v.StringValue})
		case *coordinatorpb.UpdateMetadataValue_IntValue:
			metadata.Set(key, &model.SegmentMetadataValueInt64Type{Value: v.IntValue})
		case *coordinatorpb.UpdateMetadataValue_FloatValue:
			metadata.Set(key, &model.SegmentMetadataValueFloat64Type{Value: v.FloatValue})
		default:
			log.Error("segment metadata value type not supported", zap.Any("metadata value", value))
			return nil, common.ErrUnknownSegmentMetadataType
		}
	}
	return metadata, nil
}

func convertSegmentToProto(segment *model.Segment) *coordinatorpb.Segment {
	if segment == nil {
		return nil
	}
	scope := coordinatorpb.SegmentScope_value[segment.Scope]
	segmentSceope := coordinatorpb.SegmentScope(scope)
	filePaths := make(map[string]*coordinatorpb.FilePaths)
	for t, paths := range segment.FilePaths {
		filePaths[t] = &coordinatorpb.FilePaths{
			Paths: paths,
		}
	}
	segmentpb := &coordinatorpb.Segment{
		Id:         segment.ID.String(),
		Type:       segment.Type,
		Scope:      segmentSceope,
		Collection: nil,
		Metadata:   nil,
		FilePaths:  filePaths,
	}

	collectionID := segment.CollectionID
	if collectionID != types.NilUniqueID() {
		collectionIDString := collectionID.String()
		segmentpb.Collection = &collectionIDString
	}

	if segment.Metadata == nil {
		return segmentpb
	}

	metadatapb := convertSegmentMetadataToProto(segment.Metadata)
	segmentpb.Metadata = metadatapb
	log.Debug("segment", zap.Any("segment", segmentpb))
	return segmentpb
}

func convertSegmentMetadataToProto(segmentMetadata *model.SegmentMetadata[model.SegmentMetadataValueType]) *coordinatorpb.UpdateMetadata {
	metadatapb := &coordinatorpb.UpdateMetadata{
		Metadata: make(map[string]*coordinatorpb.UpdateMetadataValue),
	}

	for key, value := range segmentMetadata.Metadata {
		switch v := value.(type) {
		case *model.SegmentMetadataValueStringType:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_StringValue{StringValue: v.Value},
			}
		case *model.SegmentMetadataValueInt64Type:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_IntValue{IntValue: v.Value},
			}
		case *model.SegmentMetadataValueFloat64Type:
			metadatapb.Metadata[key] = &coordinatorpb.UpdateMetadataValue{
				Value: &coordinatorpb.UpdateMetadataValue_FloatValue{FloatValue: v.Value},
			}
		default:
			log.Error("segment metadata value type not supported", zap.Any("metadata value", value))
		}
	}
	return metadatapb
}

func convertSegmentToModel(segmentpb *coordinatorpb.Segment) (*model.CreateSegment, error) {
	segmentID, err := types.ToUniqueID(&segmentpb.Id)
	if err != nil {
		log.Error("segment id format error", zap.String("segment.id", segmentpb.Id))
		return nil, common.ErrSegmentIDFormat
	}

	collectionID, err := types.ToUniqueID(segmentpb.Collection)
	if err != nil {
		log.Error("collection id format error", zap.String("collectionpd.id", *segmentpb.Collection))
		return nil, common.ErrCollectionIDFormat
	}

	metadatapb := segmentpb.Metadata
	metadata, err := convertSegmentMetadataToModel(metadatapb)
	if err != nil {
		log.Error("convert segment metadata to model error", zap.Error(err))
		return nil, err
	}

	return &model.CreateSegment{
		ID:           segmentID,
		Type:         segmentpb.Type,
		Scope:        segmentpb.Scope.String(),
		CollectionID: collectionID,
		Metadata:     metadata,
	}, nil
}
