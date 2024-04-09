package coordinator

import (
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func convertCollectionToModel(collectionAndMetadataList []*dbmodel.CollectionAndMetadata) []*model.Collection {
	if collectionAndMetadataList == nil {
		return nil
	}
	collections := make([]*model.Collection, 0, len(collectionAndMetadataList))
	for _, collectionAndMetadata := range collectionAndMetadataList {
		collection := &model.Collection{
			ID:           types.MustParse(collectionAndMetadata.Collection.ID),
			Name:         *collectionAndMetadata.Collection.Name,
			Dimension:    collectionAndMetadata.Collection.Dimension,
			TenantID:     collectionAndMetadata.TenantID,
			DatabaseName: collectionAndMetadata.DatabaseName,
			Ts:           collectionAndMetadata.Collection.Ts,
			LogPosition:  collectionAndMetadata.Collection.LogPosition,
			Version:      collectionAndMetadata.Collection.Version,
		}
		collection.Metadata = convertCollectionMetadataToModel(collectionAndMetadata.CollectionMetadata)
		collections = append(collections, collection)
	}
	log.Debug("collection to model", zap.Any("collections", collections))
	return collections
}

func convertCollectionMetadataToModel(collectionMetadataList []*dbmodel.CollectionMetadata) *model.CollectionMetadata[model.CollectionMetadataValueType] {
	metadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	if collectionMetadataList == nil {
		log.Debug("collection metadata to model", zap.Any("collectionMetadata", nil))
		return nil
	} else {
		for _, collectionMetadata := range collectionMetadataList {
			if collectionMetadata.Key != nil {
				switch {
				case collectionMetadata.StrValue != nil:
					metadata.Add(*collectionMetadata.Key, &model.CollectionMetadataValueStringType{Value: *collectionMetadata.StrValue})
				case collectionMetadata.IntValue != nil:
					metadata.Add(*collectionMetadata.Key, &model.CollectionMetadataValueInt64Type{Value: *collectionMetadata.IntValue})
				case collectionMetadata.FloatValue != nil:
					metadata.Add(*collectionMetadata.Key, &model.CollectionMetadataValueFloat64Type{Value: *collectionMetadata.FloatValue})
				default:
				}
			}
		}
		if metadata.Empty() {
			metadata = nil
		}
		log.Debug("collection metadata to model", zap.Any("collectionMetadata", metadata))
		return metadata
	}

}

func convertCollectionMetadataToDB(collectionID string, metadata *model.CollectionMetadata[model.CollectionMetadataValueType]) []*dbmodel.CollectionMetadata {
	if metadata == nil {
		log.Debug("collection metadata to db", zap.Any("collectionMetadata", nil))
		return nil
	}
	dbCollectionMetadataList := make([]*dbmodel.CollectionMetadata, 0, len(metadata.Metadata))
	for key, value := range metadata.Metadata {
		keyCopy := key
		dbCollectionMetadata := &dbmodel.CollectionMetadata{
			CollectionID: collectionID,
			Key:          &keyCopy,
		}
		switch v := (value).(type) {
		case *model.CollectionMetadataValueStringType:
			dbCollectionMetadata.StrValue = &v.Value
		case *model.CollectionMetadataValueInt64Type:
			dbCollectionMetadata.IntValue = &v.Value
		case *model.CollectionMetadataValueFloat64Type:
			dbCollectionMetadata.FloatValue = &v.Value
		default:
			log.Error("unknown collection metadata type", zap.Any("value", v))
		}
		dbCollectionMetadataList = append(dbCollectionMetadataList, dbCollectionMetadata)
	}
	log.Debug("collection metadata to db", zap.Any("collectionMetadata", dbCollectionMetadataList))
	return dbCollectionMetadataList
}

func convertSegmentToModel(segmentAndMetadataList []*dbmodel.SegmentAndMetadata) []*model.Segment {
	if segmentAndMetadataList == nil {
		return nil
	}
	segments := make([]*model.Segment, 0, len(segmentAndMetadataList))
	for _, segmentAndMetadata := range segmentAndMetadataList {
		segment := &model.Segment{
			ID:    types.MustParse(segmentAndMetadata.Segment.ID),
			Type:  segmentAndMetadata.Segment.Type,
			Scope: segmentAndMetadata.Segment.Scope,
			Ts:    segmentAndMetadata.Segment.Ts,
		}
		if segmentAndMetadata.Segment.CollectionID != nil {
			segment.CollectionID = types.MustParse(*segmentAndMetadata.Segment.CollectionID)
		} else {
			segment.CollectionID = types.NilUniqueID()
		}

		segment.Metadata = convertSegmentMetadataToModel(segmentAndMetadata.SegmentMetadata)
		segments = append(segments, segment)
	}
	log.Debug("segment to model", zap.Any("segments", segments))
	return segments
}

func convertSegmentMetadataToModel(segmentMetadataList []*dbmodel.SegmentMetadata) *model.SegmentMetadata[model.SegmentMetadataValueType] {
	if segmentMetadataList == nil {
		return nil
	} else {
		metadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
		for _, segmentMetadata := range segmentMetadataList {
			if segmentMetadata.Key != nil {
				switch {
				case segmentMetadata.StrValue != nil:
					metadata.Set(*segmentMetadata.Key, &model.SegmentMetadataValueStringType{Value: *segmentMetadata.StrValue})
				case segmentMetadata.IntValue != nil:
					metadata.Set(*segmentMetadata.Key, &model.SegmentMetadataValueInt64Type{Value: *segmentMetadata.IntValue})
				case segmentMetadata.FloatValue != nil:
					metadata.Set(*segmentMetadata.Key, &model.SegmentMetadataValueFloat64Type{Value: *segmentMetadata.FloatValue})
				default:
				}
			}
		}
		if metadata.Empty() {
			metadata = nil
		}
		log.Debug("segment metadata to model", zap.Any("segmentMetadata", nil))
		return metadata
	}
}

func convertSegmentMetadataToDB(segmentID string, metadata *model.SegmentMetadata[model.SegmentMetadataValueType]) []*dbmodel.SegmentMetadata {
	if metadata == nil {
		log.Debug("segment metadata db", zap.Any("segmentMetadata", nil))
		return nil
	}
	dbSegmentMetadataList := make([]*dbmodel.SegmentMetadata, 0, len(metadata.Metadata))
	for key, value := range metadata.Metadata {
		keyCopy := key
		dbSegmentMetadata := &dbmodel.SegmentMetadata{
			SegmentID: segmentID,
			Key:       &keyCopy,
		}
		switch v := (value).(type) {
		case *model.SegmentMetadataValueStringType:
			dbSegmentMetadata.StrValue = &v.Value
		case *model.SegmentMetadataValueInt64Type:
			dbSegmentMetadata.IntValue = &v.Value
		case *model.SegmentMetadataValueFloat64Type:
			dbSegmentMetadata.FloatValue = &v.Value
		default:
			log.Error("unknown segment metadata type", zap.Any("value", v))
		}
		dbSegmentMetadataList = append(dbSegmentMetadataList, dbSegmentMetadata)
	}
	log.Debug("segment metadata db", zap.Any("segmentMetadata", dbSegmentMetadataList))
	return dbSegmentMetadataList
}

func convertDatabaseToModel(dbDatabase *dbmodel.Database) *model.Database {
	return &model.Database{
		ID:     dbDatabase.ID,
		Name:   dbDatabase.Name,
		Tenant: dbDatabase.TenantID,
	}
}

func convertTenantToModel(dbTenant *dbmodel.Tenant) *model.Tenant {
	return &model.Tenant{
		Name: dbTenant.ID,
	}
}
