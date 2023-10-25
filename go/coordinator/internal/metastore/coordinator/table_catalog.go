package coordinator

import (
	"context"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/metastore"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// The catalog backed by databases using GORM.
type Catalog struct {
	metaDomain dbmodel.IMetaDomain
	txImpl     dbmodel.ITransaction
}

func NewTableCatalog(txImpl dbmodel.ITransaction, metaDomain dbmodel.IMetaDomain) *Catalog {
	return &Catalog{
		txImpl:     txImpl,
		metaDomain: metaDomain,
	}
}

var _ metastore.Catalog = (*Catalog)(nil)

func (tc *Catalog) ResetState(ctx context.Context) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		err := tc.metaDomain.CollectionDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset collection db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reest collection metadata db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.SegmentDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset segment db", zap.Error(err))
			return err
		}
		err = tc.metaDomain.SegmentMetadataDb(txCtx).DeleteAll()
		if err != nil {
			log.Error("error reset segment metadata db", zap.Error(err))
			return err
		}
		return nil
	})
}

func (tc *Catalog) CreateCollection(ctx context.Context, createCollection *model.CreateCollection, ts types.Timestamp) (*model.Collection, error) {
	var ressult *model.Collection

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// insert collection
		dbCollection := &dbmodel.Collection{
			ID:        createCollection.ID.String(),
			Name:      &createCollection.Name,
			Topic:     &createCollection.Topic,
			Dimension: createCollection.Dimension,
			Ts:        ts,
		}
		err := tc.metaDomain.CollectionDb(txCtx).Insert(dbCollection)
		if err != nil {
			return err
		}
		// insert collection metadata
		metadata := createCollection.Metadata
		dbCollectionMetadataList := convertCollectionMetadataToDB(createCollection.ID.String(), metadata)
		if len(dbCollectionMetadataList) != 0 {
			err = tc.metaDomain.CollectionMetadataDb(txCtx).Insert(dbCollectionMetadataList)
			if err != nil {
				return err
			}
		}
		// get collection
		collectionList, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(types.FromUniqueID(createCollection.ID), nil, nil)
		if err != nil {
			return err
		}
		ressult = convertCollectionToModel(collectionList)[0]
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ressult, nil
}

func (tc *Catalog) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error) {
	collectionAndMetadataList, err := tc.metaDomain.CollectionDb(ctx).GetCollections(types.FromUniqueID(collectionID), collectionName, collectionTopic)
	if err != nil {
		return nil, err
	}
	collections := convertCollectionToModel(collectionAndMetadataList)
	return collections, nil
}

func (tc *Catalog) DeleteCollection(ctx context.Context, collectionID types.UniqueID) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		err := tc.metaDomain.CollectionDb(txCtx).DeleteCollectionByID(collectionID.String())
		if err != nil {
			return err
		}
		err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(collectionID.String())
		if err != nil {
			return err
		}
		return nil
	})
}

func (tc *Catalog) UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection, ts types.Timestamp) (*model.Collection, error) {
	var result *model.Collection

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		dbCollection := &dbmodel.Collection{
			ID:        updateCollection.ID.String(),
			Name:      updateCollection.Name,
			Topic:     updateCollection.Topic,
			Dimension: updateCollection.Dimension,
			Ts:        ts,
		}
		err := tc.metaDomain.CollectionDb(txCtx).Update(dbCollection)
		if err != nil {
			return err
		}

		// Case 1: if ResetMetadata is true, then delete all metadata for the collection
		// Case 2: if ResetMetadata is true and metadata is not nil -> THIS SHOULD NEVER HAPPEN
		// Case 3: if ResetMetadata is false, and the metadata is not nil - set the metadata to the value in metadata
		// Case 4: if ResetMetadata is false and metadata is nil, then leave the metadata as is
		metadata := updateCollection.Metadata
		resetMetadata := updateCollection.ResetMetadata
		if resetMetadata {
			if metadata != nil { // Case 2
				return common.ErrInvalidMetadataUpdate
			} else { // Case 1
				err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(updateCollection.ID.String())
				if err != nil {
					return err
				}
			}
		} else {
			if metadata != nil { // Case 3
				err = tc.metaDomain.CollectionMetadataDb(txCtx).DeleteByCollectionID(updateCollection.ID.String())
				if err != nil {
					return err
				}
				dbCollectionMetadataList := convertCollectionMetadataToDB(updateCollection.ID.String(), metadata)
				if len(dbCollectionMetadataList) != 0 {
					err = tc.metaDomain.CollectionMetadataDb(txCtx).Insert(dbCollectionMetadataList)
					if err != nil {
						return err
					}
				}
			}
		}
		collectionList, err := tc.metaDomain.CollectionDb(txCtx).GetCollections(types.FromUniqueID(updateCollection.ID), nil, nil)
		if err != nil {
			return err
		}
		result = convertCollectionToModel(collectionList)[0]
		return nil
	})
	if err != nil {
		return nil, err
	}
	log.Info("collection updated", zap.Any("collection", result))
	return result, nil
}

func (tc *Catalog) CreateSegment(ctx context.Context, createSegment *model.CreateSegment, ts types.Timestamp) (*model.Segment, error) {
	var result *model.Segment

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// insert segment
		collectionString := createSegment.CollectionID.String()
		dbSegment := &dbmodel.Segment{
			ID:           createSegment.ID.String(),
			CollectionID: &collectionString,
			Type:         createSegment.Type,
			Scope:        createSegment.Scope,
			Ts:           ts,
		}
		if createSegment.Topic != nil {
			dbSegment.Topic = createSegment.Topic
		}
		err := tc.metaDomain.SegmentDb(txCtx).Insert(dbSegment)
		if err != nil {
			log.Error("error inserting segment", zap.Error(err))
			return err
		}
		// insert segment metadata
		metadata := createSegment.Metadata
		if metadata != nil {
			dbSegmentMetadataList := convertSegmentMetadataToDB(createSegment.ID.String(), metadata)
			if len(dbSegmentMetadataList) != 0 {
				err = tc.metaDomain.SegmentMetadataDb(txCtx).Insert(dbSegmentMetadataList)
				if err != nil {
					log.Error("error inserting segment metadata", zap.Error(err))
					return err
				}
			}
		}
		// get segment
		segmentList, err := tc.metaDomain.SegmentDb(txCtx).GetSegments(createSegment.ID, nil, nil, nil, types.NilUniqueID())
		if err != nil {
			log.Error("error getting segment", zap.Error(err))
			return err
		}
		result = convertSegmentToModel(segmentList)[0]
		return nil
	})
	if err != nil {
		log.Error("error creating segment", zap.Error(err))
		return nil, err
	}
	log.Info("segment created", zap.Any("segment", result))
	return result, nil
}

func (tc *Catalog) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID, ts types.Timestamp) ([]*model.Segment, error) {
	segmentAndMetadataList, err := tc.metaDomain.SegmentDb(ctx).GetSegments(segmentID, segmentType, scope, topic, collectionID)
	if err != nil {
		return nil, err
	}
	segments := make([]*model.Segment, 0, len(segmentAndMetadataList))
	for _, segmentAndMetadata := range segmentAndMetadataList {
		segment := &model.Segment{
			ID:    types.MustParse(segmentAndMetadata.Segment.ID),
			Type:  segmentAndMetadata.Segment.Type,
			Scope: segmentAndMetadata.Segment.Scope,
			Topic: segmentAndMetadata.Segment.Topic,
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
	return segments, nil
}

func (tc *Catalog) DeleteSegment(ctx context.Context, segmentID types.UniqueID) error {
	return tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		err := tc.metaDomain.SegmentDb(txCtx).DeleteSegmentByID(segmentID.String())
		if err != nil {
			log.Error("error deleting segment", zap.Error(err))
			return err
		}
		err = tc.metaDomain.SegmentMetadataDb(txCtx).DeleteBySegmentID(segmentID.String())
		if err != nil {
			log.Error("error deleting segment metadata", zap.Error(err))
			return err
		}
		return nil
	})
}

func (tc *Catalog) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment, ts types.Timestamp) (*model.Segment, error) {
	var result *model.Segment

	err := tc.txImpl.Transaction(ctx, func(txCtx context.Context) error {
		// update segment
		dbSegment := &dbmodel.UpdateSegment{
			ID:              updateSegment.ID.String(),
			Topic:           updateSegment.Topic,
			ResetTopic:      updateSegment.ResetTopic,
			Collection:      updateSegment.Collection,
			ResetCollection: updateSegment.ResetCollection,
		}

		err := tc.metaDomain.SegmentDb(txCtx).Update(dbSegment)
		if err != nil {
			return err
		}

		// Case 1: if ResetMetadata is true, then delete all metadata for the collection
		// Case 2: if ResetMetadata is true and metadata is not nil -> THIS SHOULD NEVER HAPPEN
		// Case 3: if ResetMetadata is false, and the metadata is not nil - set the metadata to the value in metadata
		// Case 4: if ResetMetadata is false and metadata is nil, then leave the metadata as is
		metadata := updateSegment.Metadata
		resetMetadata := updateSegment.ResetMetadata
		if resetMetadata {
			if metadata != nil { // Case 2
				return common.ErrInvalidMetadataUpdate
			} else { // Case 1
				err := tc.metaDomain.SegmentMetadataDb(txCtx).DeleteBySegmentID(updateSegment.ID.String())
				if err != nil {
					return err
				}
			}
		} else {
			if metadata != nil { // Case 3
				err := tc.metaDomain.SegmentMetadataDb(txCtx).DeleteBySegmentIDAndKeys(updateSegment.ID.String(), metadata.Keys())
				if err != nil {
					log.Error("error deleting segment metadata", zap.Error(err))
					return err
				}
				newMetadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
				for _, key := range metadata.Keys() {
					if metadata.Get(key) == nil {
						metadata.Remove(key)
					} else {
						newMetadata.Set(key, metadata.Get(key))
					}
				}
				dbSegmentMetadataList := convertSegmentMetadataToDB(updateSegment.ID.String(), newMetadata)
				if len(dbSegmentMetadataList) != 0 {
					err = tc.metaDomain.SegmentMetadataDb(txCtx).Insert(dbSegmentMetadataList)
					if err != nil {
						return err
					}
				}
			}
		}

		// get segment
		segmentList, err := tc.metaDomain.SegmentDb(txCtx).GetSegments(updateSegment.ID, nil, nil, nil, types.NilUniqueID())
		if err != nil {
			log.Error("error getting segment", zap.Error(err))
			return err
		}
		result = convertSegmentToModel(segmentList)[0]
		return nil
	})
	if err != nil {
		log.Error("error updating segment", zap.Error(err))
		return nil, err
	}
	log.Debug("segment updated", zap.Any("segment", result))
	return result, nil
}
