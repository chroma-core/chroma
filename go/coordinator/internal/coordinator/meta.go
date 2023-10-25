package coordinator

import (
	"context"
	"sync"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/metastore"
	"github.com/chroma/chroma-coordinator/internal/model"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// IMeta is an interface that defines methods for the cache of the catalog.
type IMeta interface {
	ResetState(ctx context.Context) error
	AddCollection(ctx context.Context, coll *model.CreateCollection) (*model.Collection, error)
	GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error)
	DeleteCollection(ctx context.Context, collectionID types.UniqueID) error
	UpdateCollection(ctx context.Context, coll *model.UpdateCollection) (*model.Collection, error)
	AddSegment(ctx context.Context, createSegment *model.CreateSegment) error
	GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*model.Segment, error)
	DeleteSegment(ctx context.Context, segmentID types.UniqueID) error
	UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment) (*model.Segment, error)
}

// MetaTable is an implementation of IMeta. It loads the system catalog during startup
// and caches in memory. The implmentation needs to make sure that the in memory cache
// is consistent with the system catalog.
//
// Operations of MetaTable are protected by a read write lock and are thread safe.
type MetaTable struct {
	ddLock           sync.RWMutex
	ctx              context.Context
	catalog          metastore.Catalog
	collectionsCache map[types.UniqueID]*model.Collection
	segmentsCache    map[types.UniqueID]*model.Segment
}

var _ IMeta = (*MetaTable)(nil)

func NewMetaTable(ctx context.Context, catalog metastore.Catalog) (*MetaTable, error) {
	mt := &MetaTable{
		ctx:              ctx,
		catalog:          catalog,
		collectionsCache: make(map[types.UniqueID]*model.Collection),
		segmentsCache:    make(map[types.UniqueID]*model.Segment),
	}
	if err := mt.reload(); err != nil {
		return nil, err
	}
	return mt, nil
}

func (mt *MetaTable) reload() error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	oldCollections, err := mt.catalog.GetCollections(mt.ctx, types.NilUniqueID(), nil, nil)
	if err != nil {
		return err
	}
	// reload is idempotent
	mt.collectionsCache = make(map[types.UniqueID]*model.Collection)
	for _, collection := range oldCollections {
		mt.collectionsCache[types.UniqueID(collection.ID)] = collection
	}

	oldSegments, err := mt.catalog.GetSegments(mt.ctx, types.NilUniqueID(), nil, nil, nil, types.NilUniqueID(), 0)
	if err != nil {
		return err
	}
	// reload is idempotent
	mt.segmentsCache = make(map[types.UniqueID]*model.Segment)
	for _, segment := range oldSegments {
		mt.segmentsCache[types.UniqueID(segment.ID)] = segment
	}
	return nil
}

func (mt *MetaTable) ResetState(ctx context.Context) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if err := mt.catalog.ResetState(ctx); err != nil {
		return err
	}
	mt.collectionsCache = make(map[types.UniqueID]*model.Collection)
	mt.segmentsCache = make(map[types.UniqueID]*model.Segment)
	return nil
}

func (mt *MetaTable) AddCollection(ctx context.Context, coll *model.CreateCollection) (*model.Collection, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if _, ok := mt.collectionsCache[coll.ID]; ok {
		return nil, common.ErrCollectionUniqueConstraintViolation
	}

	collection, err := mt.catalog.CreateCollection(ctx, coll, coll.Ts)
	if err != nil {
		return nil, err
	}
	mt.collectionsCache[types.UniqueID(coll.ID)] = collection
	return collection, nil
}

func (mt *MetaTable) GetCollections(ctx context.Context, collectionID types.UniqueID, collectionName *string, collectionTopic *string) ([]*model.Collection, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	// Get the data from the cache
	collections := make([]*model.Collection, 0, len(mt.collectionsCache))
	for _, collection := range mt.collectionsCache {
		if model.FilterCollection(collection, collectionID, collectionName, collectionTopic) {
			collections = append(collections, collection)
		}
	}
	log.Debug("meta collections", zap.Any("collections", collections))
	return collections, nil

}

func (mt *MetaTable) DeleteCollection(ctx context.Context, collectionID types.UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	_, ok := mt.collectionsCache[collectionID]
	if !ok {
		return common.ErrCollectionDeleteNonExistingCollection
	}

	if err := mt.catalog.DeleteCollection(ctx, collectionID); err != nil {
		return err
	}
	delete(mt.collectionsCache, collectionID)
	return nil
}

func (mt *MetaTable) UpdateCollection(ctx context.Context, updateCollection *model.UpdateCollection) (*model.Collection, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	collection, err := mt.catalog.UpdateCollection(ctx, updateCollection, updateCollection.Ts)
	if err != nil {
		return nil, err
	}
	mt.collectionsCache[types.UniqueID(collection.ID)] = collection
	log.Debug("collection updated", zap.Any("collection", collection))
	return collection, nil
}

func (mt *MetaTable) AddSegment(ctx context.Context, createSegment *model.CreateSegment) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	segment, err := mt.catalog.CreateSegment(ctx, createSegment, createSegment.Ts)
	if err != nil {
		return err
	}
	mt.segmentsCache[types.UniqueID(createSegment.ID)] = segment
	log.Debug("segment added", zap.Any("segment", segment))
	return nil
}

func (mt *MetaTable) GetSegments(ctx context.Context, segmentID types.UniqueID, segmentType *string, scope *string, topic *string, collectionID types.UniqueID) ([]*model.Segment, error) {
	mt.ddLock.RLock()
	defer mt.ddLock.RUnlock()

	segments := make([]*model.Segment, 0, len(mt.segmentsCache))
	for _, segment := range mt.segmentsCache {
		if model.FilterSegments(segment, segmentID, segmentType, scope, topic, collectionID) {
			segments = append(segments, segment)
		}
	}
	return segments, nil
}

func (mt *MetaTable) DeleteSegment(ctx context.Context, segmentID types.UniqueID) error {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	if _, ok := mt.segmentsCache[segmentID]; !ok {
		return common.ErrSegmentDeleteNonExistingSegment
	}

	if err := mt.catalog.DeleteSegment(ctx, segmentID); err != nil {
		return err
	}
	delete(mt.segmentsCache, segmentID)
	return nil
}

func (mt *MetaTable) UpdateSegment(ctx context.Context, updateSegment *model.UpdateSegment) (*model.Segment, error) {
	mt.ddLock.Lock()
	defer mt.ddLock.Unlock()

	segment, err := mt.catalog.UpdateSegment(ctx, updateSegment, updateSegment.Ts)
	if err != nil {
		return nil, err
	}
	mt.segmentsCache[types.UniqueID(updateSegment.ID)] = segment
	return segment, nil
}
