package grpc

import (
	"context"
	"math/rand"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type SoftDeleteCleaner struct {
	coordinator      coordinator.Coordinator
	ticker           *time.Ticker
	cleanupInterval  time.Duration
	maxAge           time.Duration
	limitPerCheck    uint
	maxInitialJitter time.Duration
}

func NewSoftDeleteCleaner(coordinator coordinator.Coordinator, cleanupInterval time.Duration, maxAge time.Duration, limitPerCheck uint) *SoftDeleteCleaner {
	return &SoftDeleteCleaner{
		coordinator:      coordinator,
		cleanupInterval:  cleanupInterval,
		maxAge:           maxAge,
		limitPerCheck:    limitPerCheck,
		maxInitialJitter: 5 * time.Second,
	}
}

func (s *SoftDeleteCleaner) Start() error {
	go s.run()
	return nil
}

func (s *SoftDeleteCleaner) run() {
	log.Info("Starting soft delete cleaner", zap.Duration("cleanup_interval", s.cleanupInterval), zap.Duration("max_age", s.maxAge), zap.Uint("limit_per_check", s.limitPerCheck))

	// Use configurable jitter instead of hard-coded 5000
	if s.maxInitialJitter > 0 {
		time.Sleep(time.Duration(rand.Int63n(int64(s.maxInitialJitter.Milliseconds())+1)) * time.Millisecond)
	}

	// Periodically check for soft deleted collections and delete them.
	s.ticker = time.NewTicker(s.cleanupInterval)
	// Delete only the collections that are older than the max age.
	for range s.ticker.C {
		// Add small random jitter (0-1 second) between checks
		time.Sleep(time.Duration(rand.Int63n(1000)) * time.Millisecond)

		collections, err := s.coordinator.GetSoftDeletedCollections(context.Background(), nil, "", "", int32(s.limitPerCheck))
		if err != nil {
			log.Error("Error while getting soft deleted collections", zap.Error(err))
			continue
		}
		numDeleted := 0
		for _, collection := range collections {
			// Skip root collections.
			// Only roots have lineage file name set.
			if collection.LineageFileName != nil && *collection.LineageFileName != "" {
				continue
			}
			timeSinceDelete := time.Since(time.Unix(collection.UpdatedAt, 0))
			if timeSinceDelete > s.maxAge {
				log.Info("Deleting soft deleted collection", zap.String("collection_id", collection.ID.String()), zap.Duration("time_since_delete", timeSinceDelete), zap.Time("last_updated_at", time.Unix(collection.UpdatedAt, 0)))
				err := s.coordinator.CleanupSoftDeletedCollection(context.Background(), &model.DeleteCollection{
					ID:           collection.ID,
					DatabaseName: collection.DatabaseName,
				})
				if err != nil {
					if err != common.ErrCollectionDeleteNonExistingCollection {
						log.Error("Error while deleting soft deleted collection", zap.Error(err), zap.String("collection", collection.ID.String()))
					}
				} else {
					numDeleted++
				}
			}
		}
	}
}

func (s *SoftDeleteCleaner) Stop() error {
	// Signal the ticker to stop.
	s.ticker.Stop()
	return nil
}
