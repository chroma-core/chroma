package grpc

import (
	"context"
	"math/rand"
	"time"

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
		log.Info("Fetched soft deleted collections", zap.Int("num_collections", len(collections)))
		if err != nil {
			log.Error("Error while getting soft deleted collections", zap.Error(err))
			continue
		}
		numDeleted := 0
		for _, collection := range collections {
			timeSinceDelete := time.Since(time.Unix(collection.UpdatedAt, 0))
			log.Info("Found soft deleted collection", zap.String("collection_id", collection.ID.String()), zap.Duration("time_since_delete", timeSinceDelete))
			if timeSinceDelete > s.maxAge {
				log.Info("Deleting soft deleted collection", zap.String("collection_id", collection.ID.String()), zap.Duration("time_since_delete", timeSinceDelete))
				err := s.coordinator.CleanupSoftDeletedCollection(context.Background(), &model.DeleteCollection{
					ID: collection.ID,
				})
				if err != nil {
					log.Error("Error while deleting soft deleted collection", zap.Error(err), zap.String("collection", collection.ID.String()))
				} else {
					numDeleted++
				}
			}
		}
		log.Info("Deleted soft deleted collections", zap.Int("numDeleted", numDeleted))
	}
}

func (s *SoftDeleteCleaner) Stop() error {
	// Signal the ticker to stop.
	s.ticker.Stop()
	return nil
}
