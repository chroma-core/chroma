package grpc

import (
	"context"
	"time"

	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type SoftDeleteCleaner struct {
	coordinator        coordinator.Coordinator
	ticker             *time.Ticker
	checkFreqSeconds   int
	gracePeriodSeconds int
	limitPerCheck      int
}

func NewSoftDeleteCleaner(coordinator coordinator.Coordinator, checkFreqSeconds int, gracePeriodSeconds int) *SoftDeleteCleaner {
	return &SoftDeleteCleaner{
		coordinator:        coordinator,
		checkFreqSeconds:   checkFreqSeconds,
		gracePeriodSeconds: gracePeriodSeconds,
		limitPerCheck:      10,
	}
}

func (s *SoftDeleteCleaner) Start() error {
	go s.run()
	return nil
}

func (s *SoftDeleteCleaner) run() {
	// Periodically check every 10 seconds for soft deleted collections and delete them.
	s.ticker = time.NewTicker(time.Duration(s.checkFreqSeconds) * time.Second)
	// Delete only the collections that are older than 1 hour.
	for range s.ticker.C {
		collections, err := s.coordinator.GetSoftDeletedCollections(context.Background(), nil, "", "", int32(s.limitPerCheck))
		if err != nil {
			log.Error("Error while getting soft deleted collections", zap.Error(err))
			continue
		}
		numDeleted := 0
		for _, collection := range collections {
			timeSinceDelete := time.Since(time.Unix(collection.UpdatedAt, 0))
			log.Info("Found soft deleted collection", zap.String("collection_id", collection.ID.String()), zap.Duration("time_since_delete", timeSinceDelete))
			if timeSinceDelete > time.Duration(s.gracePeriodSeconds) {
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
		log.Info("Deleted soft deleted collections", zap.Int("numDeleted", numDeleted), zap.Duration("duration", time.Since(time.Now())))
	}
}

func (s *SoftDeleteCleaner) Stop() error {
	// Signal the ticker to stop.
	s.ticker.Stop()
	return nil
}
