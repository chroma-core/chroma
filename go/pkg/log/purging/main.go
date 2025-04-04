package purging

import (
	"context"
	"time"

	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/pingcap/log"

	"go.uber.org/zap"
)

func PerformPurgingLoop(ctx context.Context, lg *repository.LogRepository) {
	// Log purge runs every 10 seconds.
	purgeTicker := time.NewTicker(10 * time.Second)
	// GC runs every 2 hours.
	gcTicker := time.NewTicker(2 * time.Hour)
	defer purgeTicker.Stop()
	defer gcTicker.Stop()

	// Run gc at boot
	if err := lg.GarbageCollection(ctx); err != nil {
		log.Error("failed to garbage collect", zap.Error(err))
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-purgeTicker.C:
			if err := lg.PurgeRecords(ctx); err != nil {
				log.Error("failed to purge records", zap.Error(err))
				continue
			}
		case <-gcTicker.C:
			// TODO: Add a RPC to manually trigger garbage collection
			if err := lg.GarbageCollection(ctx); err != nil {
				log.Error("failed to garbage collect", zap.Error(err))
				continue
			}
		}
	}
}
