package purging

import (
	"context"
	"time"

	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func PerformPurgingLoop(ctx context.Context, lg *repository.LogRepository) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := lg.PurgeRecords(ctx); err != nil {
				log.Error("failed to purge records", zap.Error(err))
				continue
			}
			log.Info("purged records")
		}
	}
}
