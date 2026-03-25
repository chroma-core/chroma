package coordinator

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	"github.com/chroma-core/chroma/go/shared/otel"
	"github.com/pingcap/log"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// StartDLQMetrics registers a callback that reports the current state of
// the compaction DLQ. It queries the read replica to minimize
// overhead on the primary database.
func StartDLQMetrics(ctx context.Context) {
	log.Info("Starting compaction DLQ metrics")

	// Create an observable gauge to track the current state
	gauge, err := otel.Meter.Int64ObservableGauge(
		"compaction_dlq_size",
		metric.WithDescription("Current number of collections in compaction DLQ by failure count"),
		metric.WithUnit("{collections}"),
	)
	if err != nil {
		log.Error("Failed to create compaction_dlq_size gauge", zap.Error(err))
		return
	}

	metaDomain := dao.NewMetaDomain()

	// Register callback to report current state
	registration, err := otel.Meter.RegisterCallback(
		func(callbackCtx context.Context, observer metric.Observer) error {
			collectionDb := metaDomain.CollectionDb(callbackCtx)
			failureCounts, err := collectionDb.GetDLQFailureCounts()
			if err != nil {
				log.Error("Failed to get compaction DLQ size", zap.Error(err))
				return err
			}

			// Report current count for each failure level
			for failureCount, count := range failureCounts {
				observer.ObserveInt64(gauge, count,
					metric.WithAttributes(
						attribute.Int("failure_count", int(failureCount)),
					),
				)
			}

			log.Debug("Reported compaction DLQ state", zap.Any("failure_counts", failureCounts))
			return nil
		},
		gauge,
	)
	if err != nil {
		log.Error("Failed to register metrics callback", zap.Error(err))
		return
	}

	// Wait for context cancellation
	<-ctx.Done()

	// Unregister callback on shutdown
	if err := registration.Unregister(); err != nil {
		log.Error("Failed to unregister metrics callback", zap.Error(err))
	}

	log.Info("Stopped compaction DLQ metrics")
}
