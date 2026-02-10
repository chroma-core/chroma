package grpc

import (
	"context"
	"time"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	"github.com/chroma-core/chroma/go/shared/otel"
	"github.com/pingcap/log"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

const (
	dlqMetricsInterval = 30 * time.Second
)

// StartDLQMetrics starts a background goroutine that periodically emits
// the compaction DLQ size metric. It queries the read replica to minimize
// overhead on the primary database.
func StartDLQMetrics(ctx context.Context) {
	log.Info("Starting compaction DLQ metrics goroutine")

	// Create the gauge for DLQ size
	dlqSizeGauge, err := otel.Meter.Int64Gauge(
		"compaction_dlq_size",
		metric.WithDescription("Number of collections with compaction failures (compaction_failure_count > 0)"),
		metric.WithUnit("{collections}"),
	)
	if err != nil {
		log.Error("Failed to create compaction_dlq_size gauge", zap.Error(err))
		return
	}

	ticker := time.NewTicker(dlqMetricsInterval)
	defer ticker.Stop()

	metaDomain := dao.NewMetaDomain()

	// Emit metric immediately on startup
	emitDLQMetric(ctx, metaDomain, dlqSizeGauge)

	for {
		select {
		case <-ctx.Done():
			log.Info("Stopping compaction DLQ metrics goroutine")
			return
		case <-ticker.C:
			emitDLQMetric(ctx, metaDomain, dlqSizeGauge)
		}
	}
}

func emitDLQMetric(ctx context.Context, metaDomain *dao.MetaDomain, gauge metric.Int64Gauge) {
	collectionDb := metaDomain.CollectionDb(ctx)
	dlqSize, err := collectionDb.GetCompactionDLQSize()
	if err != nil {
		log.Error("Failed to get compaction DLQ size", zap.Error(err))
		return
	}

	gauge.Record(ctx, dlqSize)
	log.Debug("Emitted compaction DLQ size metric", zap.Int64("dlq_size", dlqSize))
}
