package metrics

import (
	"context"
	"time"

	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/pingcap/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

var meter = otel.Meter("github.com/chroma-core/chroma/go/pkg/log/metrics")
var uncompactedEntriesCnt metric.Int64Gauge

func PerformMetricsLoop(ctx context.Context, lg *repository.LogRepository) {
	var err error
	uncompactedEntriesCnt, err = meter.Int64Gauge("log_total_uncompacted_records_count", metric.WithDescription("Number of uncompacted records in the log"), metric.WithUnit("{records}"))
	if err != nil {
		log.Error("failed to create metric", zap.Error(err))
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			totalUncompactedDepth, err := lg.GetTotalUncompactedRecordsCount(ctx)
			if err != nil {
				log.Error("failed to get uncompacted record count", zap.Error(err))
				continue
			}

			uncompactedEntriesCnt.Record(ctx, int64(totalUncompactedDepth))
		}
	}
}
