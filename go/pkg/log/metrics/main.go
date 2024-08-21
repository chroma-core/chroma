package metrics

import (
	"context"
	"os"
	"time"

	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/pingcap/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

var meter = otel.Meter("github.com/chroma-core/chroma/go/pkg/log/metrics")
var uncompactedEntriesCnt metric.Int64Gauge

func RunMetrics(ctx context.Context, lg *repository.LogRepository) {
	// Create metrics
	var err error
	uncompactedEntriesCnt, err = meter.Int64Gauge("log_total_uncompacted_records_count", metric.WithDescription("Number of uncompacted records in the log"), metric.WithUnit("{records}"))
	if err != nil {
		log.Error("failed to create metric", zap.Error(err))
		return
	}

	podName, _ := os.LookupEnv("POD_NAME")
	if podName == "" {
		log.Error("POD_NAME environment variable is not set")
		return
	}
	namespace, _ := os.LookupEnv("POD_NAMESPACE")
	if namespace == "" {
		log.Error("POD_NAMESPACE environment variable is not set")
		return
	}
	client, err := createKubernetesClient()
	if err != nil {
		log.Error("failed to create kubernetes client", zap.Error(err))
		return
	}

	elector, err := setupLeaderElection(client, namespace, podName, lg)
	if err != nil {
		log.Error("failed to setup leader election", zap.Error(err))
		return
	}

	elector.Run(ctx)
}

func createKubernetesClient() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}

func setupLeaderElection(client *kubernetes.Clientset, namespace, podName string, lg *repository.LogRepository) (lr *leaderelection.LeaderElector, err error) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "log-metric-queries-lock",
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}

	lr, err = leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("started leading metrics collection")
				performMetricsLoop(ctx, lr, lg)
			},
			OnStoppedLeading: func() {
				log.Info("stopped leading metrics collection")
			},
		},
	})
	return
}

func performMetricsLoop(ctx context.Context, le *leaderelection.LeaderElector, lg *repository.LogRepository) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if le.IsLeader() {
				totalUncompactedDepth, err := lg.GetTotalUncompactedRecordsCount(ctx)
				if err != nil {
					log.Error("failed to get uncompacted record count", zap.Error(err))
					continue
				}

				uncompactedEntriesCnt.Record(ctx, int64(totalUncompactedDepth))
			} else {
				log.Info("leader is inactive")
				continue
			}
		}
	}
}
