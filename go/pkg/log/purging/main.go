package purging

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/pingcap/log"
	"os"
	"time"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func RunPurging(ctx context.Context, lg *repository.LogRepository) {
	log.Info("starting purging")
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
	return
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
			Name:      "log-purging-lock",
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
				log.Info("started leading")
				performPurgingLoop(ctx, lr, lg)
			},
			OnStoppedLeading: func() {
				log.Info("stopped leading")
			},
		},
	})
	return
}

func performPurgingLoop(ctx context.Context, le *leaderelection.LeaderElector, lg *repository.LogRepository) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Info("checking leader status")
			if le.IsLeader() {
				log.Info("leader is active")
				if err := lg.PurgeRecords(ctx); err != nil {
					log.Error("failed to purge records", zap.Error(err))
					continue
				}
				log.Info("purged records")
			} else {
				log.Info("leader is inactive")
				break
			}
		}
	}
}
