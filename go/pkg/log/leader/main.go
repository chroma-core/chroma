package leader

import (
	"context"
	"os"
	"time"

	"github.com/pingcap/log"

	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

func AcquireLeaderLock(ctx context.Context, onStartedLeading func(context.Context)) {
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

	elector, err := setupLeaderElection(client, namespace, podName, onStartedLeading)
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

func setupLeaderElection(client *kubernetes.Clientset, namespace, podName string, onStartedLeading func(context.Context)) (lr *leaderelection.LeaderElector, err error) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      "log-leader-lock",
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
				onStartedLeading(ctx)
			},
			OnStoppedLeading: func() {
				log.Info("stopped leading")
			},
		},
	})
	return
}
