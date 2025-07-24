package leader

import (
	"context"
	"os"
	"strings"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// extractServiceName extracts the service name from a pod name.
// The service name is expected to be the prefix before the last hyphen in the pod name.
// For example, from pod name "chroma-query-abc123", it will return "chroma-query".
func extractServiceName(podName string) string {
	parts := strings.Split(podName, "-")
	if len(parts) > 1 {
		return strings.Join(parts[:len(parts)-1], "-")
	}
	return podName
}

// AcquireLeaderLock starts leader election and runs the given function when leadership is acquired.
// The context passed to onStartedLeading will be cancelled when leadership is lost.
// The service name is automatically determined from the pod name by extracting the prefix before the last hyphen.
// The lock name will be formatted as "{service-name}-leader".
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

	// Format the lock name with the service name
	serviceName := extractServiceName(podName)
	lockName := serviceName + "-leader"

	elector, err := setupLeaderElection(client, namespace, podName, lockName, onStartedLeading)
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

func setupLeaderElection(
	client *kubernetes.Clientset,
	namespace string,
	podName string,
	lockName string,
	onStartedLeading func(context.Context),
) (*leaderelection.LeaderElector, error) {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      lockName,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}

	return leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("started leading", zap.String("lock", lockName))
				onStartedLeading(ctx)
			},
			OnStoppedLeading: func() {
				log.Info("stopped leading", zap.String("lock", lockName))
			},
		},
	})
}
