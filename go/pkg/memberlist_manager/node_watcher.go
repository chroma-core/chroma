package memberlist_manager

import (
	"errors"
	"sync"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type NodeWatcherCallback func(node_ip string)

type IWatcher interface {
	common.Component
	RegisterCallback(callback NodeWatcherCallback)
	GetStatus(node_ip string) (Status, error)
}

type Status int

// Enum for status
const (
	Ready Status = iota
	NotReady
	Unknown
)

const MemberLabel = "member-type"

type KubernetesWatcher struct {
	mu             sync.Mutex
	stopCh         chan struct{}
	isRunning      bool
	clientSet      kubernetes.Interface      // clientset for the coordinator
	informer       cache.SharedIndexInformer // informer for the coordinator
	callbacks      []NodeWatcherCallback
	ipToKey        map[string]string
	informerHandle cache.ResourceEventHandlerRegistration
}

func NewKubernetesWatcher(clientset kubernetes.Interface, coordinator_namespace string, pod_label string, resyncPeriod time.Duration) *KubernetesWatcher {
	log.Info("Creating new kubernetes watcher", zap.String("namespace", coordinator_namespace), zap.String("pod label", pod_label), zap.Duration("resync period", resyncPeriod))
	labelSelector := labels.SelectorFromSet(map[string]string{MemberLabel: pod_label})
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, resyncPeriod, informers.WithNamespace(coordinator_namespace), informers.WithTweakListOptions(func(options *metav1.ListOptions) { options.LabelSelector = labelSelector.String() }))
	podInformer := factory.Core().V1().Pods().Informer()
	ipToKey := make(map[string]string)

	w := &KubernetesWatcher{
		isRunning: false,
		clientSet: clientset,
		informer:  podInformer,
		ipToKey:   ipToKey,
	}

	return w
}

func (w *KubernetesWatcher) Start() error {
	if w.isRunning {
		return errors.New("watcher is already running")
	}

	registration, err := w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			objPod, ok := obj.(*v1.Pod)
			if !ok {
				log.Error("Error while asserting object to pod")
			}
			if err == nil {
				log.Info("Kubernetes Pod Added", zap.String("key", key), zap.String("ip", objPod.Status.PodIP))
				ip := objPod.Status.PodIP
				w.mu.Lock()
				w.ipToKey[ip] = key
				w.mu.Unlock()
				w.notify(ip)
			} else {
				log.Error("Error while getting key from object", zap.Error(err))
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			objPod, ok := newObj.(*v1.Pod)
			if !ok {
				log.Error("Error while asserting object to pod")
			}
			if err == nil {
				log.Info("Kubernetes Pod Updated", zap.String("key", key), zap.String("ip", objPod.Status.PodIP))
				ip := objPod.Status.PodIP
				w.mu.Lock()
				w.ipToKey[ip] = key
				w.mu.Unlock()
				w.notify(ip)
			} else {
				log.Error("Error while getting key from object", zap.Error(err))
			}
		},
		DeleteFunc: func(obj interface{}) {
			_, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			objPod, ok := obj.(*v1.Pod)
			if !ok {
				log.Error("Error while asserting object to pod")
			}
			if err == nil {
				log.Info("Kubernetes Pod Deleted", zap.String("ip", objPod.Status.PodIP))
				ip := objPod.Status.PodIP
				// The contract for GetStatus is that if the ip is not in this map, then it returns NotReady
				w.mu.Lock()
				delete(w.ipToKey, ip)
				w.mu.Unlock()
				w.notify(ip)
			} else {
				log.Error("Error while getting key from object", zap.Error(err))
			}
		},
	})
	if err != nil {
		return err
	}

	w.informerHandle = registration

	w.stopCh = make(chan struct{})
	w.isRunning = true

	go w.informer.Run(w.stopCh)

	if !cache.WaitForCacheSync(w.stopCh, w.informer.HasSynced) {
		log.Error("Failed to sync cache")
	}

	return nil
}

// Stop the kubernetes watcher
func (w *KubernetesWatcher) Stop() error {
	// Stop generating updates
	if !w.isRunning {
		return errors.New("watcher is not running")
	}

	err := w.informer.RemoveEventHandler(w.informerHandle)

	close(w.stopCh)
	w.isRunning = false
	return err
}

// Register a queue
func (w *KubernetesWatcher) RegisterCallback(callback NodeWatcherCallback) {
	w.callbacks = append(w.callbacks, callback)
}

func (w *KubernetesWatcher) notify(update string) {
	for _, callback := range w.callbacks {
		callback(update)
	}
}

func (w *KubernetesWatcher) GetStatus(node_ip string) (Status, error) {
	w.mu.Lock()
	key, ok := w.ipToKey[node_ip]
	w.mu.Unlock()
	if !ok {
		return NotReady, nil
	}

	obj, exists, err := w.informer.GetIndexer().GetByKey(key)
	if err != nil {
		return Unknown, err
	}
	if !exists {
		return Unknown, errors.New("node does not exist")
	}

	pod, ok := obj.(*v1.Pod)
	if !ok {
		return Unknown, errors.New("object is not a pod")
	}
	conditions := pod.Status.Conditions
	for _, condition := range conditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			return Ready, nil
		}
	}
	return NotReady, nil

}
