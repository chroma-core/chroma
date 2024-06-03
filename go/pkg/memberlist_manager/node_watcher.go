package memberlist_manager

import (
	"errors"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	lister_v1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

type NodeWatcherCallback func(node_ip string)

type IWatcher interface {
	common.Component
	RegisterCallback(callback NodeWatcherCallback)
	ListReadyMembers() (Memberlist, error)
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
	stopCh         chan struct{}
	isRunning      bool
	clientSet      kubernetes.Interface      // clientset for the service
	informer       cache.SharedIndexInformer // informer for the service
	lister         lister_v1.PodLister       // lister for the service
	callbacks      []NodeWatcherCallback
	informerHandle cache.ResourceEventHandlerRegistration
}

func NewKubernetesWatcher(clientset kubernetes.Interface, coordinator_namespace string, pod_label string, resyncPeriod time.Duration) *KubernetesWatcher {
	log.Info("Creating new kubernetes watcher", zap.String("namespace", coordinator_namespace), zap.String("pod label", pod_label), zap.Duration("resync period", resyncPeriod))
	labelSelector := labels.SelectorFromSet(map[string]string{MemberLabel: pod_label})
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, resyncPeriod, informers.WithNamespace(coordinator_namespace), informers.WithTweakListOptions(func(options *metav1.ListOptions) { options.LabelSelector = labelSelector.String() }))
	podInformer := factory.Core().V1().Pods().Informer()
	podLister := factory.Core().V1().Pods().Lister()

	w := &KubernetesWatcher{
		isRunning: false,
		clientSet: clientset,
		informer:  podInformer,
		lister:    podLister,
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
				log.Info("Kubernetes Pod Added", zap.String("key", key), zap.Any("pod name", objPod.Name))
				name := objPod.Name
				w.notify(name)
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
				log.Info("Kubernetes Pod Updated", zap.String("key", key), zap.String("pod name", objPod.Name))
				name := objPod.Name
				w.notify(name)
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
				log.Info("Kubernetes Pod Deleted", zap.String("pod name", objPod.Name))
				name := objPod.Name
				// The contract for GetStatus is that if the ip is not in this map, then it returns NotReady
				w.notify(name)
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

func (w *KubernetesWatcher) ListReadyMembers() (Memberlist, error) {
	pods, err := w.lister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	memberlist := Memberlist{}
	for _, pod := range pods {
		conditions := pod.Status.Conditions
		for _, condition := range conditions {
			if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
				member := Member{
					id: pod.Name,
				}
				memberlist = append(memberlist, member)
			}
		}
	}
	log.Info("ListReadyMembers", zap.Any("memberlist", memberlist))
	return memberlist, nil
}
