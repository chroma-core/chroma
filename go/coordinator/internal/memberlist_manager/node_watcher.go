package memberlist_manager

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type NodeWatcherCallback func(node_ip string)

type IWatcher interface {
	Start() error
	Stop() error
	RegisterCallback(callback NodeWatcherCallback)
	GetStatus(node_ip string) (Status, error)
}

type Status int

// Enum for status
const (
	Ready Status = iota
	NotReady
	Unknown
	MaxStatus
)

type KubernetesWatcher struct {
	stopCh         chan struct{}
	isRunning      bool
	clientset      kubernetes.Interface      // clientset for the coordinator
	informer       cache.SharedIndexInformer // informer for the coordinator
	callbacks      []NodeWatcherCallback
	ipToKey        map[string]string
	informerHandle cache.ResourceEventHandlerRegistration
}

func NewKubernetesWatcher(clientset kubernetes.Interface, coordinator_namespace string, pod_label string) *KubernetesWatcher {
	labelSelector := labels.SelectorFromSet(map[string]string{"member-type": pod_label})
	// TODO: set resync period to something other than 0?
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(coordinator_namespace), informers.WithTweakListOptions(func(options *metav1.ListOptions) { options.LabelSelector = labelSelector.String() }))
	podInformer := factory.Core().V1().Pods().Informer()
	ipToKey := make(map[string]string)

	w := &KubernetesWatcher{
		isRunning: false,
		clientset: clientset,
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
			if err == nil {
				ip := obj.(*v1.Pod).Status.PodIP
				w.ipToKey[ip] = key
				w.notify(ip)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(newObj)
			if err == nil {
				ip := newObj.(*v1.Pod).Status.PodIP
				w.ipToKey[ip] = key
				w.notify(ip)
			}
		},
		DeleteFunc: func(obj interface{}) {
			_, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				ip := obj.(*v1.Pod).Status.PodIP
				// The contract for GetStatus is that if the ip is not in this map, then it returns NotReady
				delete(w.ipToKey, ip)
				w.notify(ip)
			}
		},
	})
	if err != nil {
		// TODO: should we return error?
		panic(err)
	}

	w.informerHandle = registration

	w.stopCh = make(chan struct{})
	w.isRunning = true

	go w.informer.Run(w.stopCh)

	if !cache.WaitForCacheSync(w.stopCh, w.informer.HasSynced) {
		fmt.Println("Failed to sync cache")
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
	key, ok := w.ipToKey[node_ip]
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

	pod := obj.(*v1.Pod)
	conditions := pod.Status.Conditions
	for _, condition := range conditions {
		if condition.Type == v1.PodReady && condition.Status == v1.ConditionTrue {
			return Ready, nil
		}
	}
	return NotReady, nil

}
