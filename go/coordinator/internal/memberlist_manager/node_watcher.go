package memberlist_manager

import (
	"errors"
	"fmt"
	"math/rand"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

// A watcher takes a queue and adds updates to it
type IWatcher interface {
	Start() error
	Stop() error
	RegisterCallback(callback func(NodeUpdate))
}

type Status int

// Enum for status
const (
	Ready Status = iota
	NotReady
	Unknown
	MaxStatus
)

// Represents an update to a node
type NodeUpdate struct {
	node_ip string
	status  Status
}

// A mock watcher that generates random updates
type MockWatcher struct {
	stopCh    chan struct{}
	isRunning bool
	callbacks []func(NodeUpdate)
}

// Start the mock watcher
func (w *MockWatcher) Start() error {
	// Create a goroutine that generates updates
	if (w.callbacks == nil) || (len(w.callbacks) == 0) {
		return errors.New("No callbacks registered")
	}

	if w.isRunning {
		return errors.New("Watcher is already running")
	}

	w.stopCh = make(chan struct{})
	w.isRunning = true

	go func() {
		for {
			select {
			case <-w.stopCh:
				return
			case <-time.After(5 * time.Second):
				// Every 5 seconds, generate an update
				ip := fmt.Sprintf("192.168.0.%d", rand.Intn(255))
				status := Status(rand.Intn(int(MaxStatus)))
				for _, callback := range w.callbacks {
					callback(NodeUpdate{node_ip: ip, status: status})
				}
			}
		}
	}()

	return nil
}

// Stop the mock watcher
func (w *MockWatcher) Stop() error {
	// Stop generating updates
	if !w.isRunning {
		return errors.New("Watcher is not running")
	}

	close(w.stopCh)
	w.isRunning = false
	return nil
}

// Register a queue
func (w *MockWatcher) RegisterCallback(callback func(NodeUpdate)) {
	w.callbacks = append(w.callbacks, callback)
}

// Kubernetes watcher
type KubernetesWatcher struct {
	stopCh    chan struct{}
	isRunning bool
	clientset *kubernetes.Clientset     // clientset for the coordinator
	informer  cache.SharedIndexInformer // informer for the coordinator
}

func NewKubernetesWatcher(coordinator_namespace string) *KubernetesWatcher {
	// Create a new kubernetes watcher
	// Load the default kubeconfig file
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	config, err := loadingRules.Load()

	clientConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		panic(err.Error())
	}

	// Create a clientset for the coordinator
	clientset, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		panic(err.Error())
	}

	// Create an informer for the coordinator for pods with the given label
	labelSelector := labels.SelectorFromSet(map[string]string{"member-type": "worker"})

	fmt.Println("Creating informer for namespace: " + coordinator_namespace)
	// Create a shared informer factory with the specific label selector
	// TODO: set resync period to something other than 0?
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(coordinator_namespace), informers.WithTweakListOptions(func(options *metav1.ListOptions) { options.LabelSelector = labelSelector.String() }))
	// factory := informers.NewSharedInformerFactoryWithOptions(clientset, 0, informers.WithNamespace(coordinator_namespace))

	// Get a Pod informer. This pod informer will only watch pods with the given label
	podInformer := factory.Core().V1().Pods().Informer()

	return &KubernetesWatcher{
		clientset: clientset,
		informer:  podInformer,
	}
}

func (w *KubernetesWatcher) Start() error {

	// Create a goroutine that generates updates
	if w.queue == nil {
		return errors.New("Queue is not Registered")
	}

	if w.isRunning {
		return errors.New("Watcher is already running")
	}

	w.stopCh = make(chan struct{})
	w.isRunning = true

	w.informer.Run(w.stopCh)

	if !cache.WaitForCacheSync(w.stopCh, w.informer.HasSynced) {
		fmt.Println("Failed to sync cache")
	}

	return nil
}

// Stop the kubernetes watcher
func (w *KubernetesWatcher) Stop() error {
	// Stop generating updates
	if !w.isRunning {
		return errors.New("Watcher is not running")
	}

	// TODO: unregister podInformer event handler?

	close(w.stopCh)
	w.isRunning = false
	return nil
}

// Register a queue
func (w *KubernetesWatcher) RegisterCallback(callback func(NodeUpdate)) {
	w.informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			w.queue.Add(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			w.queue.Add(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			// Handle pod deletion if necessary
		},
	})
}

func (w *KubernetesWatcher) notify(update NodeUpdate) {
	for _, callback := range w.callbacks {
		callback(update)
	}
}
