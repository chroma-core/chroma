package memberlist_manager

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

// TODO:
/*

- Switch to camel case for all variables and methods

*/

// A memberlist manager is responsible for managing the memberlist for a
// coordinator. A memberlist is a CR in the coordinator's namespace that
// contains a list of all the members in the coordinator's cluster.
// The memberlist uses k8s watch to monitor changes to pods and then updates a CR

// Code structure
// 1. MemberlistManager struct
// 2. MemberlistManager methods
// 3. MemberlistManager helper methods
// 4. MemberlistManager CR methods
// 5. MemberlistManager CR helper methods
// 6. MemberlistManager CR event handler methods


// TODO: define interface
interface MemberlistManager {
	Start() error
	Run(stopCh <-chan struct{})
	Stop() error
}

// A memberlist manager watches pods in the cluster and updates a CR with the list of pods
// that are ready. The CR is a custom resource that is defined in the coordinator's namespace.
type MemberlistManager struct {
	pod_label                  string                          // labels of the pods in the cluster
	coordinator_namespace      string                          // namespace of the coordinator
	memberlist_custom_resource string                          // name of the memberlist custom resource to update
	clientset                  *kubernetes.Clientset           // clientset for the coordinator
	dynamic_client             *dynamic.DynamicClient          // dynamic client for the memberlist custom resource
	informer                   cache.SharedIndexInformer       // informer for the coordinator
	workqueue                  workqueue.RateLimitingInterface // workqueue for the coordinator
	stopCh 				   	   chan struct{}                   // stop channel for the coordinator
}

// NewMemberlistManager creates a new memberlist manager
func NewMemberlistManager(pod_label string, coordinator_namespace string, memberlist_custom_resource string) *MemberlistManager {
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

	// Create the dynamic client for the memberlist custom resource
	dynamic_client, err := dynamic.NewForConfig(clientConfig)
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

	// Create a workqueue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// Get a Pod informer. This pod informer will only watch pods with the given label
	podInformer := factory.Core().V1().Pods().Informer()

	// Add handlers to the informer
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			queue.Add(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			queue.Add(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			// Handle pod deletion if necessary
		},
	})

	return &MemberlistManager{
		pod_label:                  pod_label,
		coordinator_namespace:      coordinator_namespace,
		memberlist_custom_resource: memberlist_custom_resource,
		clientset:                  clientset,
		dynamic_client:             dynamic_client,
		informer:                   podInformer,
		workqueue:                  queue,
	}
}

// Implement Component interface
func (m *MemberlistManager) Start() error {
	// TODO: move stopch to memberlist manager struct and use it in stop method
	defer m.workqueue.ShutDown()

	go m.informer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, m.informer.HasSynced) {
		fmt.Println("Failed to sync cache")
		return
	}

	for {
		key, shutdown := m.workqueue.Get()
		if shutdown {
			break
		}
		// print the key
		pod := key.(*v1.Pod)
		fmt.Println("Status of pod in key: " + pod.Status.Phase)
		fmt.Println("IP of pod in key: " + pod.Status.PodIP)
		m.workqueue.Done(key)

		// get the memberlist cr using the dynamic client
		// TODO: used passed in memberlist custom resource name
		gvr := schema.GroupVersionResource{Group: "chroma.cluster", Version: "v1", Resource: "memberlists"}
		unstrucuted, err := m.dynamic_client.Resource(gvr).Namespace("chroma").Get(context.TODO(), "worker-memberlist", metav1.GetOptions{}) //.Namespace(m.coordinator_namespace).Get(context.TODO(), m.memberlist_custom_resource, metav1.GetOptions{})
		if err != nil {
			panic(err.Error())
		}
		fmt.Println("Memberlist CR: ")
		fmt.Println(unstrucuted.UnstructuredContent())
	}
}

func (m *MemberlistManager) Stop() error {
	return nil
}

// Create a main function that creates a memberlist manager and runs it
// func main() {
// 	// Create a memberlist manager
// 	memberlist_manager := NewMemberlistManager("member-type=worker", "chroma", "memberlist")
// 	stopCh := make(chan struct{})
// 	defer close(stopCh)
// 	// Run the memberlist manager
// 	memberlist_manager.Run(stopCh)
// }
