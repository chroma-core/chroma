package memberlist_manager

import (
	"context"
	"testing"
	"time"

	"github.com/chroma/chroma-coordinator/internal/utils"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
)

func TestNodeWatcher(t *testing.T) {
	clientset, err := utils.GetTestKubenertesInterface()
	if err != nil {
		panic(err)
	}

	// Create a node watcher
	node_watcher := NewKubernetesWatcher(clientset, "chroma", "worker")
	node_watcher.Start()

	// create some fake pods to test the watcher
	clientset.CoreV1().Pods("chroma").Create(context.TODO(), &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "chroma",
			Labels: map[string]string{
				"member-type": "worker",
			},
		},
		Status: v1.PodStatus{
			PodIP: "10.0.0.1",
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}, metav1.CreateOptions{})

	time.Sleep(1 * time.Second)

	// Get the status of the node
	node_status, err := node_watcher.GetStatus("10.0.0.1")
	if err != nil {
		t.Fatalf("Error getting node status: %v", err)
	}
	assert.Equal(t, Ready, node_status)

	// Add a not ready pod
	clientset.CoreV1().Pods("chroma").Create(context.TODO(), &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-2",
			Namespace: "chroma",
			Labels: map[string]string{
				"member-type": "worker",
			},
		},
		Status: v1.PodStatus{
			PodIP: "10.0.0.2",
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionFalse,
				},
			},
		},
	}, metav1.CreateOptions{})

	time.Sleep(1 * time.Second)

	// Get the status of the node
	node_status, err = node_watcher.GetStatus("10.0.0.2")
	if err != nil {
		t.Fatalf("Error getting node status: %v", err)
	}
	assert.Equal(t, NotReady, node_status)

}

func TestMemberlistStore(t *testing.T) {
	memberlist_name := "test-memberlist"
	namespace := "chroma"
	memberlist := &Memberlist{}
	cr_memberlist := memberlistToCr(memberlist)

	// Following the assumptions of the real system, we initialize the CR with no members.
	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), cr_memberlist)

	memberlist_store := NewCRMemberlistStore(dynamicClient, namespace, memberlist_name)
	memberlist, err := memberlist_store.GetMemberlist()
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	// assert the memberlist is empty
	assert.Equal(t, Memberlist{}, *memberlist)

	// Add a member to the memberlist
	memberlist_store.UpdateMemberlist(&Memberlist{"10.0.0.1", "10.0.0.2"})
	memberlist, err = memberlist_store.GetMemberlist()
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	assert.Equal(t, Memberlist{"10.0.0.1", "10.0.0.2"}, *memberlist)
}

func createFakePod(ip string, clientset kubernetes.Interface) {
	clientset.CoreV1().Pods("chroma").Create(context.TODO(), &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ip,
			Namespace: "chroma",
			Labels: map[string]string{
				"member-type": "worker",
			},
		},
		Status: v1.PodStatus{
			PodIP: ip,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}, metav1.CreateOptions{})
}

func deleteFakePod(ip string, clientset kubernetes.Interface) {
	clientset.CoreV1().Pods("chroma").Delete(context.TODO(), ip, metav1.DeleteOptions{})
}

func TestMemberlistManager(t *testing.T) {
	memberlist_name := "test-memberlist"
	namespace := "chroma"
	initial_memberlist := &Memberlist{}
	initial_cr_memberlist := memberlistToCr(initial_memberlist)

	// Create a fake kubernetes client
	clientset, err := utils.GetTestKubenertesInterface()
	if err != nil {
		t.Fatalf("Error getting kubernetes client: %v", err)
	}

	// Create a fake dynamic client
	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), initial_cr_memberlist)

	// Create a node watcher
	nodeWatcher := NewKubernetesWatcher(clientset, namespace, "worker")

	// Create a memberlist store
	memberlistStore := NewCRMemberlistStore(dynamicClient, namespace, memberlist_name)

	// Create a memberlist manager
	memberlist_manager := NewMemberlistManager(nodeWatcher, memberlistStore)

	// Start the memberlist manager
	err = memberlist_manager.Start()
	if err != nil {
		t.Fatalf("Error starting memberlist manager: %v", err)
	}

	// Add a ready pod
	createFakePod("10.0.0.49", clientset)

	time.Sleep(1 * time.Second)

	// Get the memberlist
	memberlist, err := memberlistStore.GetMemberlist()
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	assert.Equal(t, Memberlist{"10.0.0.49"}, *memberlist)

	// Add another ready pod
	createFakePod("10.0.0.50", clientset)

	time.Sleep(1 * time.Second)

	// Get the memberlist
	memberlist, err = memberlistStore.GetMemberlist()
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	assert.Equal(t, Memberlist{"10.0.0.49", "10.0.0.50"}, *memberlist)

	// Delete a pod
	deleteFakePod("10.0.0.49", clientset)

	time.Sleep(1 * time.Second)

	// Get the memberlist
	memberlist, err = memberlistStore.GetMemberlist()
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	assert.Equal(t, Memberlist{"10.0.0.50"}, *memberlist)

}
