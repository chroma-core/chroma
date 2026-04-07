package memberlist_manager

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/utils"
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
	node_watcher := NewKubernetesWatcher(clientset, "chroma", "worker", 60*time.Second)
	node_watcher.Start()

	// create some fake pods to test the watcher
	clientset.CoreV1().Pods("chroma").Create(context.Background(), &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-0",
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
		Spec: v1.PodSpec{
			NodeName: "test-node-0",
		},
	}, metav1.CreateOptions{})

	// Get the status of the node
	ok := retryUntilCondition(func() bool {
		memberlist, err := node_watcher.ListReadyMembers()
		if err != nil {
			t.Fatalf("Error getting node status: %v", err)
		}

		return reflect.DeepEqual(memberlist, Memberlist{Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}})
	}, 10, 1*time.Second)
	if !ok {
		t.Fatalf("Node status did not update after adding a pod")
	}

	// Add a not ready pod
	clientset.CoreV1().Pods("chroma").Create(context.Background(), &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod-1",
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

	ok = retryUntilCondition(func() bool {
		memberlist, err := node_watcher.ListReadyMembers()
		if err != nil {
			t.Fatalf("Error getting node status: %v", err)
		}
		return reflect.DeepEqual(memberlist, Memberlist{Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}})
	}, 10, 1*time.Second)
	if !ok {
		t.Fatalf("Node status did not update after adding a not ready pod")
	}
}

func TestMemberlistStore(t *testing.T) {
	memberlistName := "test-memberlist"
	namespace := "chroma"
	memberlist := Memberlist{}
	cr_memberlist := memberlist.toCr(namespace, memberlistName, "0")

	// Following the assumptions of the real system, we initialize the CR with no members.
	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), cr_memberlist)

	memberlist_store := NewCRMemberlistStore(dynamicClient, namespace, memberlistName)
	memberlist, _, err := memberlist_store.GetMemberlist(context.Background())
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	// assert the memberlist is empty
	assert.Equal(t, Memberlist{}, memberlist)

	// Add a member to the memberlist
	memberlist_store.UpdateMemberlist(context.Background(), Memberlist{Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}, "0")
	memberlist, _, err = memberlist_store.GetMemberlist(context.Background())
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	// assert the memberlist has the correct members
	if !memberlistSame(memberlist, Memberlist{Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}) {
		t.Fatalf("Memberlist did not update after adding a member")
	}
}

func createFakePod(memberId string, podIp string, node string, clientset kubernetes.Interface) {
	clientset.CoreV1().Pods("chroma").Create(context.Background(), &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      memberId,
			Namespace: "chroma",
			Labels: map[string]string{
				"member-type": "worker",
			},
		},
		Status: v1.PodStatus{
			PodIP: podIp,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
		Spec: v1.PodSpec{
			NodeName: node,
		},
	}, metav1.CreateOptions{})
}

func createFakeNode(name string, zone string, clientset kubernetes.Interface) {
	labels := map[string]string{}
	if zone != "" {
		labels[ZoneTopologyLabel] = zone
	}
	clientset.CoreV1().Nodes().Create(context.Background(), &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: labels,
		},
	}, metav1.CreateOptions{})
}

func deleteFakePod(name string, clientset kubernetes.Interface) {
	gracefulPeriodSeconds := int64(0)
	clientset.CoreV1().Pods("chroma").Delete(context.Background(), name, metav1.DeleteOptions{
		GracePeriodSeconds: &gracefulPeriodSeconds,
	})
}

func TestMemberlistManager(t *testing.T) {
	memberlist_name := "test-memberlist"
	namespace := "chroma"
	initialMemberlist := Memberlist{}
	initialCrMemberlist := initialMemberlist.toCr(namespace, memberlist_name, "0")

	// Create a fake kubernetes client
	clientset, err := utils.GetTestKubenertesInterface()
	if err != nil {
		t.Fatalf("Error getting kubernetes client: %v", err)
	}

	// Create a fake dynamic client
	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), initialCrMemberlist)

	// Create a node watcher
	nodeWatcher := NewKubernetesWatcher(clientset, namespace, "worker", 100*time.Millisecond)

	// Create a memberlist store
	memberlistStore := NewCRMemberlistStore(dynamicClient, namespace, memberlist_name)

	// Create a memberlist manager
	memberlistManager := NewMemberlistManager(nodeWatcher, memberlistStore)
	memberlistManager.SetReconcileInterval(1 * time.Second)
	memberlistManager.SetReconcileCount(1)

	// Start the memberlist manager
	err = memberlistManager.Start()
	if err != nil {
		t.Fatalf("Error starting memberlist manager: %v", err)
	}

	// Add a ready pod
	createFakePod("test-pod-0", "10.0.0.49", "test-node-0", clientset)

	// Get the memberlist
	ok := retryUntilCondition(func() bool {
		return getMemberlistAndCompare(t, memberlistStore, Memberlist{Member{id: "test-pod-0", ip: "10.0.0.49", node: "test-node-0", zone: ""}})
	}, 30, 1*time.Second)
	if !ok {
		t.Fatalf("Memberlist did not update after adding a pod")
	}

	// Add another ready pod
	createFakePod("test-pod-1", "10.0.0.50", "test-node-1", clientset)

	// Get the memberlist
	ok = retryUntilCondition(func() bool {
		return getMemberlistAndCompare(t, memberlistStore, Memberlist{Member{id: "test-pod-0", ip: "10.0.0.49", node: "test-node-0", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.50", node: "test-node-1", zone: ""}})
	}, 30, 1*time.Second)
	if !ok {
		t.Fatalf("Memberlist did not update after adding a pod")
	}

	// Delete a pod
	deleteFakePod("test-pod-0", clientset)

	// Get the memberlist
	ok = retryUntilCondition(func() bool {
		return getMemberlistAndCompare(t, memberlistStore, Memberlist{Member{id: "test-pod-1", ip: "10.0.0.50", node: "test-node-1", zone: ""}})
	}, 30, 1*time.Second)
	if !ok {
		t.Fatalf("Memberlist did not update after deleting a pod")
	}
}

func TestMemberlistManagerWithZones(t *testing.T) {
	memberlist_name := "test-memberlist"
	namespace := "chroma"
	initialMemberlist := Memberlist{}
	initialCrMemberlist := initialMemberlist.toCr(namespace, memberlist_name, "0")

	clientset, err := utils.GetTestKubenertesInterface()
	if err != nil {
		t.Fatalf("Error getting kubernetes client: %v", err)
	}

	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), initialCrMemberlist)
	nodeWatcher := NewKubernetesWatcher(clientset, namespace, "worker", 100*time.Millisecond)
	memberlistStore := NewCRMemberlistStore(dynamicClient, namespace, memberlist_name)
	memberlistManager := NewMemberlistManager(nodeWatcher, memberlistStore)
	memberlistManager.SetReconcileInterval(1 * time.Second)
	memberlistManager.SetReconcileCount(1)

	// Create nodes with zone labels before starting the manager
	createFakeNode("node-us-east-1a", "us-east-1a", clientset)
	createFakeNode("node-us-east-1b", "us-east-1b", clientset)

	err = memberlistManager.Start()
	if err != nil {
		t.Fatalf("Error starting memberlist manager: %v", err)
	}

	// Add pods on nodes with zone labels
	createFakePod("test-pod-0", "10.0.0.49", "node-us-east-1a", clientset)
	createFakePod("test-pod-1", "10.0.0.50", "node-us-east-1b", clientset)

	ok := retryUntilCondition(func() bool {
		return getMemberlistAndCompare(t, memberlistStore, Memberlist{
			Member{id: "test-pod-0", ip: "10.0.0.49", node: "node-us-east-1a", zone: "us-east-1a"},
			Member{id: "test-pod-1", ip: "10.0.0.50", node: "node-us-east-1b", zone: "us-east-1b"},
		})
	}, 30, 1*time.Second)
	if !ok {
		t.Fatalf("Memberlist did not populate zone labels from nodes")
	}
}

func TestMemberlistSame(t *testing.T) {
	memberlist := Memberlist{}
	assert.True(t, memberlistSame(memberlist, memberlist))

	newMemberlist := Memberlist{Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}}
	assert.False(t, memberlistSame(memberlist, newMemberlist))
	assert.False(t, memberlistSame(newMemberlist, memberlist))
	assert.True(t, memberlistSame(newMemberlist, newMemberlist))

	memberlist = Memberlist{Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}
	assert.False(t, memberlistSame(newMemberlist, memberlist))
	assert.False(t, memberlistSame(memberlist, newMemberlist))
	assert.True(t, memberlistSame(memberlist, memberlist))

	memberlist = Memberlist{Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}
	newMemberlist = Memberlist{Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}
	assert.True(t, memberlistSame(memberlist, newMemberlist))
	assert.True(t, memberlistSame(newMemberlist, memberlist))

	memberlist = Memberlist{Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}
	newMemberlist = Memberlist{Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}, Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}}
	assert.True(t, memberlistSame(memberlist, newMemberlist))
	assert.True(t, memberlistSame(newMemberlist, memberlist))

	memberlist = Memberlist{Member{id: "test-pod-0", ip: "10.0.0.2", node: "test-node-0", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}
	newMemberlist = Memberlist{Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}, Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}}
	assert.False(t, memberlistSame(memberlist, newMemberlist))
	assert.False(t, memberlistSame(newMemberlist, memberlist))

	// Just one ip wrong
	memberlist = Memberlist{Member{id: "test-pod-0", ip: "10.0.0.2", node: "test-node-0", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}
	newMemberlist = Memberlist{Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}, Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}}
	assert.False(t, memberlistSame(memberlist, newMemberlist))
	assert.False(t, memberlistSame(newMemberlist, memberlist))

	// Just one node wrong
	memberlist = Memberlist{Member{id: "test-pod-0", ip: "10.0.0.2", node: "test-node-2", zone: ""}, Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}}
	newMemberlist = Memberlist{Member{id: "test-pod-1", ip: "10.0.0.2", node: "test-node-1", zone: ""}, Member{id: "test-pod-0", ip: "10.0.0.1", node: "test-node-0", zone: ""}}
	assert.False(t, memberlistSame(memberlist, newMemberlist))
	assert.False(t, memberlistSame(newMemberlist, memberlist))
}

func retryUntilCondition(f func() bool, retry_count int, retry_interval time.Duration) bool {
	for i := 0; i < retry_count; i++ {
		if f() {
			return true
		}
		time.Sleep(retry_interval)
	}
	return false
}

func getMemberlistAndCompare(t *testing.T, memberlistStore IMemberlistStore, expected_memberlist Memberlist) bool {
	memberlist, _, err := memberlistStore.GetMemberlist(context.TODO())
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	return memberlistSame(memberlist, expected_memberlist)
}
