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

	// Get the status of the node
	ok := retryUntilCondition(func() bool {
		memberlist, err := node_watcher.ListReadyMembers()
		if err != nil {
			t.Fatalf("Error getting node status: %v", err)
		}

		return reflect.DeepEqual(memberlist, Memberlist{"10.0.0.1"})
	}, 10, 1*time.Second)
	if !ok {
		t.Fatalf("Node status did not update after adding a pod")
	}

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

	ok = retryUntilCondition(func() bool {
		memberlist, err := node_watcher.ListReadyMembers()
		if err != nil {
			t.Fatalf("Error getting node status: %v", err)
		}
		return reflect.DeepEqual(memberlist, Memberlist{"10.0.0.1"})
	}, 10, 1*time.Second)
	if !ok {
		t.Fatalf("Node status did not update after adding a not ready pod")
	}
}

func TestMemberlistStore(t *testing.T) {
	memberlistName := "test-memberlist"
	namespace := "chroma"
	memberlist := &Memberlist{}
	cr_memberlist := memberlistToCr(memberlist, namespace, memberlistName, "0")

	// Following the assumptions of the real system, we initialize the CR with no members.
	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), cr_memberlist)

	memberlist_store := NewCRMemberlistStore(dynamicClient, namespace, memberlistName)
	memberlist, _, err := memberlist_store.GetMemberlist(context.TODO())
	if err != nil {
		t.Fatalf("Error getting memberlist: %v", err)
	}
	// assert the memberlist is empty
	assert.Equal(t, Memberlist{}, *memberlist)

	// Add a member to the memberlist
	memberlist_store.UpdateMemberlist(context.TODO(), &Memberlist{"10.0.0.1", "10.0.0.2"}, "0")
	memberlist, _, err = memberlist_store.GetMemberlist(context.TODO())
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
	gracefulPeriodSeconds := int64(0)
	clientset.CoreV1().Pods("chroma").Delete(context.TODO(), ip, metav1.DeleteOptions{
		GracePeriodSeconds: &gracefulPeriodSeconds,
	})
}

func TestMemberlistManager(t *testing.T) {
	memberlist_name := "test-memberlist"
	namespace := "chroma"
	initialMemberlist := &Memberlist{}
	initialCrMemberlist := memberlistToCr(initialMemberlist, namespace, memberlist_name, "0")

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
	createFakePod("10.0.0.49", clientset)

	// Get the memberlist
	ok := retryUntilCondition(func() bool {
		return getMemberlistAndCompare(t, memberlistStore, Memberlist{"10.0.0.49"})
	}, 10, 1*time.Second)
	if !ok {
		t.Fatalf("Memberlist did not update after adding a pod")
	}

	// Add another ready pod
	createFakePod("10.0.0.50", clientset)

	// Get the memberlist
	ok = retryUntilCondition(func() bool {
		return getMemberlistAndCompare(t, memberlistStore, Memberlist{"10.0.0.49", "10.0.0.50"})
	}, 10, 1*time.Second)
	if !ok {
		t.Fatalf("Memberlist did not update after adding a pod")
	}

	// Delete a pod
	deleteFakePod("10.0.0.49", clientset)

	// Get the memberlist
	ok = retryUntilCondition(func() bool {
		return getMemberlistAndCompare(t, memberlistStore, Memberlist{"10.0.0.50"})
	}, 10, 1*time.Second)
	if !ok {
		t.Fatalf("Memberlist did not update after deleting a pod")
	}
}

func TestMemberlistSame(t *testing.T) {
	memberlist := Memberlist{""}
	assert.True(t, memberlistSame(memberlist, memberlist))

	newMemberlist := Memberlist{"10.0.0.1"}
	assert.False(t, memberlistSame(memberlist, newMemberlist))
	assert.False(t, memberlistSame(newMemberlist, memberlist))
	assert.True(t, memberlistSame(newMemberlist, newMemberlist))

	memberlist = Memberlist{"10.0.0.2"}
	assert.False(t, memberlistSame(newMemberlist, memberlist))
	assert.False(t, memberlistSame(memberlist, newMemberlist))
	assert.True(t, memberlistSame(memberlist, memberlist))

	memberlist = Memberlist{"10.0.0.1", "10.0.0.2"}
	newMemberlist = Memberlist{"10.0.0.1", "10.0.0.2"}
	assert.True(t, memberlistSame(memberlist, newMemberlist))
	assert.True(t, memberlistSame(newMemberlist, memberlist))

	memberlist = Memberlist{"10.0.0.1", "10.0.0.2"}
	newMemberlist = Memberlist{"10.0.0.2", "10.0.0.1"}
	assert.True(t, memberlistSame(memberlist, newMemberlist))
	assert.True(t, memberlistSame(newMemberlist, memberlist))
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
	return reflect.DeepEqual(expected_memberlist, *memberlist)
}
