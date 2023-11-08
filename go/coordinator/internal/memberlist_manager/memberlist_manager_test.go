package memberlist_manager

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/chroma/chroma-coordinator/internal/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
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
				v1.PodCondition{
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
		panic(err)
	}
	fmt.Println("Status: ", node_status)

}

func TestMemberlistStore(t *testing.T) {
	gvr := get_gvr()
	memberlist_name := "test-memberlist"
	namespace := "chroma"

	// Create a fake MemberList resource object.
	memberlist := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "chroma.cluster/v1",
			"kind":       "MemberList",
			"metadata": map[string]interface{}{
				"name":      "test-memberlist",
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"members": []interface{}{
					map[string]interface{}{},
				},
			},
		},
	}

	// Following the assumptions of the real system, we initialize the CR with no members.
	dynamicClient := fake.NewSimpleDynamicClient(runtime.NewScheme(), memberlist)
	resource, err := dynamicClient.Resource(gvr).Namespace(namespace).Get(context.TODO(), memberlist_name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get resource: %v", err)
	}

	memberlist_store := NewCRMemberlistStore(dynamicClient, namespace, memberlist_name)
	cr_memberlist, err := memberlist_store.GetMemberlist()
	for _, member := range cr_memberlist {
		fmt.Println(member)
	}
	if err != nil {
		panic(err)
	}
}
