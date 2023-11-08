package memberlist_manager

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type IMemberlistStore interface {
	GetMemberlist() (Memberlist, error)
	UpdateMemberlist(memberlist Memberlist) error
}

type Memberlist []string

type CRMemberlistStore struct {
	dynamicClient            dynamic.Interface
	coordinatorNamespace     string
	memberlistCustomResource string
}

func NewCRMemberlistStore(dynamicClient dynamic.Interface, coordinatorNamespace string, memberlistCustomResource string) *CRMemberlistStore {
	return &CRMemberlistStore{
		dynamicClient:            dynamicClient,
		coordinatorNamespace:     coordinatorNamespace,
		memberlistCustomResource: memberlistCustomResource,
	}
}

func (s *CRMemberlistStore) GetMemberlist() (Memberlist, error) {
	gvr := get_gvr()
	unstrucuted, err := s.dynamicClient.Resource(gvr).Namespace("chroma").Get(context.TODO(), "worker-memberlist", metav1.GetOptions{}) //.Namespace(m.coordinator_namespace).Get(context.TODO(), m.memberlist_custom_resource, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("Memberlist CR: ")
	fmt.Println(unstrucuted.UnstructuredContent())
	return Memberlist{}, nil
}

func (s *CRMemberlistStore) UpdateMemberlist(memberlist Memberlist) error {
	spec := map[string]string
	for _, member := range memberlist {
		spec[member] = ""
	}

}

func get_gvr() schema.GroupVersionResource {
	gvr := schema.GroupVersionResource{Group: "chroma.cluster", Version: "v1", Resource: "memberlists"}
	return gvr
}

func create_memberlist_cr(Memberlist) {
	resource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "chroma.cluster/v1",
			"kind":       "MemberList",
			"metadata": map[string]interface{}{
				"name":      "worker-memberlist",
				"namespace": "chroma",
			},
			"spec": map[string]interface{}{
				"members": memberlist,
			},
		}
	}
}
