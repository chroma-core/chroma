package memberlist_manager

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type IMemberlistStore interface {
	GetMemberlist() (*Memberlist, error)
	UpdateMemberlist(memberlist *Memberlist) error
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

func (s *CRMemberlistStore) GetMemberlist() (*Memberlist, error) {
	gvr := getGvr()
	unstrucuted, err := s.dynamicClient.Resource(gvr).Namespace("chroma").Get(context.TODO(), "worker-memberlist", metav1.GetOptions{}) //.Namespace(m.coordinator_namespace).Get(context.TODO(), m.memberlist_custom_resource, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	cr := unstrucuted.UnstructuredContent()
	members := cr["spec"].(map[string]interface{})["members"]
	memberlist := Memberlist{}
	cast_members := members.([]interface{})
	for _, member := range cast_members {
		member_map := member.(map[string]interface{})
		memberlist = append(memberlist, member_map["url"].(string))
	}
	return &memberlist, nil
}

func (s *CRMemberlistStore) UpdateMemberlist(memberlist *Memberlist) error {
	gvr := getGvr()
	unstructured := memberlistToCr(memberlist)
	_, err := s.dynamicClient.Resource(gvr).Namespace("chroma").Update(context.TODO(), unstructured, metav1.UpdateOptions{})
	if err != nil {
		panic(err.Error())
	}
	return nil
}

func getGvr() schema.GroupVersionResource {
	gvr := schema.GroupVersionResource{Group: "chroma.cluster", Version: "v1", Resource: "memberlists"}
	return gvr
}

func memberlistToCr(memberlist *Memberlist) *unstructured.Unstructured {
	members := []interface{}{}
	for _, member := range *memberlist {
		members = append(members, map[string]interface{}{
			"url": member,
		})
	}

	resource := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "chroma.cluster/v1",
			"kind":       "MemberList",
			"metadata": map[string]interface{}{
				"name":      "worker-memberlist",
				"namespace": "chroma",
			},
			"spec": map[string]interface{}{
				"members": members,
			},
		},
	}

	return resource
}
