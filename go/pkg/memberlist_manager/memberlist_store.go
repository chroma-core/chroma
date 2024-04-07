package memberlist_manager

import (
	"context"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type IMemberlistStore interface {
	GetMemberlist(ctx context.Context) (return_memberlist *Memberlist, resourceVersion string, err error)
	UpdateMemberlist(ctx context.Context, memberlist *Memberlist, resourceVersion string) error
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

func (s *CRMemberlistStore) GetMemberlist(ctx context.Context) (return_memberlist *Memberlist, resourceVersion string, err error) {
	gvr := getGvr()
	unstrucuted, err := s.dynamicClient.Resource(gvr).Namespace(s.coordinatorNamespace).Get(ctx, s.memberlistCustomResource, metav1.GetOptions{})
	if err != nil {
		return nil, "", err
	}
	cr := unstrucuted.UnstructuredContent()
	members := cr["spec"].(map[string]interface{})["members"]
	memberlist := Memberlist{}
	if members == nil {
		// Empty memberlist
		return &memberlist, unstrucuted.GetResourceVersion(), nil
	}
	cast_members := members.([]interface{})
	for _, member := range cast_members {
		member_map := member.(map[string]interface{})
		memberlist = append(memberlist, member_map["url"].(string))
	}
	return &memberlist, unstrucuted.GetResourceVersion(), nil
}

func (s *CRMemberlistStore) UpdateMemberlist(ctx context.Context, memberlist *Memberlist, resourceVersion string) error {
	gvr := getGvr()
	log.Info("Updating memberlist store", zap.Any("memberlist", memberlist))
	unstructured := memberlistToCr(memberlist, s.coordinatorNamespace, s.memberlistCustomResource, resourceVersion)
	_, err := s.dynamicClient.Resource(gvr).Namespace("chroma").Update(context.TODO(), unstructured, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

func getGvr() schema.GroupVersionResource {
	gvr := schema.GroupVersionResource{Group: "chroma.cluster", Version: "v1", Resource: "memberlists"}
	return gvr
}

func memberlistToCr(memberlist *Memberlist, namespace string, memberlistName string, resourceVersion string) *unstructured.Unstructured {
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
				"name":            memberlistName,
				"namespace":       namespace,
				"resourceVersion": resourceVersion,
			},
			"spec": map[string]interface{}{
				"members": members,
			},
		},
	}

	return resource
}
