package memberlist_manager

import (
	"context"
	"errors"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type IMemberlistStore interface {
	GetMemberlist(ctx context.Context) (return_memberlist *Memberlist, resourceVersion string, err error)
	UpdateMemberlist(ctx context.Context, memberlist *Memberlist, resourceVersion string) error
}

type Member struct {
	id string
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface
func (m Member) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("id", m.id)
	return nil
}

type Memberlist []Member

// MarshalLogArray implements the zapcore.ArrayMarshaler interface
func (ml Memberlist) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, member := range ml {
		if err := enc.AppendObject(member); err != nil {
			return err
		}
	}
	return nil
}

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
	log.Info("Got unstructured memberlist object", zap.Any("cr", cr))
	members := cr["spec"].(map[string]interface{})["members"]
	memberlist := Memberlist{}
	if members == nil {
		// Empty memberlist
		log.Info("Get memberlist received nil memberlist, returning empty")
		return &memberlist, unstrucuted.GetResourceVersion(), nil
	}
	cast_members := members.([]interface{})
	for _, member := range cast_members {
		member_map, ok := member.(map[string]interface{})
		if !ok {
			return nil, "", errors.New("failed to cast member to map")
		}
		member_id, ok := member_map["member_id"].(string)
		if !ok {
			return nil, "", errors.New("failed to cast member_id to string")
		}
		member := Member{
			id: member_id,
		}
		memberlist = append(memberlist, member)
	}
	return &memberlist, unstrucuted.GetResourceVersion(), nil
}

func (s *CRMemberlistStore) UpdateMemberlist(ctx context.Context, memberlist *Memberlist, resourceVersion string) error {
	gvr := getGvr()
	log.Info("Updating memberlist store", zap.Any("memberlist", memberlist))
	unstructured := memberlistToCr(memberlist, s.coordinatorNamespace, s.memberlistCustomResource, resourceVersion)
	log.Info("Setting memberlist to unstructured object", zap.Any("unstructured", unstructured))
	_, err := s.dynamicClient.Resource(gvr).Namespace("chroma").Update(context.Background(), unstructured, metav1.UpdateOptions{})
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
			"member_id": member.id,
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
