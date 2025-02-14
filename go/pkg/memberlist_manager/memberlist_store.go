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
	GetMemberlist(ctx context.Context) (_ Memberlist, resourceVersion string, err error)
	UpdateMemberlist(ctx context.Context, _ Memberlist, resourceVersion string) error
}

type Member struct {
	id string
	ip string
	node string
}

// MarshalLogObject implements the zapcore.ObjectMarshaler interface
func (m Member) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("id", m.id)
	enc.AddString("ip", m.ip)
	enc.AddString("node", m.node)
	return nil
}

type Memberlist []Member

func (p Memberlist) Len() int           { return len(p) }
func (p Memberlist) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Memberlist) Less(i, j int) bool { return p[i].id < p[j].id }

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

func (s *CRMemberlistStore) GetMemberlist(ctx context.Context) (return_memberlist Memberlist, resourceVersion string, err error) {
	gvr := getGvr()
	unstrucuted, err := s.dynamicClient.Resource(gvr).Namespace(s.coordinatorNamespace).Get(ctx, s.memberlistCustomResource, metav1.GetOptions{})
	if err != nil {
		return nil, "", err
	}
	cr := unstrucuted.UnstructuredContent()
	log.Debug("Got unstructured memberlist object", zap.Any("cr", cr))
	members := cr["spec"].(map[string]interface{})["members"]
	if members == nil {
		// Empty memberlist
		log.Debug("Get memberlist received nil memberlist, returning empty")
		return nil, unstrucuted.GetResourceVersion(), nil
	}
	cast_members := members.([]interface{})
	memberlist := make(Memberlist, 0, len(cast_members))

	for _, member := range cast_members {
		member_map, ok := member.(map[string]interface{})
		if !ok {
			return nil, "", errors.New("failed to cast member to map")
		}
		member_id, ok := member_map["member_id"].(string)
		if !ok {
			return nil, "", errors.New("failed to cast member_id to string")
		}
		// If member_ip is in the CR, extract it, otherwise set it to empty string
		// This is for backwards compatibility with older CRs that don't have member_ip
		member_ip, ok := member_map["member_ip"].(string)
		if !ok {
			member_ip = ""
		}
		// If the member_node_name is in the CR, extract it, otherwise set it to empty string
		// This is for backwards compatibility with older CRs that don't have member_node_name
		member_node_name, ok := member_map["member_node_name"].(string)
		if !ok {
			member_node_name = ""
		}

		memberlist = append(memberlist, Member{member_id, member_ip, member_node_name})
	}
	return memberlist, unstrucuted.GetResourceVersion(), nil
}

func (s *CRMemberlistStore) UpdateMemberlist(ctx context.Context, memberlist Memberlist, resourceVersion string) error {
	gvr := getGvr()
	log.Debug("Updating memberlist store", zap.Any("memberlist", memberlist))
	unstructured := memberlist.toCr(s.coordinatorNamespace, s.memberlistCustomResource, resourceVersion)
	log.Debug("Setting memberlist to unstructured object", zap.Any("unstructured", unstructured))
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

func (list Memberlist) toCr(namespace string, memberlistName string, resourceVersion string) *unstructured.Unstructured {
	members := make([]interface{}, len(list))
	for i, member := range list {
		members[i] = map[string]interface{}{
			"member_id": member.id,
			"member_ip": member.ip,
			"member_node_name": member.node,
		}
	}

	return &unstructured.Unstructured{
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
}
