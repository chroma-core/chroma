package memberlist_manager

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"
)

type INode interface {
	GetIP() string
}

type Memberlist struct {
	Nodes []INode
}

type IMemberlistStore interface {
	GetMemberlist() (Memberlist, error)
	UpdateMemberlist(memberlist Memberlist) error
}

// A mock memberlist store that stores the memberlist in memory
type MockMemberlistStore struct {
	memberlist Memberlist
}

func NewMockMemberlistStore() *MockMemberlistStore {
	return &MockMemberlistStore{memberlist: Memberlist{}}
}

// Get the current memberlist or error
func (s *MockMemberlistStore) GetMemberlist() (Memberlist, error) {
	return s.memberlist, nil
}

// Update the memberlist
func (s *MockMemberlistStore) UpdateMemberlist(memberlist Memberlist) error {
	// passes the memberlist by value so that the memberlist is copied, this is to prevent the memberlist from being modified
	// outside of the memberlist manager. Since it will be small, this should not be a problem
	s.memberlist = memberlist
	return nil
}

type CRMemberlistStore struct {
	dynamic_client             dynamic.Interface
	coordinator_namespace      string
	memberlist_custom_resource string
}

func NewCRMemberlistStore(coordinator_namespace string, memberlist_custom_resource string) *CRMemberlistStore {
	// Load the default kubeconfig file
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	config, err := loadingRules.Load()

	clientConfig, err := clientcmd.NewDefaultClientConfig(*config, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		panic(err.Error())
	}

	// Create the dynamic client for the memberlist custom resource
	dynamic_client, err := dynamic.NewForConfig(clientConfig)
	if err != nil {
		panic(err.Error())
	}

	return &CRMemberlistStore{
		dynamic_client:             dynamic_client,
		coordinator_namespace:      coordinator_namespace,
		memberlist_custom_resource: memberlist_custom_resource,
	}
}

// Get the current memberlist or error
func (s *CRMemberlistStore) GetMemberlist() (Memberlist, error) {
	gvr := schema.GroupVersionResource{Group: "chroma.cluster", Version: "v1", Resource: "memberlists"}
	unstrucuted, err := s.dynamic_client.Resource(gvr).Namespace("chroma").Get(context.TODO(), "worker-memberlist", metav1.GetOptions{}) //.Namespace(m.coordinator_namespace).Get(context.TODO(), m.memberlist_custom_resource, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	fmt.Println("Memberlist CR: ")
	fmt.Println(unstrucuted.UnstructuredContent())
	return Memberlist{}, nil
}
