package memberlist_manager

import (
	"context"
	"errors"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"k8s.io/client-go/util/workqueue"
)

// A memberlist manager is responsible for managing the memberlist for a
// coordinator. A memberlist consists of a store and a watcher. The store
// is responsible for storing the memberlist in a persistent store, and the
// watcher is responsible for watching the nodes in the cluster and updating
// the store accordingly. Concretely, the memberlist manager reconciles between these
// and the store is backed by a Kubernetes custom resource, and the watcher is a
// kubernetes watch on pods with a given label.

type IMemberlistManager interface {
	common.Component
}

type MemberlistManager struct {
	workqueue       workqueue.RateLimitingInterface // workqueue for the coordinator
	nodeWatcher     IWatcher                        // node watcher for the coordinator
	memberlistStore IMemberlistStore                // memberlist store for the coordinator
}

func NewMemberlistManager(nodeWatcher IWatcher, memberlistStore IMemberlistStore) *MemberlistManager {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	return &MemberlistManager{
		workqueue:       queue,
		nodeWatcher:     nodeWatcher,
		memberlistStore: memberlistStore,
	}
}

func (m *MemberlistManager) Start() error {
	log.Info("Starting memberlist manager")
	m.nodeWatcher.RegisterCallback(func(nodeIp string) {
		m.workqueue.Add(nodeIp)
	})
	err := m.nodeWatcher.Start()
	if err != nil {
		return err
	}
	go m.run()
	return nil
}

func (m *MemberlistManager) run() {
	for {
		interface_key, shutdown := m.workqueue.Get()
		if shutdown {
			log.Info("Shutting down memberlist manager")
			break
		}

		key, ok := interface_key.(string)
		if !ok {
			log.Error("Error while asserting workqueue key to string")
			m.workqueue.Done(key)
			continue
		}

		nodeUpdate, err := m.nodeWatcher.GetStatus(key)
		if err != nil {
			log.Error("Error while getting status of node", zap.Error(err))
			m.workqueue.Done(key)
			continue
		}

		err = m.reconcile(key, nodeUpdate)
		if err != nil {
			log.Error("Error while reconciling memberlist", zap.Error(err))
		}

		m.workqueue.Done(key)
	}
}

func (m *MemberlistManager) reconcile(nodeIp string, status Status) error {
	memberlist, resourceVersion, err := m.memberlistStore.GetMemberlist(context.Background())
	if err != nil {
		return err
	}
	if memberlist == nil {
		return errors.New("Memberlist recieved is nil")
	}
	exists := false
	// Loop through the memberlist and generate a new one based on the update
	// If we find the node in the existing list and the status is Ready, we add it to the new list
	// If we find the node in the existing list and the status is NotReady, we don't add it to the new list
	// If we don't find the node in the existing list and the status is Ready, we add it to the new list
	newMemberlist := Memberlist{}
	for _, node := range *memberlist {
		if node == nodeIp {
			if status == Ready {
				newMemberlist = append(newMemberlist, node)
			}
			// Else here implies the node is not ready, so we don't add it to the new memberlist
			exists = true
		} else {
			// This update doesn't pertains to this node, so we just add it to the new memberlist
			newMemberlist = append(newMemberlist, node)
		}
	}
	if !exists && status == Ready {
		newMemberlist = append(newMemberlist, nodeIp)
	}
	return m.memberlistStore.UpdateMemberlist(context.Background(), &newMemberlist, resourceVersion)
}

func (m *MemberlistManager) Stop() error {
	m.workqueue.ShutDown()
	return nil
}
