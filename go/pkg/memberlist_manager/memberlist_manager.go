package memberlist_manager

import (
	"context"
	"errors"
	"time"

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
	workqueue         workqueue.RateLimitingInterface // workqueue for the coordinator
	nodeWatcher       IWatcher                        // node watcher for the coordinator
	memberlistStore   IMemberlistStore                // memberlist store for the coordinator
	reconcileInterval time.Duration                   // interval for reconciliation
	reconcileCount    uint                            // number of updates to reconcile at once
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

func (m *MemberlistManager) reconcileMemberlist(updates map[string]bool) {
	memberlist, resourceVersion, err := m.getOldMemberlist()
	if err != nil {
		log.Error("Error while getting memberlist", zap.Error(err))
		return
	}
	log.Debug("Old Memberlist", zap.Any("memberlist", memberlist))
	newMemberlist, err := m.nodeWatcher.ListReadyMembers()
	if err != nil {
		log.Error("Error while getting ready members", zap.Error(err))
		return
	}
	// do not update memberlist if there's no change
	if !memberlistSame(memberlist, newMemberlist) {
		err = m.updateMemberlist(newMemberlist, *resourceVersion)
		if err != nil {
			log.Error("Error while updating memberlist", zap.Error(err))
			return
		}
	} else {
		log.Debug("Memberlist has not changed")
	}
	for key := range updates {
		m.workqueue.Done(key)
	}
}

func (m *MemberlistManager) run() {
	count := uint(0)
	updates := map[string]bool{}
	shutdownChan := make(chan struct{})
	eventChan := make(chan string)
	ticker := time.NewTicker(m.reconcileInterval)
	go func() {
		for {
			interface_key, shutdown := m.workqueue.Get()
			if shutdown {
				log.Info("Shutting down memberlist manager")
				shutdownChan <- struct{}{}
				break
			}
			key, ok := interface_key.(string)
			log.Debug("Reconciling memberlist", zap.String("key", key))
			if !ok {
				log.Error("Error while asserting workqueue key to string")
				m.workqueue.Done(key)
			}
			eventChan <- key
		}
	}()

	for {
		select {
		case key := <-eventChan:
			count++
			updates[key] = true
			if count >= m.reconcileCount {
				m.reconcileMemberlist(updates)
				count = uint(0)
				updates = map[string]bool{}
			}
		case <-shutdownChan:
			return
		case <-ticker.C:
			m.reconcileMemberlist(updates)
			count = uint(0)
			updates = map[string]bool{}
		}
	}
}

func memberlistSame(oldMemberlist Memberlist, newMemberlist Memberlist) bool {
	if len(oldMemberlist) != len(newMemberlist) {
		return false
	}
	// use a map to check if the new memberlist contains all the old members
	newMemberlistMap := make(map[string]bool)
	for _, member := range newMemberlist {
		newMemberlistMap[member.id] = true
	}
	for _, member := range oldMemberlist {
		if _, ok := newMemberlistMap[member.id]; !ok {
			return false
		}
	}
	return true
}

func (m *MemberlistManager) getOldMemberlist() (Memberlist, *string, error) {
	memberlist, resourceVersion, err := m.memberlistStore.GetMemberlist(context.Background())
	if err != nil {
		return nil, nil, err
	}
	if memberlist == nil {
		return nil, nil, errors.New("Memberlist recieved is nil")
	}
	return *memberlist, &resourceVersion, nil
}

func (m *MemberlistManager) updateMemberlist(memberlist Memberlist, resourceVersion string) error {
	return m.memberlistStore.UpdateMemberlist(context.Background(), &memberlist, resourceVersion)
}

func (m *MemberlistManager) SetReconcileInterval(interval time.Duration) {
	m.reconcileInterval = interval
}

func (m *MemberlistManager) SetReconcileCount(count uint) {
	m.reconcileCount = count
}

func (m *MemberlistManager) Stop() error {
	m.workqueue.ShutDown()
	return nil
}
