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

func (m *MemberlistManager) run() {
	count := uint(0)
	updates := make(map[string]Status)
	lastUpdate := time.Now()
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
		updates[key] = nodeUpdate
		count++
		if err != nil {
			log.Error("Error while getting status of node", zap.Error(err))
			m.workqueue.Done(key)
			continue
		}
		if count >= m.reconcileCount || time.Since(lastUpdate) > m.reconcileInterval {
			memberlist, resourceVersion, err := m.getOldMemberlist()
			if err != nil {
				log.Error("Error while getting memberlist", zap.Error(err))
				continue
			}
			newMemberlist, err := reconcileBatch(memberlist, updates)
			if err != nil {
				log.Error("Error while reconciling memberlist", zap.Error(err))
				continue
			}
			err = m.updateMemberlist(newMemberlist, *resourceVersion)
			if err != nil {
				log.Error("Error while updating memberlist", zap.Error(err))
				continue
			}

			for key := range updates {
				m.workqueue.Done(key)
			}
			updates = make(map[string]Status)
			count = uint(0)
			lastUpdate = time.Now()
		}
	}
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

func reconcileBatch(memberlist Memberlist, updates map[string]Status) (Memberlist, error) {
	newMemberlist := Memberlist{}
	exists := map[string]bool{}
	for _, node := range memberlist {
		if status, ok := updates[node]; ok {
			if status == Ready {
				newMemberlist = append(newMemberlist, node)
			}
			exists[node] = true
		} else {
			newMemberlist = append(newMemberlist, node)
		}
	}
	for node, status := range updates {
		if _, ok := exists[node]; !ok && status == Ready {
			newMemberlist = append(newMemberlist, node)
		}
	}
	log.Info("Getting new memberlist", zap.Any("newMemberlist", newMemberlist))
	return newMemberlist, nil
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
