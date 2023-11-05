package memberlist_manager

import (
	"fmt"

	"k8s.io/client-go/util/workqueue"
)

// TODO:
/*

- Switch to camel case for all variables and methods

*/

// A memberlist manager is responsible for managing the memberlist for a
// coordinator. A memberlist is a CR in the coordinator's namespace that
// contains a list of all the members in the coordinator's cluster.
// The memberlist uses k8s watch to monitor changes to pods and then updates a CR

type IMemberlistManager interface {
	Start() error
	Stop() error
}

type MemberlistManager struct {
	workqueue        workqueue.RateLimitingInterface // workqueue for the coordinator
	node_watcher     IWatcher                        // node watcher for the coordinator
	memberlist_store IMemberlistStore                // memberlist store for the coordinator
}

func NewMemberlistManager(node_watcher IWatcher, memberlist_store IMemberlistStore) *MemberlistManager {
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	return &MemberlistManager{
		workqueue:        queue,
		node_watcher:     node_watcher,
		memberlist_store: memberlist_store,
	}
}

func (m *MemberlistManager) Start() error {
	m.node_watcher.RegisterCallback(func(node_update NodeUpdate) {
		m.workqueue.Add(node_update)
	})
	err := m.node_watcher.Start()
	if err != nil {
		return err
	}
	go m.run()
	return nil
}

func (m *MemberlistManager) run() {
	for {
		key, shutdown := m.workqueue.Get()
		if shutdown {
			fmt.Println("Shutting down memberlist manager")
			break
		}
		fmt.Printf("Update HERE: %s\n", key.(NodeUpdate).node_ip)
		// TODO: use cache instead of storing the memberlist on the queue
		nodeUpdate := key.(NodeUpdate)
		m.reconcile(&nodeUpdate)
		m.workqueue.Done(key)
	}
}

func (m *MemberlistManager) reconcile(node_update *NodeUpdate) error {
	memberlist, err := m.memberlist_store.GetMemberlist()
	fmt.Printf("Current memberlist: %v\n", memberlist)
	if err != nil {
		return err
	}
	exists := false
	new_memberlist := Memberlist{}
	for _, node := range memberlist.Nodes {
		if node.GetIP() == node_update.GetIP() {
			if node_update.status == Ready {
				new_memberlist.Nodes = append(new_memberlist.Nodes, node_update)
			}
			exists = true
		}
	}
	if !exists && node_update.status == Ready {
		fmt.Printf("Adding node: %s\n", node_update.GetIP())
		new_memberlist.Nodes = append(new_memberlist.Nodes, node_update)
	}
	fmt.Printf("Updated memberlist: %v\n", new_memberlist.Nodes[0])
	return m.memberlist_store.UpdateMemberlist(new_memberlist)
}

func (m *MemberlistManager) Stop() error {
	m.workqueue.ShutDown()
	return nil
}
