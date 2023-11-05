package memberlist_manager

import (
	"fmt"
	"testing"
	"time"

	"github.com/chroma/chroma-coordinator/internal/utils"
)

func TestNodeWatcher(t *testing.T) {
	// Simple test that ensure that the node watcher is called
	utils.RunKubernetesIntegrationTest(t, func(t *testing.T) {
		fmt.Println("Running node watcher test")
		// Create a kubernetes node watcher
		node_watcher := NewKubernetesWatcher("chroma", "worker")
		// Register a callback that we assert is called
		callback_called := false
		node_watcher.RegisterCallback(func(node_update NodeUpdate) {
			callback_called = true
		})
		// Start the node watcher
		err := node_watcher.Start()
		if err != nil {
			t.Fatal(err)
		}
		// Wait for the callback to be called
		time.Sleep(10 * time.Second)
		if !callback_called {
			t.Fatal("Callback was not called")
		}
		// Stop the node watcher
		err = node_watcher.Stop()
		if err != nil {
			t.Fatal(err)
		}
	})
}
