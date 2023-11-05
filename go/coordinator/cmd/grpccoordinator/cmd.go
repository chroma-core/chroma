package grpccoordinator

import (
	"fmt"
	"time"

	"github.com/chroma/chroma-coordinator/cmd/flag"
	"github.com/chroma/chroma-coordinator/internal/grpccoordinator"
	"github.com/chroma/chroma-coordinator/internal/memberlist_manager"

	"github.com/spf13/cobra"
)

var (
	conf = grpccoordinator.Config{}

	Cmd = &cobra.Command{
		Use:   "coordinator",
		Short: "Start a coordinator",
		Long:  `Long description`,
		Run:   exec,
	}
)

func init() {
	flag.GRPCAddr(Cmd, &conf.BindAddress)
	Cmd.Flags().StringVar(&conf.SystemCatalogProvider, "system-catalog-provider", "memory", "System catalog provider")
	Cmd.Flags().StringVar(&conf.Username, "username", "root", "MetaTable username")
	Cmd.Flags().StringVar(&conf.Password, "password", "", "MetaTable password")
	Cmd.Flags().StringVar(&conf.Address, "db-address", "127.0.0.1:3306", "MetaTable db address")
	Cmd.Flags().StringVar(&conf.DBName, "db-name", "", "MetaTable db name")
	Cmd.Flags().IntVar(&conf.MaxIdleConns, "max-idle-conns", 10, "MetaTable max idle connections")
	Cmd.Flags().IntVar(&conf.MaxOpenConns, "max-open-conns", 10, "MetaTable max open connections")
}

func exec(*cobra.Command, []string) {
	// utils.RunProcess(func() (io.Closer, error) {
	// 	return grpccoordinator.New(conf)
	// })
	// Create a memberlist manager
	// memberlist_manager := memberlist_manager.NewMemberlistManager("member-type=worker", "chroma", "worker-memberlist")
	// Run the memberlist manager
	// memberlist_manager.Start()
	// Create a node watcher
	node_watcher := memberlist_manager.NewKubernetesWatcher("chroma", "worker")
	// Create a mock memberlist store
	memberlist_store := memberlist_manager.NewMockMemberlistStore()
	// Create a memberlist manager with the node watcher and memberlist store
	memberlist_manager := memberlist_manager.NewMemberlistManager(node_watcher, memberlist_store)
	// Run the memberlist manager
	memberlist_manager.Start()
	// Sleep for 10 seconds
	time.Sleep(10 * time.Second)
	// Print the memberlist
	memberlist, err := memberlist_store.GetMemberlist()
	fmt.Printf("After getting memberlist: %v\n", memberlist)
	if err != nil {
		panic(err)
	}
	for _, node := range memberlist.Nodes {
		println(node.GetIP())
	}
}
