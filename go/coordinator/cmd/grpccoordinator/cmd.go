package grpccoordinator

import (
	"io"

	"github.com/chroma/chroma-coordinator/cmd/flag"
	"github.com/chroma/chroma-coordinator/internal/grpccoordinator"
	"github.com/chroma/chroma-coordinator/internal/utils"
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
	utils.RunProcess(func() (io.Closer, error) {
		return grpccoordinator.New(conf)
	})
}
