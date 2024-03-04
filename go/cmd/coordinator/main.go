package main

import (
	"fmt"
	"os"

	"github.com/chroma-core/chroma/go/internal/utils"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"go.uber.org/automaxprocs/maxprocs"
)

var (
	rootCmd = &cobra.Command{
		Use:   "chroma",
		Short: "Chroma root command",
		Long:  `Chroma root command`,
	}
)

func init() {
	rootCmd.AddCommand(Cmd)
}

func main() {
	utils.LogLevel = zerolog.DebugLevel
	utils.ConfigureLogger()
	if _, err := maxprocs.Set(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := rootCmd.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
