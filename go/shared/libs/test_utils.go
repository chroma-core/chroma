package libs

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"os/exec"
	"path"
	"runtime"
	"time"
)

func StartPgContainer(ctx context.Context) (connectionString string, err error) {
	var container *postgres.PostgresContainer
	dbName := "chroma"
	dbUsername := "chroma"
	dbPassword := "chroma"
	container, err = postgres.RunContainer(ctx,
		testcontainers.WithImage("docker.io/postgres:15.2-alpine"),
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUsername),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)
	if err != nil {
		return
	}
	var ports nat.PortMap
	ports, err = container.Ports(ctx)
	if err != nil {
		return
	}
	if _, ok := ports["5432/tcp"]; !ok {
		err = fmt.Errorf("test")
	}
	port := ports["5432/tcp"][0].HostPort
	connectionString = fmt.Sprintf("postgres://chroma:chroma@localhost:%s/chroma?sslmode=disable", port)
	return
}

func RunMigration(ctx context.Context, connectionString string) (err error) {
	cmd := exec.Command("/bin/sh", "bin/migrate_up_test.sh", connectionString)
	_, dir, _, _ := runtime.Caller(0)
	cmd.Dir = path.Join(dir, "../../../")
	var byte []byte
	byte, err = cmd.CombinedOutput()
	fmt.Println(string(byte))
	return
}
