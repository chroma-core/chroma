package dbcore

import (
	"context"
	"fmt"
	"github.com/chroma-core/chroma/go/pkg/logservice/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/docker/go-connections/nat"
	"github.com/pingcap/log"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"strconv"

	"gorm.io/gorm"
	"time"
)

var (
	globalDB *gorm.DB
)

type DBConfig struct {
	Username     string
	Password     string
	Address      string
	Port         int
	DBName       string
	MaxIdleConns int
	MaxOpenConns int
	SslMode      string
}

// SetGlobalDB Only for test
func SetGlobalDB(db *gorm.DB) {
	globalDB = db
}

func CreateTestTables(db *gorm.DB) {
	log.Info("CreateTestTables")
	db.AutoMigrate(&dbmodel.RecordLog{}, &dbmodel.CollectionPosition{})
}

func StartPgContainer(ctx context.Context) (config dbcore.DBConfig, err error) {
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
	port, _ := strconv.Atoi(ports["5432/tcp"][0].HostPort)
	config = dbcore.DBConfig{
		Username: dbUsername,
		Password: dbPassword,
		Address:  "localhost",
		Port:     port,
		DBName:   dbName,
	}
	return
}

func ConfigDatabaseForTesting() (db *gorm.DB, err error) {
	var config dbcore.DBConfig
	config, err = StartPgContainer(context.Background())
	if err != nil {
		return
	}
	db, err = dbcore.ConnectPostgres(config)
	if err != nil {
		return
	}
	SetGlobalDB(db)
	CreateTestTables(db)
	return
}
