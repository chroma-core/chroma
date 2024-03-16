package dbcore

import (
	"context"
	"fmt"
	"github.com/chroma-core/chroma/go/pkg/coordinator/ent"
	"github.com/chroma-core/chroma/go/pkg/types"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	_ "github.com/lib/pq"
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

func ConnectPostgres(cfg DBConfig) (*gorm.DB, error) {
	log.Info("ConnectPostgres", zap.String("host", cfg.Address), zap.String("database", cfg.DBName), zap.Int("port", cfg.Port))
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=%s",
		cfg.Address, cfg.Username, cfg.Password, cfg.DBName, cfg.Port, cfg.SslMode)

	ormLogger := logger.Default
	ormLogger.LogMode(logger.Info)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger:          ormLogger,
		CreateBatchSize: 100,
	})
	if err != nil {
		log.Error("fail to connect db",
			zap.String("host", cfg.Address),
			zap.String("database", cfg.DBName),
			zap.Error(err))
		return nil, err
	}

	idb, err := db.DB()
	if err != nil {
		log.Error("fail to create db instance",
			zap.String("host", cfg.Address),
			zap.String("database", cfg.DBName),
			zap.Error(err))
		return nil, err
	}
	idb.SetMaxIdleConns(cfg.MaxIdleConns)
	idb.SetMaxOpenConns(cfg.MaxOpenConns)

	globalDB = db

	log.Info("Postgres connected success",
		zap.String("host", cfg.Address),
		zap.String("database", cfg.DBName),
		zap.Error(err))

	return db, nil
}

// SetGlobalDB Only for test
func SetGlobalDB(db *gorm.DB) {
	globalDB = db
}

type ctxTransactionKey struct{}

func CtxWithTransaction(ctx context.Context, tx *gorm.DB) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, ctxTransactionKey{}, tx)
}

type txImpl struct{}

func NewTxImpl() *txImpl {
	return &txImpl{}
}

func (*txImpl) Transaction(ctx context.Context, fn func(txctx context.Context) error) error {
	db := globalDB.WithContext(ctx)

	return db.Transaction(func(tx *gorm.DB) error {
		txCtx := CtxWithTransaction(ctx, tx)
		return fn(txCtx)
	})
}

func GetDB(ctx context.Context) *gorm.DB {
	iface := ctx.Value(ctxTransactionKey{})

	if iface != nil {
		tx, ok := iface.(*gorm.DB)
		if !ok {
			log.Error("unexpect context value type", zap.Any("type", reflect.TypeOf(tx)))
			return nil
		}

		return tx
	}

	return globalDB.WithContext(ctx)
}

func CreateDefaultTenantAndDatabase(db *gorm.DB) string {
	defaultTenant := &dbmodel.Tenant{
		ID:                 common.DefaultTenant,
		LastCompactionTime: time.Now().Unix(),
	}
	db.Model(&dbmodel.Tenant{}).Where("id = ?", common.DefaultTenant).Save(defaultTenant)

	var database []dbmodel.Database
	databaseId := types.NewUniqueID().String()
	result := db.Model(&dbmodel.Database{}).
		Where("name = ?", common.DefaultDatabase).
		Where("tenant_id = ?", common.DefaultTenant).
		Find(&database)
	if result.Error != nil {
		return ""
	}

	if result.RowsAffected == 0 {
		db.Create(&dbmodel.Database{
			ID:       databaseId,
			Name:     common.DefaultDatabase,
			TenantID: common.DefaultTenant,
		})
		return databaseId
	}

	err := result.Row().Scan(&database)
	if err != nil {
		return ""
	}
	return database[0].ID
}

func CreateTestTables(db *gorm.DB) {
	log.Info("CreateTestTables")
	tableExist := db.Migrator().HasTable(&dbmodel.Tenant{})
	if !tableExist {
		db.Migrator().CreateTable(&dbmodel.Tenant{})
	}
	tableExist = db.Migrator().HasTable(&dbmodel.Database{})
	if !tableExist {
		db.Migrator().CreateTable(&dbmodel.Database{})
	}
	tableExist = db.Migrator().HasTable(&dbmodel.CollectionMetadata{})
	if !tableExist {
		db.Migrator().CreateTable(&dbmodel.CollectionMetadata{})
	}
	tableExist = db.Migrator().HasTable(&dbmodel.Collection{})
	if !tableExist {
		db.Migrator().CreateTable(&dbmodel.Collection{})
	}
	tableExist = db.Migrator().HasTable(&dbmodel.SegmentMetadata{})
	if !tableExist {
		db.Migrator().CreateTable(&dbmodel.SegmentMetadata{})
	}
	tableExist = db.Migrator().HasTable(&dbmodel.Segment{})
	if !tableExist {
		db.Migrator().CreateTable(&dbmodel.Segment{})
	}
	tableExist = db.Migrator().HasTable(&dbmodel.Notification{})
	if !tableExist {
		db.Migrator().CreateTable(&dbmodel.Notification{})
	}

	// create default tenant and database
	CreateDefaultTenantAndDatabase(db)
}

func GetDBConfigForTesting() DBConfig {
	dbAddress := os.Getenv("POSTGRES_HOST")
	dbPort, _ := strconv.Atoi(os.Getenv("POSTGRES_PORT"))
	return DBConfig{
		Username:     "chroma",
		Password:     "chroma",
		Address:      dbAddress,
		Port:         dbPort,
		DBName:       "chroma",
		MaxIdleConns: 10,
		MaxOpenConns: 100,
		SslMode:      "disable",
	}
}

func ConfigEntClientForTesting() (*ent.Client, error) {
	dbConfig := GetDBConfigForTesting()
	clientDataSource := fmt.Sprintf("host=%s port=%d user=%s dbname=%s password=%s sslmode=disable",
		dbConfig.Address, dbConfig.Port, dbConfig.Username, dbConfig.DBName, dbConfig.Password)
	log.Info("ConfigEntClientForTesting", zap.String("clientDataSource", clientDataSource))
	client, err := ent.Open("postgres", clientDataSource)
	if err != nil {
		log.Error("failed opening connection to postgres", zap.Error(err))
		return nil, err
	}
	err = client.Schema.Create(context.Background())
	if err != nil {
		log.Error("failed creating schema resources", zap.Error(err))
		return nil, err
	}
	return client, err
}

func ConfigDatabaseForTesting() *gorm.DB {
	db, err := ConnectPostgres(GetDBConfigForTesting())
	if err != nil {
		panic("failed to connect database")
	}
	SetGlobalDB(db)
	CreateTestTables(db)
	return db
}
