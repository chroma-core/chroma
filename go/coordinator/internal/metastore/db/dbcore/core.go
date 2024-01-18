package dbcore

import (
	"context"
	"fmt"
	"gorm.io/driver/postgres"
	"reflect"

	"github.com/chroma/chroma-coordinator/internal/common"
	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
	"github.com/chroma/chroma-coordinator/internal/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
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
}

func ConnectTiDB(cfg DBConfig) *gorm.DB {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&tls=%s&parseTime=true",
		cfg.Username, cfg.Password, cfg.Address, cfg.Port, cfg.DBName, "false")

	ormLogger := logger.Default
	ormLogger.LogMode(logger.Info)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})

	if err != nil {
		panic(err)
	}

	log.Info("TiDB connected success",
		zap.String("host", cfg.Address),
		zap.String("database", cfg.DBName),
		zap.Error(err))

	globalDB = db

	log.Info("TiDB: AutoMigrate")
	db.AutoMigrate(&dbmodel.Tenant{})
	db.AutoMigrate(&dbmodel.Database{})
	db.AutoMigrate(&dbmodel.Collection{})
	db.AutoMigrate(&dbmodel.CollectionMetadata{})
	db.AutoMigrate(&dbmodel.Segment{})
	db.AutoMigrate(&dbmodel.SegmentMetadata{})
	db.AutoMigrate(&dbmodel.Notification{})

	return db
}

func ConnectPostgres(cfg DBConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%d sslmode=require",
		cfg.Address, cfg.Username, cfg.Password, cfg.DBName, cfg.Port)

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

	log.Info("db connected success",
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

func ConfigTiDBForTesting() *gorm.DB {
	dBConfig := DBConfig{
		Username: "root",
		Password: "emuY1ktyq5Tq4nGx",
		Address:  "tidb.ootbbu125szh.clusters.tidb-cloud.com",
		Port:     4000,
		DBName:   "test",
	}
	db := ConnectTiDB(dBConfig)
	CreateTestTables(db)
	return db
}

func CreateTestTables(db *gorm.DB) {
	// Setup tenant related tables
	db.Migrator().DropTable(&dbmodel.Tenant{})
	db.Migrator().CreateTable(&dbmodel.Tenant{})
	db.Model(&dbmodel.Tenant{}).Create(&dbmodel.Tenant{
		ID: common.DefaultTenant,
	})

	// Setup database related tables
	db.Migrator().DropTable(&dbmodel.Database{})
	db.Migrator().CreateTable(&dbmodel.Database{})

	db.Model(&dbmodel.Database{}).Create(&dbmodel.Database{
		ID:       types.NilUniqueID().String(),
		Name:     common.DefaultDatabase,
		TenantID: common.DefaultTenant,
	})

	// Setup collection related tables
	db.Migrator().DropTable(&dbmodel.Collection{})
	db.Migrator().DropTable(&dbmodel.CollectionMetadata{})
	db.Migrator().CreateTable(&dbmodel.Collection{})
	db.Migrator().CreateTable(&dbmodel.CollectionMetadata{})

	// Setup segment related tables
	db.Migrator().DropTable(&dbmodel.Segment{})
	db.Migrator().DropTable(&dbmodel.SegmentMetadata{})
	db.Migrator().CreateTable(&dbmodel.Segment{})
	db.Migrator().CreateTable(&dbmodel.SegmentMetadata{})

	// Setup notification related tables
	db.Migrator().DropTable(&dbmodel.Notification{})
	db.Migrator().CreateTable(&dbmodel.Notification{})
}

func ConfigDatabaseForTesting() *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic("failed to connect database")
	}
	SetGlobalDB(db)
	CreateTestTables(db)
	return db
}
