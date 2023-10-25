package dbcore

import (
	"context"
	"fmt"
	"reflect"

	"github.com/chroma/chroma-coordinator/internal/metastore/db/dbmodel"
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
	DBName       string
	MaxIdleConns int
	MaxOpenConns int
}

func Connect(cfg DBConfig) (*gorm.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8mb4&parseTime=True&loc=Local&tls=true&interpolateParams=true",
		cfg.Username, cfg.Password, cfg.Address, cfg.DBName)

	ormLogger := logger.Default
	ormLogger.LogMode(logger.Info)
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
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

func ConfigDatabaseForTesting() *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	SetGlobalDB(db)

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
	return db
}
