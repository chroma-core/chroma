package daotest

import (
	"time"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// defaults for a test collection
var (
	defaultConfigurationJsonStr       = "{\"a\": \"param\", \"b\": \"param2\", \"3\": true}"
	defaultSchemaStr                  = "{\"a\": \"param\", \"b\": \"param2\", \"3\": true}"
	defaultDimension                  = int32(128)
	defaultTotalRecordsPostCompaction = uint64(100)
	defaultSizeBytesPostCompaction    = uint64(500000)
	defaultLastCompactionTimeSecs     = uint64(1741037006)
)

// TestDatabaseID is the default database ID for a test collection. It's exported because it's so frequently used
// and can be the same across most tests.
const TestDatabaseID = "test_database"

// TestTenantID is the default tenant ID for a test collection. It's exported because it's so frequently used
// and can be the same across most tests.
const TestTenantID = "test_tenant"

// NewDefaultTestCollection is a "shim" for callsites that existed before dao.CreateTestCollection was refactored to
// take a dbmodel.Collection instead of a subset of its fields as arguments. It should not be used for new tests.
func NewDefaultTestCollection(collectionName string, dimension int32, databaseID string, lineageFileName *string) *dbmodel.Collection {
	log.Info("new default test collection", zap.String("collectionName", collectionName), zap.Int32("dimension", dimension), zap.String("databaseID", databaseID))
	return NewTestCollection(
		TestTenantID,
		databaseID,
		collectionName,
		WithDimension(dimension),
		WithTotalRecordsPostCompaction(defaultTotalRecordsPostCompaction),
		WithSizeBytesPostCompaction(defaultSizeBytesPostCompaction),
		WithLastCompactionTimeSecs(defaultLastCompactionTimeSecs),
		WithLineageFileName(lineageFileName),
	)
}

// NewTestCollection creates a new test collection with the given name, database ID, and tenant ID.
// Name, databaseID, and tenantID are required, other fields have defaults but can be overridden with
// option function of a similar name.
// Note: collection.CreatedAt is set to the current time, but collection.UpdatedAt is not set.
func NewTestCollection(tenantID, databaseID, collectionName string, options ...TestCollectionOption) *dbmodel.Collection {
	log.Info("new test collection", zap.String("tenantID", tenantID), zap.String("databaseID", databaseID), zap.String("collectionName", collectionName))
	collectionId := types.NewUniqueID().String()

	collection := &dbmodel.Collection{
		ID:                   collectionId,
		Name:                 &collectionName,
		ConfigurationJsonStr: &defaultConfigurationJsonStr,
		SchemaStr:            &defaultSchemaStr,
		Dimension:            &defaultDimension,
		DatabaseID:           databaseID,
		CreatedAt:            time.Now(),
		Tenant:               tenantID,
	}

	for _, option := range options {
		option(collection)
	}

	return collection
}

type TestCollectionOption func(*dbmodel.Collection)

func WithConfigurationJsonStr(configurationJsonStr string) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.ConfigurationJsonStr = &configurationJsonStr
	}
}

func WithSchemaStr(schemaStr string) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.SchemaStr = &schemaStr
	}
}

func WithDimension(dimension int32) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.Dimension = &dimension
	}
}

func WithTs(ts int64) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.Ts = ts
	}
}

func WithIsDeleted(isDeleted bool) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.IsDeleted = isDeleted
	}
}

func WithCreatedAt(createdAt time.Time) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.CreatedAt = createdAt
	}
}

func WithUpdatedAt(updatedAt time.Time) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.UpdatedAt = updatedAt
	}
}

func WithLogPosition(logPosition int64) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.LogPosition = logPosition
	}
}

func WithVersion(version int32) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.Version = version
	}
}

func WithVersionFileName(versionFileName string) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.VersionFileName = versionFileName
	}
}

func WithRootCollectionID(rootCollectionId string) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.RootCollectionId = &rootCollectionId
	}
}

func WithLineageFileName(lineageFileName *string) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.LineageFileName = lineageFileName
	}
}

func WithTotalRecordsPostCompaction(totalRecordsPostCompaction uint64) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.TotalRecordsPostCompaction = totalRecordsPostCompaction
	}
}

func WithSizeBytesPostCompaction(sizeBytesPostCompaction uint64) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.SizeBytesPostCompaction = sizeBytesPostCompaction
	}
}

func WithLastCompactionTimeSecs(lastCompactionTimeSecs uint64) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.LastCompactionTimeSecs = lastCompactionTimeSecs
	}
}

func WithNumVersions(numVersions uint32) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.NumVersions = numVersions
	}
}

func WithOldestVersionTs(oldestVersionTs time.Time) func(*dbmodel.Collection) {
	return func(collection *dbmodel.Collection) {
		collection.OldestVersionTs = oldestVersionTs
	}
}
