package coordinator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel/mocks"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	defaultTenant   = "default_tenant"
	defaultDatabase = "default_database"
)

func TestCatalog_CreateCollection(t *testing.T) {
	// create a mock transaction implementation
	mockTxImpl := &mocks.ITransaction{}

	// create a mock meta domain implementation
	mockMetaDomain := &mocks.IMetaDomain{}

	// create a new catalog instance
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, nil, false)

	// create a mock collection
	metadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	metadata.Add("test_key", &model.CollectionMetadataValueStringType{Value: "test_value"})
	collection := &model.CreateCollection{
		ID:       types.MustParse("00000000-0000-0000-0000-000000000001"),
		Name:     "test_collection",
		Metadata: metadata,
		TenantID: "test_tenant",
	}

	// create a mock timestamp
	ts := types.Timestamp(1234567890)

	// mock the insert collection method
	name := "test_collection"
	mockTxImpl.On("Transaction", context.Background(), mock.Anything).Return(nil)
	mockMetaDomain.On("CollectionDb", context.Background()).Return(&mocks.ICollectionDb{})
	mockMetaDomain.CollectionDb(context.Background()).(*mocks.ICollectionDb).On("Insert", &dbmodel.Collection{
		ID:   "00000000-0000-0000-0000-000000000001",
		Name: &name,
		Ts:   ts,
	}).Return(nil)

	// mock the insert collection metadata method
	testKey := "test_key"
	testValue := "test_value"
	mockMetaDomain.On("CollectionMetadataDb", context.Background()).Return(&mocks.ICollectionMetadataDb{})
	mockMetaDomain.CollectionMetadataDb(context.Background()).(*mocks.ICollectionMetadataDb).On("Insert", []*dbmodel.CollectionMetadata{
		{
			CollectionID: "00000000-0000-0000-0000-000000000001",
			Key:          &testKey,
			StrValue:     &testValue,
			Ts:           ts,
		},
	}).Return(nil)

	// call the CreateCollection method
	_, _, err := catalog.CreateCollection(context.Background(), collection, ts)

	// assert that the method returned no error
	assert.NoError(t, err)

	// assert that the mock methods were called as expected
	mockMetaDomain.AssertExpectations(t)
}

func TestCatalog_GetCollections(t *testing.T) {
	// create a mock meta domain implementation
	mockMetaDomain := &mocks.IMetaDomain{}

	// create a new catalog instance
	catalog := NewTableCatalog(nil, mockMetaDomain, nil, false)

	// create a mock collection ID
	collectionID := types.MustParse("00000000-0000-0000-0000-000000000001")

	// create a mock collection name
	collectionName := "test_collection"

	// create a mock collection and metadata list
	name := "test_collection"
	testKey := "test_key"
	testValue := "test_value"
	collectionConfigurationJsonStr := "{\"a\": \"param\", \"b\": \"param2\", \"3\": true}"
	collectionAndMetadataList := []*dbmodel.CollectionAndMetadata{
		{
			Collection: &dbmodel.Collection{
				ID:                   "00000000-0000-0000-0000-000000000001",
				Name:                 &name,
				ConfigurationJsonStr: &collectionConfigurationJsonStr,
				Ts:                   types.Timestamp(1234567890),
			},
			CollectionMetadata: []*dbmodel.CollectionMetadata{
				{
					CollectionID: "00000000-0000-0000-0000-000000000001",
					Key:          &testKey,
					StrValue:     &testValue,
					Ts:           types.Timestamp(1234567890),
				},
			},
		},
	}

	// mock the get collections method
	mockMetaDomain.On("CollectionDb", context.Background()).Return(&mocks.ICollectionDb{})
	var n *int32
	mockMetaDomain.CollectionDb(context.Background()).(*mocks.ICollectionDb).On("GetCollections", types.FromUniqueID(collectionID), &collectionName, common.DefaultTenant, common.DefaultDatabase, n, n).Return(collectionAndMetadataList, nil)

	// call the GetCollections method
	collections, err := catalog.GetCollections(context.Background(), collectionID, &collectionName, defaultTenant, defaultDatabase, nil, nil)

	// assert that the method returned no error
	assert.NoError(t, err)

	// assert that the collections were returned as expected
	metadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	metadata.Add("test_key", &model.CollectionMetadataValueStringType{Value: "test_value"})
	assert.Equal(t, []*model.Collection{
		{
			ID:                   types.MustParse("00000000-0000-0000-0000-000000000001"),
			Name:                 "test_collection",
			ConfigurationJsonStr: collectionConfigurationJsonStr,
			Ts:                   types.Timestamp(1234567890),
			Metadata:             metadata,
		},
	}, collections)

	// assert that the mock methods were called as expected
	mockMetaDomain.AssertExpectations(t)
}

func TestCatalog_GetCollectionSize(t *testing.T) {
	mockMetaDomain := &mocks.IMetaDomain{}
	catalog := NewTableCatalog(nil, mockMetaDomain, nil, false)
	collectionID := types.MustParse("00000000-0000-0000-0000-000000000001")
	mockMetaDomain.On("CollectionDb", context.Background()).Return(&mocks.ICollectionDb{})
	var total_records_post_compaction uint64 = uint64(100)
	mockMetaDomain.CollectionDb(context.Background()).(*mocks.ICollectionDb).On("GetCollectionSize", *types.FromUniqueID(collectionID)).Return(total_records_post_compaction, nil)
	collection_size, err := catalog.GetCollectionSize(context.Background(), collectionID)

	assert.NoError(t, err)
	assert.Equal(t, total_records_post_compaction, collection_size)
	mockMetaDomain.AssertExpectations(t)
}

type mockS3MetaStore struct {
	mu    sync.RWMutex
	files map[string]*coordinatorpb.CollectionVersionFile
}

func newMockS3MetaStore() *mockS3MetaStore {
	return &mockS3MetaStore{
		files: make(map[string]*coordinatorpb.CollectionVersionFile),
	}
}

func (m *mockS3MetaStore) GetVersionFile(tenantID, collectionID string, version int64, fileName string) (*coordinatorpb.CollectionVersionFile, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if file, exists := m.files[fileName]; exists {
		return file, nil
	}
	return &coordinatorpb.CollectionVersionFile{
		VersionHistory: &coordinatorpb.CollectionVersionHistory{
			Versions: []*coordinatorpb.CollectionVersionInfo{},
		},
	}, nil
}

func (m *mockS3MetaStore) PutVersionFile(tenantID, databaseID, collectionID, fileName string, file *coordinatorpb.CollectionVersionFile) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.files[fileName] = file
	return fileName, nil
}

func (m *mockS3MetaStore) HasObjectWithPrefix(ctx context.Context, prefix string) (bool, error) {
	return false, nil
}

func (m *mockS3MetaStore) DeleteVersionFile(tenantID, databaseID, collectionID, fileName string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.files, fileName)
	return nil
}

func TestCatalog_FlushCollectionCompactionForVersionedCollection(t *testing.T) {
	// Create mocks
	mockTxImpl := &mocks.ITransaction{}
	mockMetaDomain := &mocks.IMetaDomain{}
	mockCollectionDb := &mocks.ICollectionDb{}
	mockTenantDb := &mocks.ITenantDb{}
	mockSegmentDb := &mocks.ISegmentDb{}
	mockS3Store := newMockS3MetaStore()

	// Create catalog with version file enabled
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, mockS3Store, true)

	// Test data
	collectionID := types.MustParse("00000000-0000-0000-0000-000000000001")
	tenantID := "test_tenant"
	currentVersion := int32(1)
	logPosition := int64(100)

	// Set up initial version file that would have been created by CreateCollection
	initialVersionFile := &coordinatorpb.CollectionVersionFile{
		CollectionInfoImmutable: &coordinatorpb.CollectionInfoImmutable{
			CollectionId: collectionID.String(),
		},
		VersionHistory: &coordinatorpb.CollectionVersionHistory{
			Versions: []*coordinatorpb.CollectionVersionInfo{
				{
					Version: 1,
				},
			},
		},
	}
	fileName, err := mockS3Store.PutVersionFile(tenantID, "test_database", collectionID.String(), "version_1.pb", initialVersionFile)
	assert.NoError(t, err)
	assert.Equal(t, "version_1.pb", fileName)

	// Setup mock collection entry
	mockCollectionEntry := &dbmodel.Collection{
		ID:              collectionID.String(),
		Version:         int32(currentVersion),
		VersionFileName: "version_1.pb",
		LogPosition:     logPosition,
	}

	// Setup mock behaviors
	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockMetaDomain.On("TenantDb", mock.Anything).Return(mockTenantDb)
	mockMetaDomain.On("SegmentDb", mock.Anything).Return(mockSegmentDb)

	mockCollectionDb.On("GetCollectionEntry", types.FromUniqueID(collectionID), mock.Anything).Return(mockCollectionEntry, nil)
	mockCollectionDb.On("UpdateLogPositionAndVersionInfo",
		collectionID.String(),
		logPosition,
		currentVersion,
		"version_1.pb",
		currentVersion+1,
		mock.Anything,
		uint64(1),
		uint64(1),
		mock.Anything,
	).Return(int64(1), nil)

	mockTenantDb.On("UpdateTenantLastCompactionTime", tenantID, mock.Anything).Return(nil)
	mockSegmentDb.On("RegisterFilePaths", mock.Anything).Return(nil)

	mockTxImpl.On("Transaction", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		fn := args.Get(1).(func(context.Context) error)
		fn(context.Background())
	}).Return(nil)

	// Create test input
	flushRequest := &model.FlushCollectionCompaction{
		ID:                         collectionID,
		TenantID:                   tenantID,
		CurrentCollectionVersion:   currentVersion,
		LogPosition:                logPosition,
		FlushSegmentCompactions:    []*model.FlushSegmentCompaction{},
		TotalRecordsPostCompaction: 1,
		SizeBytesPostCompaction:    1,
	}

	// Execute test
	result, err := catalog.FlushCollectionCompaction(context.Background(), flushRequest)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, collectionID.String(), result.ID)
	assert.Equal(t, currentVersion+1, result.CollectionVersion)
	assert.Greater(t, result.TenantLastCompactionTime, int64(0))

	// Verify mock expectations
	mockMetaDomain.AssertExpectations(t)
	mockCollectionDb.AssertExpectations(t)
	mockTenantDb.AssertExpectations(t)
	mockSegmentDb.AssertExpectations(t)

	// Verify S3 store has the new version file
	assert.Greater(t, len(mockS3Store.files), 0)
}

func TestCatalog_DeleteCollectionVersion(t *testing.T) {
	// Create mocks
	mockTxImpl := &mocks.ITransaction{}
	mockMetaDomain := &mocks.IMetaDomain{}
	mockCollectionDb := &mocks.ICollectionDb{}
	mockS3Store := newMockS3MetaStore()

	// Create catalog with version file enabled
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, mockS3Store, true)

	// Test data
	tenantID := "test_tenant"
	databaseID := "test_database"
	collectionID := "00000000-0000-0000-0000-000000000001"
	versions_to_delete := []int64{3}
	currentVersion := int32(3)
	existingVersionFileName := "3_existing_version"

	// Setup initial version file in S3
	initialVersionFile := &coordinatorpb.CollectionVersionFile{
		CollectionInfoImmutable: &coordinatorpb.CollectionInfoImmutable{
			TenantId:     tenantID,
			CollectionId: collectionID,
		},
		VersionHistory: &coordinatorpb.CollectionVersionHistory{
			Versions: []*coordinatorpb.CollectionVersionInfo{
				{Version: 0, CreatedAtSecs: 0},
				{Version: 1, CreatedAtSecs: 1000},
				{Version: 2, CreatedAtSecs: 2000},
				{Version: 3, CreatedAtSecs: 3000},
			},
		},
	}
	mockS3Store.PutVersionFile(tenantID, databaseID, collectionID, existingVersionFileName, initialVersionFile)

	// Setup mock collection entry
	mockCollectionEntry := &dbmodel.Collection{
		ID:              collectionID,
		Version:         currentVersion,
		VersionFileName: existingVersionFileName,
		OldestVersionTs: time.Unix(0, 0),
		NumVersions:     4,
	}

	// Setup mock behaviors
	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollectionEntry", &collectionID, mock.Anything).Return(mockCollectionEntry, nil)
	mockCollectionDb.On("UpdateVersionRelatedFields",
		collectionID,
		existingVersionFileName,
		mock.AnythingOfType("string"),
		mock.AnythingOfType("*time.Time"), // expect any time value
		mock.AnythingOfType("*int"),       // numActiveVersions
	).Return(int64(1), nil)

	// Create test request
	req := &coordinatorpb.DeleteCollectionVersionRequest{
		Versions: []*coordinatorpb.VersionListForCollection{
			{
				TenantId:     tenantID,
				CollectionId: collectionID,
				Versions:     versions_to_delete,
			},
		},
	}

	// Execute test
	resp, err := catalog.DeleteCollectionVersion(context.Background(), req)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.CollectionIdToSuccess[collectionID])

	existingVersionFileName, err = catalog.GetVersionFileNamesForCollection(context.Background(), tenantID, collectionID)
	assert.NoError(t, err)
	// Verify the version file was updated correctly
	updatedFile, err := mockS3Store.GetVersionFile(
		tenantID,
		collectionID,
		int64(currentVersion),
		existingVersionFileName,
	)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(updatedFile.VersionHistory.Versions))
	assert.Equal(t, int64(0), updatedFile.VersionHistory.Versions[0].Version)
	assert.Equal(t, int64(1), updatedFile.VersionHistory.Versions[1].Version)
	assert.Equal(t, int64(2), updatedFile.VersionHistory.Versions[2].Version)

	// Verify mock expectations
	mockMetaDomain.AssertExpectations(t)
	mockCollectionDb.AssertExpectations(t)
}

func TestCatalog_DeleteCollectionVersion_CollectionNotFound(t *testing.T) {
	// Create mocks
	mockTxImpl := &mocks.ITransaction{}
	mockMetaDomain := &mocks.IMetaDomain{}
	mockCollectionDb := &mocks.ICollectionDb{}
	mockS3Store := newMockS3MetaStore()

	// Create catalog with version file enabled
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, mockS3Store, true)

	// Test data
	tenantID := "test_tenant"
	collectionID := "00000000-0000-0000-0000-000000000001"
	versions := []int64{1, 2}

	// Setup mock behaviors
	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollectionEntry", &collectionID, mock.Anything).Return(nil, nil)

	// Create test request
	req := &coordinatorpb.DeleteCollectionVersionRequest{
		Versions: []*coordinatorpb.VersionListForCollection{
			{
				TenantId:     tenantID,
				CollectionId: collectionID,
				Versions:     versions,
			},
		},
	}

	// Execute test
	resp, err := catalog.DeleteCollectionVersion(context.Background(), req)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.CollectionIdToSuccess[collectionID])

	// Verify mock expectations
	mockMetaDomain.AssertExpectations(t)
	mockCollectionDb.AssertExpectations(t)
}

func TestCatalog_MarkVersionForDeletion(t *testing.T) {
	// Create mocks
	mockTxImpl := &mocks.ITransaction{}
	mockMetaDomain := &mocks.IMetaDomain{}
	mockCollectionDb := &mocks.ICollectionDb{}
	mockS3Store := newMockS3MetaStore()

	// Create catalog with version file enabled
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, mockS3Store, true)

	// Test data
	tenantID := "test_tenant"
	databaseID := "test_database"
	collectionID := "00000000-0000-0000-0000-000000000001"
	versions := []int64{1, 2}
	currentVersion := int32(3)
	existingVersionFileName := "3_existing_version"

	// Setup initial version file in S3
	initialVersionFile := &coordinatorpb.CollectionVersionFile{
		CollectionInfoImmutable: &coordinatorpb.CollectionInfoImmutable{
			TenantId:     tenantID,
			CollectionId: collectionID,
		},
		VersionHistory: &coordinatorpb.CollectionVersionHistory{
			Versions: []*coordinatorpb.CollectionVersionInfo{
				{Version: 1, CreatedAtSecs: 1000},
				{Version: 2, CreatedAtSecs: 2000},
				{Version: 3, CreatedAtSecs: 3000},
			},
		},
	}
	mockS3Store.PutVersionFile(tenantID, databaseID, collectionID, existingVersionFileName, initialVersionFile)

	// Setup mock collection entry
	mockCollectionEntry := &dbmodel.Collection{
		ID:              collectionID,
		Version:         currentVersion,
		VersionFileName: existingVersionFileName,
	}

	// Setup mock behaviors
	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollectionEntry", &collectionID, mock.Anything).Return(mockCollectionEntry, nil)
	mockCollectionDb.On("UpdateVersionRelatedFields",
		collectionID,
		existingVersionFileName,
		mock.AnythingOfType("string"),
		(*time.Time)(nil),           // oldestVersionTs
		mock.AnythingOfType("*int"), // numActiveVersions
	).Return(int64(1), nil)

	// Create test request
	req := &coordinatorpb.MarkVersionForDeletionRequest{
		Versions: []*coordinatorpb.VersionListForCollection{
			{
				TenantId:     tenantID,
				CollectionId: collectionID,
				Versions:     versions,
			},
		},
	}

	// Execute test
	resp, err := catalog.MarkVersionForDeletion(context.Background(), req)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.True(t, resp.CollectionIdToSuccess[collectionID])

	// Verify the version file was updated correctly
	existingVersionFileName, err = catalog.GetVersionFileNamesForCollection(context.Background(), tenantID, collectionID)
	assert.NoError(t, err)
	updatedFile, err := mockS3Store.GetVersionFile(
		tenantID,
		collectionID,
		int64(currentVersion),
		existingVersionFileName,
	)
	assert.NoError(t, err)

	// Verify versions are marked for deletion
	markedVersions := 0
	for _, version := range updatedFile.VersionHistory.Versions {
		if version.MarkedForDeletion {
			markedVersions++
		}
	}
	assert.Equal(t, 2, markedVersions)

	// Verify mock expectations
	mockMetaDomain.AssertExpectations(t)
	mockCollectionDb.AssertExpectations(t)
}

func TestCatalog_MarkVersionForDeletion_CollectionNotFound(t *testing.T) {
	// Create mocks
	mockTxImpl := &mocks.ITransaction{}
	mockMetaDomain := &mocks.IMetaDomain{}
	mockCollectionDb := &mocks.ICollectionDb{}
	mockS3Store := newMockS3MetaStore()

	// Create catalog with version file enabled
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, mockS3Store, true)

	// Test data
	tenantID := "test_tenant"
	collectionID := "00000000-0000-0000-0000-000000000001"
	versions := []int64{1, 2}

	// Setup mock behaviors
	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollectionEntry", &collectionID, mock.Anything).Return(nil, nil)

	// Create test request
	req := &coordinatorpb.MarkVersionForDeletionRequest{
		Versions: []*coordinatorpb.VersionListForCollection{
			{
				TenantId:     tenantID,
				CollectionId: collectionID,
				Versions:     versions,
			},
		},
	}

	// Execute test
	resp, err := catalog.MarkVersionForDeletion(context.Background(), req)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.CollectionIdToSuccess[collectionID])

	// Verify mock expectations
	mockMetaDomain.AssertExpectations(t)
	mockCollectionDb.AssertExpectations(t)
}

func TestCatalog_MarkVersionForDeletion_VersionNotFound(t *testing.T) {
	// Create mocks
	mockTxImpl := &mocks.ITransaction{}
	mockMetaDomain := &mocks.IMetaDomain{}
	mockCollectionDb := &mocks.ICollectionDb{}
	mockS3Store := newMockS3MetaStore()

	// Create catalog with version file enabled
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, mockS3Store, true)

	// Test data
	tenantID := "test_tenant"
	collectionID := "00000000-0000-0000-0000-000000000001"
	versions := []int64{4, 5} // Versions that don't exist
	currentVersion := int32(3)
	existingVersionFileName := "3_existing_version"

	// Setup initial version file in S3
	initialVersionFile := &coordinatorpb.CollectionVersionFile{
		CollectionInfoImmutable: &coordinatorpb.CollectionInfoImmutable{
			TenantId:     tenantID,
			CollectionId: collectionID,
		},
		VersionHistory: &coordinatorpb.CollectionVersionHistory{
			Versions: []*coordinatorpb.CollectionVersionInfo{
				{Version: 1, CreatedAtSecs: 1000},
				{Version: 2, CreatedAtSecs: 2000},
				{Version: 3, CreatedAtSecs: 3000},
			},
		},
	}
	mockS3Store.PutVersionFile(tenantID, "test_database", collectionID, existingVersionFileName, initialVersionFile)

	// Setup mock collection entry
	mockCollectionEntry := &dbmodel.Collection{
		ID:              collectionID,
		Version:         currentVersion,
		VersionFileName: existingVersionFileName,
	}

	// Setup mock behaviors
	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("GetCollectionEntry", &collectionID, mock.Anything).Return(mockCollectionEntry, nil)

	// Create test request
	req := &coordinatorpb.MarkVersionForDeletionRequest{
		Versions: []*coordinatorpb.VersionListForCollection{
			{
				TenantId:     tenantID,
				CollectionId: collectionID,
				Versions:     versions,
			},
		},
	}

	// Execute test
	resp, err := catalog.MarkVersionForDeletion(context.Background(), req)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.CollectionIdToSuccess[collectionID])

	// Verify mock expectations
	mockMetaDomain.AssertExpectations(t)
	mockCollectionDb.AssertExpectations(t)
}

func TestCatalog_ListCollectionsToGc(t *testing.T) {
	// Create mocks
	mockTxImpl := &mocks.ITransaction{}
	mockMetaDomain := &mocks.IMetaDomain{}
	mockCollectionDb := &mocks.ICollectionDb{}
	mockS3Store := newMockS3MetaStore()

	// Create catalog
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, mockS3Store, true)

	// Test data
	cutoffTimeSecs := uint64(time.Now().Add(-24 * time.Hour).Unix()) // 24 hours ago
	limit := uint64(10)

	// Mock collections to return
	collectionsToGc := []*dbmodel.CollectionToGc{
		{
			ID:              "00000000-0000-0000-0000-000000000001",
			Name:            "collection1",
			Version:         3,
			VersionFileName: "3_existing_version",
			OldestVersionTs: time.Now().Add(-48 * time.Hour), // 48 hours ago
			NumVersions:     3,
		},
		{
			ID:              "00000000-0000-0000-0000-000000000002",
			Name:            "collection2",
			Version:         2,
			VersionFileName: "2_existing_version",
			OldestVersionTs: time.Now().Add(-36 * time.Hour), // 36 hours ago
			NumVersions:     2,
		},
	}

	// Setup mock behaviors
	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("ListCollectionsToGc", &cutoffTimeSecs, &limit).Return(collectionsToGc, nil)

	// Execute test
	result, err := catalog.ListCollectionsToGc(context.Background(), &cutoffTimeSecs, &limit)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 2, len(result))

	// Verify first collection
	assert.Equal(t, "00000000-0000-0000-0000-000000000001", result[0].ID.String())
	assert.Equal(t, "collection1", result[0].Name)
	assert.Equal(t, int64(3), result[0].LatestVersion)
	assert.Equal(t, "3_existing_version", result[0].VersionFilePath)

	// Verify second collection
	assert.Equal(t, "00000000-0000-0000-0000-000000000002", result[1].ID.String())
	assert.Equal(t, "collection2", result[1].Name)
	assert.Equal(t, int64(2), result[1].LatestVersion)
	assert.Equal(t, "2_existing_version", result[1].VersionFilePath)

	// Verify mock expectations
	mockMetaDomain.AssertExpectations(t)
	mockCollectionDb.AssertExpectations(t)
}

func TestCatalog_ListCollectionsToGc_NilParameters(t *testing.T) {
	// Create mocks
	mockTxImpl := &mocks.ITransaction{}
	mockMetaDomain := &mocks.IMetaDomain{}
	mockCollectionDb := &mocks.ICollectionDb{}
	mockS3Store := newMockS3MetaStore()

	// Create catalog
	catalog := NewTableCatalog(mockTxImpl, mockMetaDomain, mockS3Store, true)

	// Mock collections to return
	collectionsToGc := []*dbmodel.CollectionToGc{
		{
			ID:              "00000000-0000-0000-0000-000000000001",
			Name:            "collection1",
			Version:         3,
			VersionFileName: "3_existing_version",
			OldestVersionTs: time.Now().Add(-48 * time.Hour),
			NumVersions:     3,
		},
	}

	// Setup mock behaviors
	mockMetaDomain.On("CollectionDb", mock.Anything).Return(mockCollectionDb)
	mockCollectionDb.On("ListCollectionsToGc", (*uint64)(nil), (*uint64)(nil)).Return(collectionsToGc, nil)

	// Execute test with nil parameters
	result, err := catalog.ListCollectionsToGc(context.Background(), nil, nil)

	// Verify results
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, len(result))

	// Verify collection details
	assert.Equal(t, "00000000-0000-0000-0000-000000000001", result[0].ID.String())
	assert.Equal(t, "collection1", result[0].Name)
	assert.Equal(t, int64(3), result[0].LatestVersion)
	assert.Equal(t, "3_existing_version", result[0].VersionFilePath)

	// Verify mock expectations
	mockMetaDomain.AssertExpectations(t)
	mockCollectionDb.AssertExpectations(t)
}
