package coordinator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	s3metastore "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/s3"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
	"pgregory.net/rapid"
)

type APIsTestSuite struct {
	suite.Suite
	db                *gorm.DB
	read_db           *gorm.DB
	collectionId1     types.UniqueID
	collectionId2     types.UniqueID
	records           [][]byte
	tenantName        string
	databaseName      string
	databaseId        string
	sampleCollections []*model.Collection
	coordinator       *Coordinator
	s3MetaStore       *s3metastore.S3MetaStore
	minioContainer    *s3metastore.MinioContainer
}

func (suite *APIsTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db, suite.read_db = dbcore.ConfigDatabaseForTesting()

	ctx := context.Background()
	// Add timeout context
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	s3MetaStore, minioContainer, err := s3metastore.NewS3MetaStoreWithContainer(
		ctx,
		"chroma-storage",
		"sysdb",
	)
	suite.NoError(err)
	suite.s3MetaStore = s3MetaStore
	suite.minioContainer = minioContainer
}

func (suite *APIsTestSuite) TearDownSuite() {
	if suite.minioContainer != nil {
		ctx := context.Background()
		err := suite.minioContainer.Terminate(ctx)
		suite.NoError(err)
	}
}

func (suite *APIsTestSuite) SetupTest() {
	log.Info("setup test")
	suite.tenantName = "tenant_" + suite.T().Name()
	suite.databaseName = "database_" + suite.T().Name()
	DbId, err := dao.CreateTestTenantAndDatabase(suite.db, suite.tenantName, suite.databaseName)
	suite.NoError(err)
	suite.databaseId = DbId
	suite.sampleCollections = SampleCollections(suite.tenantName, suite.databaseName)
	for index, collection := range suite.sampleCollections {
		collection.ID = types.NewUniqueID()
		collection.Name = "collection_" + suite.T().Name() + strconv.Itoa(index)
	}
	ctx := context.Background()
	c, err := NewCoordinator(ctx, CoordinatorConfig{
		ObjectStore:        suite.s3MetaStore,
		VersionFileEnabled: true,
	})
	if err != nil {
		suite.T().Fatalf("error creating coordinator: %v", err)
	}
	suite.coordinator = c
	for _, collection := range suite.sampleCollections {
		_, _, errCollectionCreation := c.CreateCollection(ctx, &model.CreateCollection{
			ID:           collection.ID,
			Name:         collection.Name,
			Metadata:     collection.Metadata,
			Dimension:    collection.Dimension,
			TenantID:     collection.TenantID,
			DatabaseName: collection.DatabaseName,
		})
		suite.NoError(errCollectionCreation)
	}
}

func (suite *APIsTestSuite) TearDownTest() {
	log.Info("teardown test")
	err := dao.CleanUpTestDatabase(suite.db, suite.tenantName, suite.databaseName)
	suite.NoError(err)
	err = dao.CleanUpTestTenant(suite.db, suite.tenantName)
	suite.NoError(err)
}

// TODO: This is not complete yet. We need to add more tests for the other APIs.
// We will deprecate the example based tests once we have enough tests here.
func testCollection(t *rapid.T) {
	dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	c, err := NewCoordinator(ctx, CoordinatorConfig{
		ObjectStore:        nil,
		VersionFileEnabled: false,
	})
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	err = c.ResetState(ctx)
	if err != nil {
		t.Fatalf("error resetting coordinator state: %v", err)
	}
	t.Repeat(map[string]func(*rapid.T){
		"create_collection": func(t *rapid.T) {
			stringValue := generateCollectionStringMetadataValue(t)
			intValue := generateCollectionInt64MetadataValue(t)
			floatValue := generateCollectionFloat64MetadataValue(t)

			metadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
			metadata.Add("string_value", stringValue)
			metadata.Add("int_value", intValue)
			metadata.Add("float_value", floatValue)

			collection := rapid.Custom[*model.CreateCollection](func(t *rapid.T) *model.CreateCollection {
				return &model.CreateCollection{
					ID:       types.MustParse(rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "collection_id")),
					Name:     rapid.String().Draw(t, "collection_name"),
					Metadata: nil,
				}
			}).Draw(t, "collection")

			_, _, err := c.CreateCollection(ctx, collection)
			if err != nil {
				if err == common.ErrCollectionNameEmpty && collection.Name == "" {
					t.Logf("expected error for empty collection name")
				} else {
					t.Fatalf("error creating collection: %v", err)
				}
			}
			if err == nil {
				// verify the correctness
				collectionList, err := c.GetCollections(ctx, []types.UniqueID{collection.ID}, nil, common.DefaultTenant, common.DefaultDatabase, nil, nil, false)
				if err != nil {
					t.Fatalf("error getting collections: %v", err)
				}
				if len(collectionList) != 1 {
					t.Fatalf("More than 1 collection with the same collection id")
				}
				for _, collectionReturned := range collectionList {
					if collection.ID != collectionReturned.ID {
						t.Fatalf("collection id is the right value")
					}
				}
			}
		},
	})
}

func testSegment(t *rapid.T) {
	dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	c, err := NewCoordinator(ctx, CoordinatorConfig{
		ObjectStore:        nil,
		VersionFileEnabled: false,
	})
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}

	stringValue := generateSegmentStringMetadataValue(t)
	intValue := generateSegmentInt64MetadataValue(t)
	floatValue := generateSegmentFloat64MetadataValue(t)

	metadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	metadata.Set("string_value", stringValue)
	metadata.Set("int_value", intValue)
	metadata.Set("float_value", floatValue)

	t.Repeat(map[string]func(*rapid.T){
		"create_segment": func(t *rapid.T) {
			segment := rapid.Custom[*model.Segment](func(t *rapid.T) *model.Segment {
				return &model.Segment{
					ID:           types.MustParse(rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "segment_id")),
					Type:         "test-segment-type",
					Scope:        "test-segment-scope",
					Metadata:     nil,
					CollectionID: types.MustParse(rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "collection_id")),
				}
			}).Draw(t, "segment")

			err := c.CreateSegment(ctx, segment)
			if err != nil {
				t.Fatalf("error creating segment: %v", err)
			}
		},
	})
}

func generateCollectionStringMetadataValue(t *rapid.T) model.CollectionMetadataValueType {
	return &model.CollectionMetadataValueStringType{
		Value: rapid.String().Draw(t, "string_value"),
	}
}

func generateCollectionInt64MetadataValue(t *rapid.T) model.CollectionMetadataValueType {
	return &model.CollectionMetadataValueInt64Type{
		Value: rapid.Int64().Draw(t, "int_value"),
	}
}

func generateCollectionFloat64MetadataValue(t *rapid.T) model.CollectionMetadataValueType {
	return &model.CollectionMetadataValueFloat64Type{
		Value: rapid.Float64().Draw(t, "float_value"),
	}
}

func generateSegmentStringMetadataValue(t *rapid.T) model.SegmentMetadataValueType {
	return &model.SegmentMetadataValueStringType{
		Value: rapid.String().Draw(t, "string_value"),
	}
}

func generateSegmentInt64MetadataValue(t *rapid.T) model.SegmentMetadataValueType {
	return &model.SegmentMetadataValueInt64Type{
		Value: rapid.Int64().Draw(t, "int_value"),
	}
}

func generateSegmentFloat64MetadataValue(t *rapid.T) model.SegmentMetadataValueType {
	return &model.SegmentMetadataValueFloat64Type{
		Value: rapid.Float64().Draw(t, "float_value"),
	}
}

func TestAPIs(t *testing.T) {
	// rapid.Check(t, testCollection)
	// rapid.Check(t, testSegment)
}

func SampleCollections(tenantID string, databaseName string) []*model.Collection {
	dimension := int32(128)
	metadata1 := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	metadata1.Add("test_str", &model.CollectionMetadataValueStringType{Value: "str1"})
	metadata1.Add("test_int", &model.CollectionMetadataValueInt64Type{Value: 1})
	metadata1.Add("test_float", &model.CollectionMetadataValueFloat64Type{Value: 1.3})

	metadata2 := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	metadata2.Add("test_str", &model.CollectionMetadataValueStringType{Value: "str2"})
	metadata2.Add("test_int", &model.CollectionMetadataValueInt64Type{Value: 2})
	metadata2.Add("test_float", &model.CollectionMetadataValueFloat64Type{Value: 2.3})

	metadata3 := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	metadata3.Add("test_str", &model.CollectionMetadataValueStringType{Value: "str3"})
	metadata3.Add("test_int", &model.CollectionMetadataValueInt64Type{Value: 3})
	metadata3.Add("test_float", &model.CollectionMetadataValueFloat64Type{Value: 3.3})
	sampleCollections := []*model.Collection{
		{
			ID:           types.MustParse("93ffe3ec-0107-48d4-8695-51f978c509dc"),
			Name:         "test_collection_1",
			Metadata:     metadata1,
			Dimension:    &dimension,
			TenantID:     tenantID,
			DatabaseName: databaseName,
		},
		{
			ID:           types.MustParse("f444f1d7-d06c-4357-ac22-5a4a1f92d761"),
			Name:         "test_collection_2",
			Metadata:     metadata2,
			Dimension:    nil,
			TenantID:     tenantID,
			DatabaseName: databaseName,
		},
		{
			ID:           types.MustParse("43babc1a-e403-4a50-91a9-16621ba29ab0"),
			Name:         "test_collection_3",
			Metadata:     metadata3,
			Dimension:    nil,
			TenantID:     tenantID,
			DatabaseName: databaseName,
		},
	}
	return sampleCollections
}

func (suite *APIsTestSuite) TestCreateCollectionAndSegments() {
	ctx := context.Background()

	// Create a new collection with segments
	newCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "test_collection_and_segments",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	segments := []*model.Segment{
		{
			ID:           types.NewUniqueID(),
			Type:         "test_type",
			Scope:        "VECTOR",
			CollectionID: newCollection.ID,
		},
		{
			ID:           types.NewUniqueID(),
			Type:         "test_type",
			Scope:        "VECTOR",
			CollectionID: newCollection.ID,
			FilePaths: map[string][]string{
				"test_path": {"test_file1"},
			},
		},
	}

	// Create collection and segments
	createdCollection, created, err := suite.coordinator.CreateCollectionAndSegments(ctx, newCollection, segments)
	suite.NoError(err)
	suite.True(created)
	suite.Equal(newCollection.ID, createdCollection.ID)
	suite.Equal(newCollection.Name, createdCollection.Name)
	suite.NotNil(createdCollection.VersionFileName)
	// suite.Equal(len(segments), len(createdSegments))

	// Verify the collection was created
	result, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{newCollection.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(newCollection.ID, result[0].ID)
	suite.Equal(newCollection.Name, result[0].Name)

	// Verify the segments were created
	for _, segment := range segments {
		segmentResult, err := suite.coordinator.GetSegments(ctx, segment.ID, &segment.Type, &segment.Scope, newCollection.ID)
		suite.NoError(err)
		suite.Len(segmentResult, 1)
		suite.Equal(segment.ID, segmentResult[0].ID)
	}

	// The same information should be returned by the GetCollectionWithSegments endpoint
	collection, collection_segments, error := suite.coordinator.GetCollectionWithSegments(ctx, newCollection.ID)
	suite.NoError(error)
	suite.Equal(newCollection.ID, collection.ID)
	suite.Equal(newCollection.Name, collection.Name)
	expected_ids, actual_ids := []types.UniqueID{}, []types.UniqueID{}
	for _, segment := range segments {
		expected_ids = append(expected_ids, segment.ID)
	}
	for _, segment := range collection_segments {
		suite.Equal(collection.ID, segment.CollectionID)
		actual_ids = append(actual_ids, segment.ID)
	}
	suite.ElementsMatch(expected_ids, actual_ids)

	// Validate version file
	suite.NotNil(collection.VersionFileName)
	versionFile, err := suite.s3MetaStore.GetVersionFile(context.Background(), collection.VersionFileName)
	suite.NoError(err)
	suite.NotNil(versionFile)
	v0 := versionFile.VersionHistory.Versions[0]
	suite.NotNil(v0)

	// Validate file paths of segments
	suite.NotNil(v0.SegmentInfo)
	suite.NotNil(v0.SegmentInfo.SegmentCompactionInfo)
	suite.Equal(len(v0.SegmentInfo.SegmentCompactionInfo), 2)
	for _, segment := range segments {
		assertExpectedSegmentInfoExist(suite, segment, v0.SegmentInfo.SegmentCompactionInfo)
	}

	// Attempt to create a duplicate collection (should fail)
	_, _, err = suite.coordinator.CreateCollectionAndSegments(ctx, newCollection, segments)
	suite.Error(err)

	// Create a new collection with new name and ID, so that the segment creation will fail
	newCollection.ID = types.NewUniqueID()
	newCollection.Name = "test_collection_and_segments_2"
	segments[0].ID = segments[1].ID // Ensure the segment ID is the same as the existing one, so that the segment creation will fail
	_, _, err = suite.coordinator.CreateCollectionAndSegments(ctx, newCollection, segments)
	suite.Error(err)

	// Check that the collection was not created
	collections, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{newCollection.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Empty(collections)

	// Create a collection on a database that does not exist.
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "test_collection_and_segments",
		TenantID:     suite.tenantName,
		DatabaseName: "non_existent_database",
	})
	suite.Error(err)
	// Check the error code is ErrDatabaseNotFound
	suite.Equal(common.ErrDatabaseNotFound, err)
	suite.Assertions.Contains(err.Error(), "database not found")
}

// TestCreateGetDeleteCollections tests the create, get and delete collection APIs.
// Test does not check for soft delete and hard delete scenarios, but checks that
// the APIs are working as expected. i.e. Get does not return deleted collections.
func (suite *APIsTestSuite) TestCreateGetDeleteCollections() {
	ctx := context.Background()
	results, err := suite.coordinator.GetCollections(ctx, nil, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})
	suite.Len(results, len(suite.sampleCollections))
	for i, collection := range results {
		suite.Equal(suite.sampleCollections[i].ID, collection.ID)
		suite.Equal(suite.sampleCollections[i].Name, collection.Name)
		suite.Equal(suite.sampleCollections[i].TenantID, collection.TenantID)
		suite.Equal(suite.sampleCollections[i].DatabaseName, collection.DatabaseName)
		suite.Equal(suite.sampleCollections[i].Dimension, collection.Dimension)
		suite.Equal(suite.sampleCollections[i].Metadata, collection.Metadata)
	}

	// Duplicate create fails
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           suite.sampleCollections[0].ID,
		Name:         suite.sampleCollections[0].Name,
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	})
	suite.Error(err)

	// Create collection with empty name fails
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           suite.sampleCollections[0].ID,
		Name:         "",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	})
	suite.Error(err)

	// Create collection with empty tenant id fails
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           suite.sampleCollections[0].ID,
		Name:         suite.sampleCollections[0].Name,
		DatabaseName: suite.databaseName,
	})
	suite.Error(err)

	// Create collection with random tenant id fails
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           suite.sampleCollections[0].ID,
		Name:         suite.sampleCollections[0].Name,
		TenantID:     "random_tenant_id",
		DatabaseName: suite.databaseName,
	})
	suite.Error(err)

	// Find by name
	for _, collection := range suite.sampleCollections {
		result, err := suite.coordinator.GetCollections(ctx, nil, &collection.Name, suite.tenantName, suite.databaseName, nil, nil, false)
		suite.NoError(err)
		suite.Equal(len(result), 1)
		suite.Equal(collection.ID, result[0].ID)
		suite.Equal(collection.Name, result[0].Name)
		suite.Equal(collection.TenantID, result[0].TenantID)
		suite.Equal(collection.DatabaseName, result[0].DatabaseName)
		suite.Equal(collection.Dimension, result[0].Dimension)
		suite.Equal(collection.Metadata, result[0].Metadata)
	}

	// Find by id
	for _, collection := range suite.sampleCollections {
		result, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{collection.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
		suite.NoError(err)
		suite.Equal(len(result), 1)
		suite.Equal(collection.ID, result[0].ID)
		suite.Equal(collection.Name, result[0].Name)
		suite.Equal(collection.TenantID, result[0].TenantID)
		suite.Equal(collection.DatabaseName, result[0].DatabaseName)
		suite.Equal(collection.Dimension, result[0].Dimension)
		suite.Equal(collection.Metadata, result[0].Metadata)
	}

	// Delete
	c1 := suite.sampleCollections[0]
	deleteCollection := &model.DeleteCollection{
		ID:           c1.ID,
		DatabaseName: suite.databaseName,
		TenantID:     suite.tenantName,
	}
	err = suite.coordinator.SoftDeleteCollection(ctx, deleteCollection)
	suite.NoError(err)

	results, err = suite.coordinator.GetCollections(ctx, nil, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	result_ids := make([]types.UniqueID, len(results))
	for i, result := range results {
		result_ids[i] = result.ID
	}

	sample_ids := make([]types.UniqueID, len(suite.sampleCollections))
	for i, collection := range suite.sampleCollections {
		sample_ids[i] = collection.ID
	}
	suite.NotContains(result_ids, c1.ID)
	suite.Len(results, len(suite.sampleCollections)-1)
	suite.ElementsMatch(result_ids, sample_ids[1:])
	byIDResult, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{c1.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Empty(byIDResult)

	// Duplicate delete throws an exception
	err = suite.coordinator.SoftDeleteCollection(ctx, deleteCollection)
	suite.Error(err)

	// Re-create the deleted collection
	// Recreating the deleted collection with new ID since the old ID is already in use by the soft deleted collection.
	createCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         suite.sampleCollections[0].Name,
		Dimension:    suite.sampleCollections[0].Dimension,
		Metadata:     suite.sampleCollections[0].Metadata,
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
		Ts:           types.Timestamp(time.Now().Unix()),
	}
	_, _, err = suite.coordinator.CreateCollection(ctx, createCollection)
	suite.NoError(err)

	// Verify collection was re-created
	results, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{createCollection.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(results, 1)
	suite.Equal(createCollection.ID, results[0].ID)
	suite.Equal(createCollection.Name, results[0].Name)
	suite.Equal(createCollection.Dimension, results[0].Dimension)
	suite.Equal(createCollection.Metadata, results[0].Metadata)

	// Create segments associated with collection
	segment := &model.Segment{
		ID:           types.MustParse("00000000-0000-0000-0000-000000000001"),
		CollectionID: createCollection.ID,
		Type:         "test_segment",
		Scope:        "test_scope",
		Ts:           types.Timestamp(time.Now().Unix()),
	}
	err = suite.coordinator.CreateSegment(ctx, segment)
	suite.NoError(err)

	// Verify segment was created
	segments, err := suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, createCollection.ID)
	suite.NoError(err)
	suite.Len(segments, 1)
	suite.Equal(segment.ID, segments[0].ID)
	suite.Equal(segment.CollectionID, segments[0].CollectionID)
	suite.Equal(segment.Type, segments[0].Type)
	suite.Equal(segment.Scope, segments[0].Scope)

	// Delete the re-created collection with segments
	deleteCollection = &model.DeleteCollection{
		ID:           createCollection.ID,
		DatabaseName: suite.databaseName,
		TenantID:     suite.tenantName,
	}
	err = suite.coordinator.SoftDeleteCollection(ctx, deleteCollection)
	suite.NoError(err)

	// Verify collection and segment were deleted
	results, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{createCollection.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Empty(results)
	// Segments will not be deleted since the collection is only soft deleted.
	// Hard deleting the collection will also delete the segments.
	segments, err = suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, createCollection.ID)
	suite.NoError(err)
	suite.NotEmpty(segments)
	err = suite.coordinator.FinishCollectionDeletion(ctx, deleteCollection)
	suite.NoError(err)
	segments, err = suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, createCollection.ID)
	suite.NoError(err)
	suite.Empty(segments)
}

func (suite *APIsTestSuite) TestCollectionSize() {
	ctx := context.Background()

	for _, collection := range suite.sampleCollections {
		result, err := suite.coordinator.GetCollectionSize(ctx, collection.ID)
		suite.NoError(err)
		suite.Equal(uint64(0), result)
	}
}

func (suite *APIsTestSuite) TestUpdateCollections() {
	ctx := context.Background()
	coll := &model.Collection{
		Name:         suite.sampleCollections[0].Name,
		ID:           suite.sampleCollections[0].ID,
		Metadata:     suite.sampleCollections[0].Metadata,
		Dimension:    suite.sampleCollections[0].Dimension,
		TenantID:     suite.sampleCollections[0].TenantID,
		DatabaseName: suite.sampleCollections[0].DatabaseName,
	}

	// Update name
	coll.Name = "new_name"
	result, err := suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Name: &coll.Name})
	suite.NoError(err)
	suite.Equal(coll.ID, result.ID)
	suite.Equal(coll.Name, result.Name)
	suite.Equal(coll.TenantID, result.TenantID)
	suite.Equal(coll.DatabaseName, result.DatabaseName)
	suite.Equal(coll.Dimension, result.Dimension)
	suite.Equal(coll.Metadata, result.Metadata)

	resultList, err := suite.coordinator.GetCollections(ctx, nil, &coll.Name, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Equal(len(resultList), 1)
	suite.Equal(coll.ID, resultList[0].ID)
	suite.Equal(coll.Name, resultList[0].Name)
	suite.Equal(coll.TenantID, resultList[0].TenantID)
	suite.Equal(coll.DatabaseName, resultList[0].DatabaseName)
	suite.Equal(coll.Dimension, resultList[0].Dimension)
	suite.Equal(coll.Metadata, resultList[0].Metadata)

	// Update dimension
	newDimension := int32(128)
	coll.Dimension = &newDimension
	result, err = suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Dimension: coll.Dimension})
	suite.NoError(err)
	suite.Equal(coll.ID, result.ID)
	suite.Equal(coll.Name, result.Name)
	suite.Equal(coll.TenantID, result.TenantID)
	suite.Equal(coll.DatabaseName, result.DatabaseName)
	suite.Equal(coll.Dimension, result.Dimension)
	suite.Equal(coll.Metadata, result.Metadata)

	resultList, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{coll.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Equal(len(resultList), 1)
	suite.Equal(coll.ID, resultList[0].ID)
	suite.Equal(coll.Name, resultList[0].Name)
	suite.Equal(coll.TenantID, resultList[0].TenantID)
	suite.Equal(coll.DatabaseName, resultList[0].DatabaseName)
	suite.Equal(coll.Dimension, resultList[0].Dimension)
	suite.Equal(coll.Metadata, resultList[0].Metadata)

	// Reset the metadata
	newMetadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	newMetadata.Add("test_str2", &model.CollectionMetadataValueStringType{Value: "str2"})
	coll.Metadata = newMetadata
	result, err = suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Metadata: coll.Metadata})
	suite.NoError(err)
	suite.Equal(coll.ID, result.ID)
	suite.Equal(coll.Name, result.Name)
	suite.Equal(coll.TenantID, result.TenantID)
	suite.Equal(coll.DatabaseName, result.DatabaseName)
	suite.Equal(coll.Dimension, result.Dimension)
	suite.Equal(coll.Metadata, result.Metadata)

	resultList, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{coll.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Equal(len(resultList), 1)
	suite.Equal(coll.ID, resultList[0].ID)
	suite.Equal(coll.Name, resultList[0].Name)
	suite.Equal(coll.TenantID, resultList[0].TenantID)
	suite.Equal(coll.DatabaseName, resultList[0].DatabaseName)
	suite.Equal(coll.Dimension, resultList[0].Dimension)
	suite.Equal(coll.Metadata, resultList[0].Metadata)

	// Delete all metadata keys
	coll.Metadata = nil
	result, err = suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Metadata: coll.Metadata, ResetMetadata: true})
	suite.NoError(err)
	suite.Equal(coll.ID, result.ID)
	suite.Equal(coll.Name, result.Name)
	suite.Equal(coll.TenantID, result.TenantID)
	suite.Equal(coll.DatabaseName, result.DatabaseName)
	suite.Equal(coll.Dimension, result.Dimension)
	suite.Equal(coll.Metadata, result.Metadata)

	resultList, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{coll.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Equal(len(resultList), 1)
	suite.Equal(coll.ID, resultList[0].ID)
	suite.Equal(coll.Name, resultList[0].Name)
	suite.Equal(coll.TenantID, resultList[0].TenantID)
	suite.Equal(coll.DatabaseName, resultList[0].DatabaseName)
	suite.Equal(coll.Dimension, resultList[0].Dimension)
	suite.Equal(coll.Metadata, resultList[0].Metadata)
}

func (suite *APIsTestSuite) TestGetOrCreateCollectionsTwice() {
	ctx := context.Background()

	id := types.NewUniqueID()
	name := "test_get_or_create_collection_twice"

	_, created, err := suite.coordinator.CreateCollectionAndSegments(ctx, &model.CreateCollection{
		ID:           id,
		Name:         name,
		Metadata:     suite.sampleCollections[0].Metadata,
		Dimension:    suite.sampleCollections[0].Dimension,
		GetOrCreate:  true,
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}, []*model.Segment{})
	suite.NoError(err)
	suite.True(created)

	now := time.Now()

	_, created, err = suite.coordinator.CreateCollectionAndSegments(ctx, &model.CreateCollection{
		ID:           id,
		Name:         name,
		Metadata:     suite.sampleCollections[0].Metadata,
		Dimension:    suite.sampleCollections[0].Dimension,
		GetOrCreate:  true,
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}, []*model.Segment{})
	suite.NoError(err)
	suite.False(created)

	objects, err := suite.s3MetaStore.S3.ListObjects(context.Background(), &s3.ListObjectsInput{
		Bucket: aws.String(suite.s3MetaStore.BucketName),
		Prefix: aws.String(""),
	})
	suite.NoError(err)

	// There should be exactly one version file
	numVersionFiles := 0
	var versionFileLastModified *time.Time
	for _, object := range objects.Contents {
		if strings.Contains(*object.Key, "versionfile") && strings.Contains(*object.Key, id.String()) {
			numVersionFiles++
			suite.NotNil(object.LastModified, "Version file should have a LastModified timestamp")
			versionFileLastModified = object.LastModified
		}
	}
	suite.Equal(1, numVersionFiles)

	// The version file should not have been modified after the first creation
	suite.True(versionFileLastModified.Before(now), "Version file was modified after the first creation")
}

func (suite *APIsTestSuite) TestCreateUpdateWithDatabase() {
	ctx := context.Background()
	newDatabaseName := "test_apis_CreateUpdateWithDatabase"
	newDatabaseId := uuid.New().String()
	// Create database with empty string in name fails
	_, err := suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     newDatabaseId,
		Name:   "",
		Tenant: suite.tenantName,
	})
	suite.Error(err)

	// Create database with empty string in tenant fails
	_, err = suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     newDatabaseId,
		Name:   newDatabaseName,
		Tenant: "",
	})
	suite.Error(err)

	// Create database with random non-existent tenant id fails
	_, err = suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     newDatabaseId,
		Name:   newDatabaseName,
		Tenant: "random_tenant_id",
	})
	suite.Error(err)

	// Correct creation
	_, err = suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     newDatabaseId,
		Name:   newDatabaseName,
		Tenant: suite.tenantName,
	})
	suite.NoError(err)

	suite.sampleCollections[0].ID = types.NewUniqueID()
	suite.sampleCollections[0].Name = suite.sampleCollections[0].Name + "1"
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           suite.sampleCollections[0].ID,
		Name:         suite.sampleCollections[0].Name,
		Metadata:     suite.sampleCollections[0].Metadata,
		Dimension:    suite.sampleCollections[0].Dimension,
		TenantID:     suite.sampleCollections[0].TenantID,
		DatabaseName: newDatabaseName,
	})
	suite.NoError(err)
	newName1 := "new_name_1"
	_, err = suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{
		ID:   suite.sampleCollections[1].ID,
		Name: &newName1,
	})
	suite.NoError(err)
	result, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{suite.sampleCollections[1].ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(newName1, result[0].Name)

	newName0 := "new_name_0"
	_, err = suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{
		ID:   suite.sampleCollections[0].ID,
		Name: &newName0,
	})
	suite.NoError(err)
	//suite.Equal(newName0, collection.Name)
	result, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{suite.sampleCollections[0].ID}, nil, suite.tenantName, newDatabaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(newName0, result[0].Name)

	// clean up
	err = dao.CleanUpTestDatabase(suite.db, suite.tenantName, newDatabaseName)
	suite.NoError(err)
}

func (suite *APIsTestSuite) TestGetMultipleWithDatabase() {
	newDatabaseName := "test_apis_GetMultipleWithDatabase"
	ctx := context.Background()

	newDatabaseId := uuid.New().String()
	_, err := suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     newDatabaseId,
		Name:   newDatabaseName,
		Tenant: suite.tenantName,
	})
	suite.NoError(err)

	for index, collection := range suite.sampleCollections {
		collection.ID = types.NewUniqueID()
		collection.Name = collection.Name + "1"
		collection.TenantID = suite.tenantName
		collection.DatabaseName = newDatabaseName
		_, _, err := suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
			ID:           collection.ID,
			Name:         collection.Name,
			Metadata:     collection.Metadata,
			Dimension:    collection.Dimension,
			TenantID:     collection.TenantID,
			DatabaseName: collection.DatabaseName,
		})
		suite.NoError(err)
		suite.sampleCollections[index] = collection
	}
	result, err := suite.coordinator.GetCollections(ctx, nil, nil, suite.tenantName, newDatabaseName, nil, nil, false)
	suite.NoError(err)
	suite.Equal(len(suite.sampleCollections), len(result))
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	for index, collection := range result {
		suite.Equal(suite.sampleCollections[index].ID, collection.ID)
		suite.Equal(suite.sampleCollections[index].Name, collection.Name)
		suite.Equal(suite.sampleCollections[index].TenantID, collection.TenantID)
		suite.Equal(suite.sampleCollections[index].DatabaseName, collection.DatabaseName)
		suite.Equal(suite.sampleCollections[index].Dimension, collection.Dimension)
		suite.Equal(suite.sampleCollections[index].Metadata, collection.Metadata)
	}

	result, err = suite.coordinator.GetCollections(ctx, nil, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Equal(len(suite.sampleCollections), len(result))

	// clean up
	err = dao.CleanUpTestDatabase(suite.db, suite.tenantName, newDatabaseName)
	suite.NoError(err)
}

func (suite *APIsTestSuite) TestCreateDatabaseWithTenants() {
	ctx := context.Background()

	// Create a new tenant
	newTenantName := "tenant1"
	_, err := suite.coordinator.CreateTenant(ctx, &model.CreateTenant{
		Name: newTenantName,
	})
	suite.NoError(err)

	// Create tenant that already exits and expect an error
	_, err = suite.coordinator.CreateTenant(ctx, &model.CreateTenant{
		Name: newTenantName,
	})
	suite.Error(err)

	// Create tenant that already exits and expect an error
	_, err = suite.coordinator.CreateTenant(ctx, &model.CreateTenant{
		Name: suite.tenantName,
	})
	suite.Error(err)

	// Create a new database within this tenant and also in the default tenant
	newDatabaseName := "test_apis_CreateDatabaseWithTenants"
	_, err = suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     types.MustParse("33333333-d7d7-413b-92e1-731098a6e492").String(),
		Name:   newDatabaseName,
		Tenant: newTenantName,
	})
	suite.NoError(err)

	_, err = suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     types.MustParse("44444444-d7d7-413b-92e1-731098a6e492").String(),
		Name:   newDatabaseName,
		Tenant: suite.tenantName,
	})
	suite.NoError(err)

	// Create a new collection in the new tenant
	suite.sampleCollections[0].ID = types.NewUniqueID()
	suite.sampleCollections[0].Name = suite.sampleCollections[0].Name + "1"
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           suite.sampleCollections[0].ID,
		Name:         suite.sampleCollections[0].Name,
		Metadata:     suite.sampleCollections[0].Metadata,
		Dimension:    suite.sampleCollections[0].Dimension,
		TenantID:     newTenantName,
		DatabaseName: newDatabaseName,
	})
	suite.NoError(err)

	// Create a new collection in the default tenant
	suite.sampleCollections[1].ID = types.NewUniqueID()
	suite.sampleCollections[1].Name = suite.sampleCollections[1].Name + "2"
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           suite.sampleCollections[1].ID,
		Name:         suite.sampleCollections[1].Name,
		Metadata:     suite.sampleCollections[1].Metadata,
		Dimension:    suite.sampleCollections[1].Dimension,
		TenantID:     suite.tenantName,
		DatabaseName: newDatabaseName,
	})
	suite.NoError(err)

	// Check that both tenants have the correct collections
	expected := []*model.Collection{suite.sampleCollections[0]}
	expected[0].TenantID = newTenantName
	expected[0].DatabaseName = newDatabaseName
	result, err := suite.coordinator.GetCollections(ctx, nil, nil, newTenantName, newDatabaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(expected[0].ID, result[0].ID)
	suite.Equal(expected[0].Name, result[0].Name)
	suite.Equal(expected[0].TenantID, result[0].TenantID)
	suite.Equal(expected[0].DatabaseName, result[0].DatabaseName)
	suite.Equal(expected[0].Dimension, result[0].Dimension)
	suite.Equal(expected[0].Metadata, result[0].Metadata)

	expected = []*model.Collection{suite.sampleCollections[1]}
	expected[0].TenantID = suite.tenantName
	expected[0].DatabaseName = newDatabaseName
	result, err = suite.coordinator.GetCollections(ctx, nil, nil, suite.tenantName, newDatabaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(expected[0].ID, result[0].ID)
	suite.Equal(expected[0].Name, result[0].Name)
	suite.Equal(expected[0].TenantID, result[0].TenantID)
	suite.Equal(expected[0].DatabaseName, result[0].DatabaseName)
	suite.Equal(expected[0].Dimension, result[0].Dimension)
	suite.Equal(expected[0].Metadata, result[0].Metadata)

	// A new tenant DOES NOT have a default database. This does not error, instead 0
	// results are returned
	result, err = suite.coordinator.GetCollections(ctx, nil, nil, newTenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Equal(0, len(result))

	// clean up
	err = dao.CleanUpTestTenant(suite.db, newTenantName)
	suite.NoError(err)
	err = dao.CleanUpTestDatabase(suite.db, suite.tenantName, newDatabaseName)
	suite.NoError(err)
}

func (suite *APIsTestSuite) TestCreateGetDeleteTenants() {
	ctx := context.Background()

	// Create a new tenant
	newTenantName := "tenant1"
	_, err := suite.coordinator.CreateTenant(ctx, &model.CreateTenant{
		Name: newTenantName,
	})
	suite.NoError(err)

	// Create tenant that already exits and expect an error
	_, err = suite.coordinator.CreateTenant(ctx, &model.CreateTenant{
		Name: newTenantName,
	})
	suite.Error(err)

	// Create tenant that already exits and expect an error
	_, err = suite.coordinator.CreateTenant(ctx, &model.CreateTenant{
		Name: suite.tenantName,
	})
	suite.Error(err)

	// Get the tenant and check that it exists
	result, err := suite.coordinator.GetTenant(ctx, &model.GetTenant{Name: newTenantName})
	suite.NoError(err)
	suite.Equal(newTenantName, result.Name)

	// Get a tenant that does not exist and expect an error
	_, err = suite.coordinator.GetTenant(ctx, &model.GetTenant{Name: "tenant2"})
	suite.Error(err)

	// Create a new database within this tenant
	newDatabaseName := "test_apis_CreateGetDeleteTenants"
	_, err = suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     types.MustParse("33333333-d7d7-413b-92e1-731098a6e492").String(),
		Name:   newDatabaseName,
		Tenant: newTenantName,
	})
	suite.NoError(err)

	// Get the database and check that it exists
	databaseResult, err := suite.coordinator.GetDatabase(ctx, &model.GetDatabase{
		Name:   newDatabaseName,
		Tenant: newTenantName,
	})
	suite.NoError(err)
	suite.Equal(newDatabaseName, databaseResult.Name)
	suite.Equal(newTenantName, databaseResult.Tenant)

	// Get a database that does not exist in a tenant that does exist and expect an error
	_, err = suite.coordinator.GetDatabase(ctx, &model.GetDatabase{
		Name:   "new_database1",
		Tenant: newTenantName,
	})
	suite.Error(err)

	// Get a database that does not exist in a tenant that does not exist and expect an
	// error
	_, err = suite.coordinator.GetDatabase(ctx, &model.GetDatabase{
		Name:   "new_database1",
		Tenant: "tenant2",
	})
	suite.Error(err)

	// clean up
	err = dao.CleanUpTestTenant(suite.db, newTenantName)
	suite.NoError(err)
	err = dao.CleanUpTestDatabase(suite.db, suite.tenantName, newDatabaseName)
	suite.NoError(err)
}

func SampleSegments(sampleCollections []*model.Collection) []*model.Segment {
	metadata1 := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	metadata1.Set("test_str", &model.SegmentMetadataValueStringType{Value: "str1"})
	metadata1.Set("test_int", &model.SegmentMetadataValueInt64Type{Value: 1})
	metadata1.Set("test_float", &model.SegmentMetadataValueFloat64Type{Value: 1.3})

	metadata2 := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	metadata2.Set("test_str", &model.SegmentMetadataValueStringType{Value: "str2"})
	metadata2.Set("test_int", &model.SegmentMetadataValueInt64Type{Value: 2})
	metadata2.Set("test_float", &model.SegmentMetadataValueFloat64Type{Value: 2.3})

	metadata3 := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	metadata3.Set("test_str", &model.SegmentMetadataValueStringType{Value: "str3"})
	metadata3.Set("test_int", &model.SegmentMetadataValueInt64Type{Value: 3})
	metadata3.Set("test_float", &model.SegmentMetadataValueFloat64Type{Value: 3.3})

	sampleSegments := []*model.Segment{
		{
			ID:           types.MustParse("00000000-d7d7-413b-92e1-731098a6e492"),
			Type:         "test_type_a",
			Scope:        "VECTOR",
			CollectionID: sampleCollections[0].ID,
			Metadata:     metadata1,
			FilePaths:    map[string][]string{},
		},
		{
			ID:           types.MustParse("11111111-d7d7-413b-92e1-731098a6e492"),
			Type:         "test_type_b",
			Scope:        "VECTOR",
			CollectionID: sampleCollections[1].ID,
			Metadata:     metadata2,
			FilePaths:    map[string][]string{},
		},
	}
	return sampleSegments
}

func (suite *APIsTestSuite) TestCreateGetDeleteSegments() {
	ctx := context.Background()
	c := suite.coordinator

	sampleSegments := SampleSegments(suite.sampleCollections)
	for _, segment := range sampleSegments {
		errSegmentCreation := c.CreateSegment(ctx, &model.Segment{
			ID:           segment.ID,
			Type:         segment.Type,
			Scope:        segment.Scope,
			CollectionID: segment.CollectionID,
			Metadata:     segment.Metadata,
		})
		suite.NoError(errSegmentCreation)

		// Create segment with empty collection id fails
		errSegmentCreation = c.CreateSegment(ctx, &model.Segment{
			ID:           segment.ID,
			Type:         segment.Type,
			Scope:        segment.Scope,
			CollectionID: types.NilUniqueID(),
			Metadata:     segment.Metadata,
		})
		suite.Error(errSegmentCreation)

		// Create segment to test unique constraint violation on segment.id.
		// This should fail because the id is already taken.
		errSegmentCreation = c.CreateSegment(ctx, &model.Segment{
			ID:           segment.ID,
			Type:         segment.Type,
			Scope:        segment.Scope,
			CollectionID: types.MustParse("00000000-d7d7-413b-92e1-731098a6e777"),
			Metadata:     segment.Metadata,
		})
		suite.Error(errSegmentCreation)
	}

	var results []*model.Segment
	for _, segment := range sampleSegments {
		result, err := c.GetSegments(ctx, segment.ID, nil, nil, segment.CollectionID)
		suite.NoError(err)
		suite.Equal([]*model.Segment{segment}, result)
		results = append(results, result...)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID.String() < results[j].ID.String()
	})
	suite.Equal(sampleSegments, results)

	// Duplicate create fails
	err := c.CreateSegment(ctx, &model.Segment{
		ID:           sampleSegments[0].ID,
		Type:         sampleSegments[0].Type,
		Scope:        sampleSegments[0].Scope,
		CollectionID: sampleSegments[0].CollectionID,
		Metadata:     sampleSegments[0].Metadata,
	})
	suite.Error(err)

	// Find by id
	for _, segment := range sampleSegments {
		result, err := c.GetSegments(ctx, segment.ID, nil, nil, segment.CollectionID)
		suite.NoError(err)
		suite.Equal([]*model.Segment{segment}, result)
	}

	// Find by type
	testTypeA := "test_type_a"
	result, err := c.GetSegments(ctx, types.NilUniqueID(), &testTypeA, nil, suite.sampleCollections[0].ID)
	suite.NoError(err)
	suite.Equal(sampleSegments[:1], result)

	testTypeB := "test_type_b"
	result, err = c.GetSegments(ctx, types.NilUniqueID(), &testTypeB, nil, suite.sampleCollections[1].ID)
	suite.NoError(err)
	suite.ElementsMatch(sampleSegments[1:], result)

	// Find by collection ID
	result, err = c.GetSegments(ctx, types.NilUniqueID(), nil, nil, suite.sampleCollections[0].ID)
	suite.NoError(err)
	suite.Equal(sampleSegments[:1], result)

	// Find by type and collection ID (positive case)
	result, err = c.GetSegments(ctx, types.NilUniqueID(), &testTypeA, nil, suite.sampleCollections[0].ID)
	suite.NoError(err)
	suite.Equal(sampleSegments[:1], result)

	// Find by type and collection ID (negative case)
	result, err = c.GetSegments(ctx, types.NilUniqueID(), &testTypeB, nil, suite.sampleCollections[0].ID)
	suite.NoError(err)
	suite.Empty(result)

	// Delete Segments will not delete the collection, hence no tests for that.
	// Reason - After introduction of Atomic delete of collection & segments,
	// the DeleteSegment API will not delete the collection. See comments in
	// coordinator.go/DeleteSegment for more details.
	s1 := sampleSegments[0]
	err = c.DeleteSegment(ctx, s1.ID, s1.CollectionID)
	suite.NoError(err)

	results, err = c.GetSegments(ctx, types.NilUniqueID(), nil, nil, s1.CollectionID)
	suite.NoError(err)
	suite.Contains(results, s1)
}

func (suite *APIsTestSuite) TestUpdateSegment() {
	metadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	metadata.Set("test_str", &model.SegmentMetadataValueStringType{Value: "str1"})
	metadata.Set("test_int", &model.SegmentMetadataValueInt64Type{Value: 1})
	metadata.Set("test_float", &model.SegmentMetadataValueFloat64Type{Value: 1.3})

	segment := &model.Segment{
		ID:           types.UniqueID(uuid.New()),
		Type:         "test_type_a",
		Scope:        "VECTOR",
		CollectionID: suite.sampleCollections[0].ID,
		Metadata:     metadata,
		FilePaths:    map[string][]string{},
	}

	ctx := context.Background()
	errSegmentCreation := suite.coordinator.CreateSegment(ctx, &model.Segment{
		ID:           segment.ID,
		Type:         segment.Type,
		Scope:        segment.Scope,
		CollectionID: segment.CollectionID,
		Metadata:     segment.Metadata,
	})
	suite.NoError(errSegmentCreation)

	collectionID := segment.CollectionID.String()

	// TODO: revisit why we need this
	// Update collection to new value
	//segment.CollectionID = sampleCollections[1].ID
	//newCollecionID := segment.CollectionID.String()
	//c.UpdateSegment(ctx, &model.UpdateSegment{
	//	ID:         segment.ID,
	//	Collection: &newCollecionID,
	//})
	//result, err = c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
	//assert.NoError(t, err)
	//assert.Equal(t, []*model.Segment{segment}, result)

	// Update collection to None
	//segment.CollectionID = types.NilUniqueID()
	//c.UpdateSegment(ctx, &model.UpdateSegment{
	//	ID:              segment.ID,
	//	Collection:      nil,
	//	ResetCollection: true,
	//})
	//result, err = c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
	//assert.NoError(t, err)
	//assert.Equal(t, []*model.Segment{segment}, result)

	// Add a new metadata key
	segment.Metadata.Set("test_str2", &model.SegmentMetadataValueStringType{Value: "str2"})
	_, err := suite.coordinator.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Metadata:   segment.Metadata})
	suite.NoError(err)
	result, err := suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, segment.CollectionID)
	suite.NoError(err)
	suite.Equal([]*model.Segment{segment}, result)

	// Update a metadata key
	segment.Metadata.Set("test_str", &model.SegmentMetadataValueStringType{Value: "str3"})
	_, err = suite.coordinator.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Metadata:   segment.Metadata})
	suite.NoError(err)
	result, err = suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, segment.CollectionID)
	suite.NoError(err)
	suite.Equal([]*model.Segment{segment}, result)

	// Delete a metadata key
	segment.Metadata.Remove("test_str")
	newMetadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	newMetadata.Set("test_str", nil)
	_, err = suite.coordinator.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Metadata:   newMetadata})
	suite.NoError(err)
	result, err = suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, segment.CollectionID)
	suite.NoError(err)
	suite.Equal([]*model.Segment{segment}, result)

	// Delete all metadata keys
	segment.Metadata = nil
	_, err = suite.coordinator.UpdateSegment(ctx, &model.UpdateSegment{
		Collection:    &collectionID,
		ID:            segment.ID,
		Metadata:      segment.Metadata,
		ResetMetadata: true},
	)
	suite.NoError(err)
	result, err = suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, segment.CollectionID)
	suite.NoError(err)
	suite.Equal([]*model.Segment{segment}, result)
}

// TestSoftAndHardDeleteCollection tests the soft and hard delete scenarios for collections.
func (suite *APIsTestSuite) TestSoftAndHardDeleteCollection() {
	ctx := context.Background()

	// Test Hard Delete scenario
	// Create test collection
	testCollection2 := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "test_hard_delete_collection",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	// Create the collection
	_, _, err := suite.coordinator.CreateCollection(ctx, testCollection2)
	suite.NoError(err)

	// Hard delete the collection
	err = suite.coordinator.SoftDeleteCollection(ctx, &model.DeleteCollection{
		ID:           testCollection2.ID,
		TenantID:     testCollection2.TenantID,
		DatabaseName: testCollection2.DatabaseName,
	})
	suite.NoError(err)
	err = suite.coordinator.FinishCollectionDeletion(ctx, &model.DeleteCollection{
		ID:           testCollection2.ID,
		TenantID:     testCollection2.TenantID,
		DatabaseName: testCollection2.DatabaseName,
	})
	suite.NoError(err)

	// Verify collection is not returned in normal get
	results, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{testCollection2.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Empty(results)

	// Verify collection does not appear in soft deleted list
	id := testCollection2.ID.String()
	softDeletedResults, err := suite.coordinator.GetSoftDeletedCollections(ctx, &id, suite.tenantName, suite.databaseName, 10)
	suite.NoError(err)
	suite.Empty(softDeletedResults)

	// Test Soft Delete scenario
	// Create a test collection
	testCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "test_soft_delete_collection",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	// Create the collection
	_, _, err = suite.coordinator.CreateCollection(ctx, testCollection)
	suite.NoError(err)

	// Soft delete the collection
	err = suite.coordinator.SoftDeleteCollection(ctx, &model.DeleteCollection{
		ID:           testCollection.ID,
		TenantID:     testCollection.TenantID,
		DatabaseName: testCollection.DatabaseName,
	})
	suite.NoError(err)

	// Verify collection is not returned in normal get
	results, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{testCollection.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Empty(results)

	// Do a flush collection compaction
	flushCollectionInfo, err := suite.coordinator.FlushCollectionCompaction(ctx, &model.FlushCollectionCompaction{
		ID:       testCollection.ID,
		TenantID: testCollection.TenantID,
	})
	// The flush collection compaction should fail because the collection is soft deleted.
	suite.Error(err)
	// Check for ErrCollectionSoftDeleted error.
	suite.True(errors.Is(err, common.ErrCollectionSoftDeleted))
	// Check that the flush collection info is nil.
	suite.Nil(flushCollectionInfo)

	// Verify collection appears in soft deleted list
	id = testCollection.ID.String()
	softDeletedResults, err = suite.coordinator.GetSoftDeletedCollections(ctx, &id, suite.tenantName, suite.databaseName, 10)
	suite.NoError(err)
	suite.Len(softDeletedResults, 1)
	suite.Equal(testCollection.ID, softDeletedResults[0].ID)

	// Create a new collection with the same name as the soft deleted one.
	// This should pass, and create a new soft deleted collection whose name is
	// of the form "deleted_<name>_<timestamp>"
	newTestCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         testCollection.Name,
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}
	_, _, err = suite.coordinator.CreateCollection(ctx, newTestCollection)
	suite.NoError(err)

	// Get the newly created collection to verify it exists
	results, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{newTestCollection.ID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(results, 1)
	suite.Equal(newTestCollection.Name, results[0].Name)

	// Verify the soft deleted collection still appears in the soft deleted list but with a different name.
	softDeletedResults, err = suite.coordinator.GetSoftDeletedCollections(ctx, nil, suite.tenantName, suite.databaseName, 10)
	suite.NoError(err)
	suite.Len(softDeletedResults, 1)
	suite.Equal(id, softDeletedResults[0].ID.String())
	renamedCollectionNamePrefix := fmt.Sprintf("deleted_%s_", testCollection.Name)
	suite.Contains(softDeletedResults[0].Name, renamedCollectionNamePrefix)
}

func (suite *APIsTestSuite) TestCollectionVersioningWithMinio() {
	ctx := context.Background()

	collectionID := types.NewUniqueID()
	// Create a new collection
	newCollection := &model.CreateCollection{
		ID:           collectionID,
		Name:         "test_collection_versioning",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	segments := []*model.Segment{
		{
			ID:           types.NewUniqueID(),
			Type:         "test_type_a",
			Scope:        "VECTOR",
			CollectionID: collectionID,
		},
	}

	// Create collection
	createdCollection, created, err := suite.coordinator.CreateCollectionAndSegments(ctx, newCollection, segments)
	suite.NoError(err)
	suite.True(created)
	suite.Equal(newCollection.ID, createdCollection.ID)
	suite.Equal(newCollection.Name, createdCollection.Name)

	// Do a flush collection compaction
	flushInfo, err := suite.coordinator.FlushCollectionCompaction(ctx, &model.FlushCollectionCompaction{
		ID:                       newCollection.ID,
		TenantID:                 newCollection.TenantID,
		LogPosition:              0,
		CurrentCollectionVersion: 0,
		FlushSegmentCompactions: []*model.FlushSegmentCompaction{
			{
				ID:        types.NewUniqueID(),
				FilePaths: map[string][]string{"file_1": {"path_1"}},
			},
		},
	})
	suite.NoError(err)
	suite.NotNil(flushInfo)

	// Assert that num_versions is 2
	type NumVersions struct {
		NumVersions int64 `gorm:"column:num_versions"`
	}
	res := &NumVersions{}
	suite.db.Select("num_versions").Table("collections").Find(&res, "id = ?", newCollection.ID.String())
	suite.Equal(int64(2), res.NumVersions)

	// TODO(rohitcp): Add these tests back once version file is enabled.
	// Verify version file exists in S3
	// versionFilePathPrefix := suite.s3MetaStore.GetVersionFilePath(newCollection.TenantID, newCollection.ID.String(), "")
	// exists, err := suite.s3MetaStore.HasObjectWithPrefix(ctx, versionFilePathPrefix)
	// suite.NoError(err)
	// suite.True(exists, "Version file should exist in S3")
}

func findSegmentInfo(segmentID types.UniqueID, segmentInfos []*coordinatorpb.FlushSegmentCompactionInfo) *coordinatorpb.FlushSegmentCompactionInfo {
	for _, segmentInfo := range segmentInfos {
		if segmentInfo.SegmentId == segmentID.String() {
			return segmentInfo
		}
	}
	return nil
}

func assertExpectedSegmentInfoExist(suite *APIsTestSuite, expectedSegment *model.Segment, segmentInfos []*coordinatorpb.FlushSegmentCompactionInfo) {
	segmentInfo := findSegmentInfo(expectedSegment.ID, segmentInfos)
	suite.NotNil(segmentInfo)

	if expectedSegment.FilePaths == nil {
		suite.Nil(segmentInfo.FilePaths)
		return
	}

	suite.NotNil(segmentInfo.FilePaths)

	filePaths := map[string][]string{}
	for key, filePath := range segmentInfo.FilePaths {
		filePaths[key] = filePath.Paths
	}
	suite.Equal(filePaths, expectedSegment.FilePaths)
}

func (suite *APIsTestSuite) TestForkCollection() {
	ctx := context.Background()

	sourceCreateCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "test_fork_collection_source",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	sourceCreateMetadataSegment := &model.Segment{
		ID:           types.NewUniqueID(),
		Type:         "test_blockfile",
		Scope:        "METADATA",
		CollectionID: sourceCreateCollection.ID,
	}

	sourceCreateRecordSegment := &model.Segment{
		ID:           types.NewUniqueID(),
		Type:         "test_blockfile",
		Scope:        "RECORD",
		CollectionID: sourceCreateCollection.ID,
	}

	sourceCreateVectorSegment := &model.Segment{
		ID:           types.NewUniqueID(),
		Type:         "test_hnsw",
		Scope:        "VECTOR",
		CollectionID: sourceCreateCollection.ID,
	}

	segments := []*model.Segment{
		sourceCreateMetadataSegment,
		sourceCreateRecordSegment,
		sourceCreateVectorSegment,
	}

	// Create source collection
	_, _, err := suite.coordinator.CreateCollectionAndSegments(ctx, sourceCreateCollection, segments)
	suite.NoError(err)

	sourceFlushMetadataSegment := &model.FlushSegmentCompaction{
		ID: sourceCreateMetadataSegment.ID,
		FilePaths: map[string][]string{
			"fts_index": {"metadata_sparse_index_file"},
		},
	}

	sourceFlushRecordSegment := &model.FlushSegmentCompaction{
		ID: sourceCreateRecordSegment.ID,
		FilePaths: map[string][]string{
			"data_record": {"record_sparse_index_file"},
		},
	}

	sourceFlushVectorSegment := &model.FlushSegmentCompaction{
		ID: sourceCreateVectorSegment.ID,
		FilePaths: map[string][]string{
			"hnsw_index": {"hnsw_source_layer_file"},
		},
	}

	sourceFlushCollectionCompaction := &model.FlushCollectionCompaction{
		ID:                       sourceCreateCollection.ID,
		TenantID:                 sourceCreateCollection.TenantID,
		LogPosition:              1000,
		CurrentCollectionVersion: 0,
		FlushSegmentCompactions: []*model.FlushSegmentCompaction{
			sourceFlushMetadataSegment,
			sourceFlushRecordSegment,
			sourceFlushVectorSegment,
		},
		TotalRecordsPostCompaction: 1000,
		SizeBytesPostCompaction:    65536,
	}

	// Flush some data to sourceo collection
	_, err = suite.coordinator.FlushCollectionCompaction(ctx, sourceFlushCollectionCompaction)
	suite.NoError(err)

	// Fork source collection
	forkCollection := &model.ForkCollection{
		SourceCollectionID:                   sourceCreateCollection.ID,
		SourceCollectionLogCompactionOffset:  800,
		SourceCollectionLogEnumerationOffset: 1200,
		TargetCollectionID:                   types.NewUniqueID(),
		TargetCollectionName:                 "test_fork_collection_fork_1",
	}

	collection, collection_segments, err := suite.coordinator.ForkCollection(ctx, forkCollection)
	suite.NoError(err)
	suite.Equal(forkCollection.TargetCollectionID, collection.ID)
	suite.Equal(forkCollection.TargetCollectionName, collection.Name)
	suite.Equal(sourceCreateCollection.ID, *collection.RootCollectionID)
	suite.Equal(sourceCreateCollection.TenantID, collection.TenantID)
	suite.Equal(sourceCreateCollection.DatabaseName, collection.DatabaseName)
	suite.Equal(sourceFlushCollectionCompaction.LogPosition, collection.LogPosition)
	suite.Equal(sourceFlushCollectionCompaction.TotalRecordsPostCompaction, collection.TotalRecordsPostCompaction)
	suite.Equal(sourceFlushCollectionCompaction.SizeBytesPostCompaction, collection.SizeBytesPostCompaction)
	for _, segment := range collection_segments {
		suite.Equal(collection.ID, segment.CollectionID)
		suite.Contains([]string{"METADATA", "RECORD", "VECTOR"}, segment.Scope)
		if segment.Scope == "METADATA" {
			suite.NotEqual(sourceCreateMetadataSegment.ID, segment.ID)
			suite.Equal(sourceFlushMetadataSegment.FilePaths, segment.FilePaths)
		} else if segment.Scope == "RECORD" {
			suite.NotEqual(sourceCreateRecordSegment.ID, segment.ID)
			suite.Equal(sourceFlushRecordSegment.FilePaths, segment.FilePaths)
		} else if segment.Scope == "VECTOR" {
			suite.NotEqual(sourceCreateVectorSegment.ID, segment.ID)
			suite.Equal(sourceFlushVectorSegment.FilePaths, segment.FilePaths)
		}
	}

	// Check version file of forked collection
	suite.Equal(collection.RootCollectionID, &sourceCreateCollection.ID)
	suite.NotNil(collection.VersionFileName)
	versionFile, err := suite.s3MetaStore.GetVersionFile(context.Background(), collection.VersionFileName)
	suite.NoError(err)
	suite.NotNil(versionFile)
	v0 := versionFile.VersionHistory.Versions[0]
	suite.NotNil(v0)
	// Validate file paths of segments
	suite.NotNil(v0.SegmentInfo)
	suite.NotNil(v0.SegmentInfo.SegmentCompactionInfo)
	suite.Equal(len(v0.SegmentInfo.SegmentCompactionInfo), 3)

	for _, segment := range collection_segments {
		assertExpectedSegmentInfoExist(suite, segment, v0.SegmentInfo.SegmentCompactionInfo)
	}

	// Attempt to fork a collcetion with same name (should fail)
	forkCollectionWithSameName := &model.ForkCollection{
		SourceCollectionID:                   sourceCreateCollection.ID,
		SourceCollectionLogCompactionOffset:  800,
		SourceCollectionLogEnumerationOffset: 1200,
		TargetCollectionID:                   types.NewUniqueID(),
		TargetCollectionName:                 "test_fork_collection_source",
	}
	_, _, err = suite.coordinator.ForkCollection(ctx, forkCollectionWithSameName)
	suite.Error(err)

	// Check that the collection was not created
	collections, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{forkCollectionWithSameName.TargetCollectionID}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Empty(collections)

	res, err := suite.coordinator.ListCollectionsToGc(ctx, nil, nil, nil, nil)
	suite.NoError(err)
	suite.NotEmpty(res)
	suite.Equal(1, len(res))
	// ListCollectionsToGc groups by fork trees and should always return the root of the tree
	suite.Equal(forkCollectionWithSameName.SourceCollectionID, res[0].ID)

	// Collection has 2 versions, so setting minVersionsIfAlive to 2 should return 1 collection
	minVersionsIfAlive := uint64(2)
	res, err = suite.coordinator.ListCollectionsToGc(ctx, nil, nil, nil, &minVersionsIfAlive)
	suite.NoError(err)
	suite.Equal(1, len(res))

	// Collection has 2 versions, so setting minVersionsIfAlive to 3 should return 0 collections
	minVersionsIfAlive = uint64(3)
	res, err = suite.coordinator.ListCollectionsToGc(ctx, nil, nil, nil, &minVersionsIfAlive)
	suite.NoError(err)
	suite.Equal(0, len(res))

	// Get source collection to grab lineage path and validate it exists
	sourceCollection, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{sourceCreateCollection.ID}, nil, sourceCreateCollection.TenantID, sourceCreateCollection.DatabaseName, nil, nil, false)
	suite.NoError(err)
	suite.Equal(1, len(sourceCollection))
	exists, err := suite.s3MetaStore.HasObjectWithPrefix(ctx, *sourceCollection[0].LineageFileName)
	suite.NoError(err)
	suite.True(exists, "Lineage file should exist in S3")

	// If the collection is soft deleted, it should always be returned by ListCollectionsToGc, even if it does not meet the minVersionsIfAlive criteria
	err = suite.coordinator.catalog.DeleteCollection(ctx, &model.DeleteCollection{
		ID:           sourceCreateCollection.ID,
		TenantID:     sourceCreateCollection.TenantID,
		DatabaseName: sourceCreateCollection.DatabaseName,
	}, true)
	suite.NoError(err)

	minVersionsIfAlive = uint64(3)
	res, err = suite.coordinator.ListCollectionsToGc(ctx, nil, nil, nil, &minVersionsIfAlive)
	suite.NoError(err)
	suite.Equal(1, len(res))
}

func (suite *APIsTestSuite) TestBatchGetCollectionVersionFilePaths() {
	ctx := context.Background()

	// Create a new collection
	newCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "test_batch_get_collection_version_file_paths",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	newSegments := []*model.Segment{}

	// Create the collection
	suite.coordinator.catalog.versionFileEnabled = true
	_, _, err := suite.coordinator.CreateCollectionAndSegments(ctx, newCollection, newSegments)
	suite.NoError(err)

	// Get the version file paths for the collection
	versionFilePaths, err := suite.coordinator.BatchGetCollectionVersionFilePaths(ctx, &coordinatorpb.BatchGetCollectionVersionFilePathsRequest{
		CollectionIds: []string{newCollection.ID.String()},
	})
	suite.NoError(err)
	suite.Len(versionFilePaths.CollectionIdToVersionFilePath, 1)

	// Verify version file exists in S3
	exists, err := suite.s3MetaStore.HasObjectWithPrefix(ctx, versionFilePaths.CollectionIdToVersionFilePath[newCollection.ID.String()])
	suite.NoError(err)
	suite.True(exists, "Version file should exist in S3")
}

func (suite *APIsTestSuite) TestCountForks() {
	ctx := context.Background()

	sourceCreateCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "test_fork_collection_source",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	sourceCreateMetadataSegment := &model.Segment{
		ID:           types.NewUniqueID(),
		Type:         "test_blockfile",
		Scope:        "METADATA",
		CollectionID: sourceCreateCollection.ID,
	}

	sourceCreateRecordSegment := &model.Segment{
		ID:           types.NewUniqueID(),
		Type:         "test_blockfile",
		Scope:        "RECORD",
		CollectionID: sourceCreateCollection.ID,
	}

	sourceCreateVectorSegment := &model.Segment{
		ID:           types.NewUniqueID(),
		Type:         "test_hnsw",
		Scope:        "VECTOR",
		CollectionID: sourceCreateCollection.ID,
	}

	segments := []*model.Segment{
		sourceCreateMetadataSegment,
		sourceCreateRecordSegment,
		sourceCreateVectorSegment,
	}

	_, _, err := suite.coordinator.CreateCollectionAndSegments(ctx, sourceCreateCollection, segments)
	suite.NoError(err)

	sourceFlushMetadataSegment := &model.FlushSegmentCompaction{
		ID: sourceCreateMetadataSegment.ID,
		FilePaths: map[string][]string{
			"fts_index": {"metadata_sparse_index_file"},
		},
	}

	sourceFlushRecordSegment := &model.FlushSegmentCompaction{
		ID: sourceCreateRecordSegment.ID,
		FilePaths: map[string][]string{
			"data_record": {"record_sparse_index_file"},
		},
	}

	sourceFlushVectorSegment := &model.FlushSegmentCompaction{
		ID: sourceCreateVectorSegment.ID,
		FilePaths: map[string][]string{
			"hnsw_index": {"hnsw_source_layer_file"},
		},
	}

	sourceFlushCollectionCompaction := &model.FlushCollectionCompaction{
		ID:                       sourceCreateCollection.ID,
		TenantID:                 sourceCreateCollection.TenantID,
		LogPosition:              1000,
		CurrentCollectionVersion: 0,
		FlushSegmentCompactions: []*model.FlushSegmentCompaction{
			sourceFlushMetadataSegment,
			sourceFlushRecordSegment,
			sourceFlushVectorSegment,
		},
		TotalRecordsPostCompaction: 1000,
		SizeBytesPostCompaction:    65536,
	}

	// Flush some data to sourceo collection
	_, err = suite.coordinator.FlushCollectionCompaction(ctx, sourceFlushCollectionCompaction)
	suite.NoError(err)

	var forkedCollectionIDs []types.UniqueID

	// Create 5 forks from the source collection
	for i := 0; i < 5; i++ {
		forkCollection := &model.ForkCollection{
			SourceCollectionID:                   sourceCreateCollection.ID,
			SourceCollectionLogCompactionOffset:  800,
			SourceCollectionLogEnumerationOffset: 1200,
			TargetCollectionID:                   types.NewUniqueID(),
			TargetCollectionName:                 fmt.Sprintf("test_fork_collection_fork_source%d", i),
		}
		forkedCollection, _, err := suite.coordinator.ForkCollection(ctx, forkCollection)
		suite.NoError(err)
		forkedCollectionIDs = append(forkedCollectionIDs, forkedCollection.ID)
	}

	// Create 5 forks from one of the forked collections
	for i := 0; i < 5; i++ {
		forkCollection := &model.ForkCollection{
			SourceCollectionID:                   forkedCollectionIDs[0],
			SourceCollectionLogCompactionOffset:  800,
			SourceCollectionLogEnumerationOffset: 1200,
			TargetCollectionID:                   types.NewUniqueID(),
			TargetCollectionName:                 fmt.Sprintf("test_fork_collection_fork_forked%d", i),
		}
		forkedCollection, _, err := suite.coordinator.ForkCollection(ctx, forkCollection)
		suite.NoError(err)
		forkedCollectionIDs = append(forkedCollectionIDs, forkedCollection.ID)
	}

	count, err := suite.coordinator.CountForks(ctx, sourceCreateCollection.ID)
	suite.NoError(err)
	suite.Equal(uint64(10), count)

	// Check that each forked collection has 10 forks as well
	for _, forkedCollectionID := range forkedCollectionIDs {
		count, err := suite.coordinator.CountForks(ctx, forkedCollectionID)
		suite.NoError(err)
		suite.Equal(uint64(10), count)
	}
}

func (suite *APIsTestSuite) TestGetCollections() {
	ctx := context.Background()

	// Does not error if collection is not found
	result, err := suite.coordinator.GetCollections(ctx, []types.UniqueID{types.NewUniqueID()}, nil, suite.tenantName, suite.databaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(result, 0)

	createCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "collection_1",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	_, _, err = suite.coordinator.CreateCollectionAndSegments(ctx, createCollection, []*model.Segment{})
	suite.NoError(err)

	// Can fetch the collection by ID
	result, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{createCollection.ID}, nil, createCollection.TenantID, createCollection.DatabaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(createCollection.ID, result[0].ID)

	// Soft delete collection
	err = suite.coordinator.SoftDeleteCollection(ctx, &model.DeleteCollection{
		ID:           createCollection.ID,
		TenantID:     createCollection.TenantID,
		DatabaseName: createCollection.DatabaseName,
	})
	suite.NoError(err)

	// Is not returned when include soft deleted is false
	result, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{createCollection.ID}, nil, createCollection.TenantID, createCollection.DatabaseName, nil, nil, false)
	suite.NoError(err)
	suite.Len(result, 0)

	// Is returned when include soft deleted is true
	result, err = suite.coordinator.GetCollections(ctx, []types.UniqueID{createCollection.ID}, nil, createCollection.TenantID, createCollection.DatabaseName, nil, nil, true)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(createCollection.ID, result[0].ID)
}

func (suite *APIsTestSuite) TestGetCollectionByResourceName() {
	ctx := context.Background()

	testCollection := &model.CreateCollection{
		ID:           types.NewUniqueID(),
		Name:         "test_collection_by_resource_name",
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	}

	_, _, err := suite.coordinator.CreateCollection(ctx, testCollection)
	suite.NoError(err)

	tenantResourceName := "test_tenant_resource_name"
	err = suite.coordinator.SetTenantResourceName(ctx, suite.tenantName, tenantResourceName)
	suite.NoError(err)

	collection, err := suite.coordinator.GetCollectionByResourceName(ctx, tenantResourceName, suite.databaseName, testCollection.Name)
	suite.NoError(err)
	suite.Equal(testCollection.ID, collection.ID)
	suite.Equal(testCollection.Name, collection.Name)
	suite.Equal(testCollection.TenantID, collection.TenantID)
	suite.Equal(testCollection.DatabaseName, collection.DatabaseName)

	_, err = suite.coordinator.GetCollectionByResourceName(ctx, tenantResourceName, suite.databaseName, "non_existent_collection")
	suite.Error(err)
	suite.True(errors.Is(err, common.ErrCollectionNotFound))

	_, err = suite.coordinator.GetCollectionByResourceName(ctx, tenantResourceName, "non_existent_database", testCollection.Name)
	suite.Error(err)
	suite.True(errors.Is(err, common.ErrCollectionNotFound))

	_, err = suite.coordinator.GetCollectionByResourceName(ctx, "non_existent_tenant_resource_name", suite.databaseName, testCollection.Name)
	suite.Error(err)
	suite.True(errors.Is(err, common.ErrCollectionNotFound))
}

func TestAPIsTestSuite(t *testing.T) {
	testSuite := new(APIsTestSuite)
	suite.Run(t, testSuite)
}
