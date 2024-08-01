package coordinator

import (
	"context"
	"sort"
	"strconv"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dao"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
	"pgregory.net/rapid"
)

type APIsTestSuite struct {
	suite.Suite
	db                *gorm.DB
	collectionId1     types.UniqueID
	collectionId2     types.UniqueID
	records           [][]byte
	tenantName        string
	databaseName      string
	databaseId        string
	sampleCollections []*model.Collection
	coordinator       *Coordinator
}

func (suite *APIsTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db = dbcore.ConfigDatabaseForTesting()
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
	c, err := NewCoordinator(ctx, suite.db, nil, nil)
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
	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	c, err := NewCoordinator(ctx, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
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
				collectionList, err := c.GetCollections(ctx, collection.ID, nil, common.DefaultTenant, common.DefaultDatabase, nil, nil)
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
	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	c, err := NewCoordinator(ctx, db, nil, nil)
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
			segment := rapid.Custom[*model.CreateSegment](func(t *rapid.T) *model.CreateSegment {
				return &model.CreateSegment{
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

func (suite *APIsTestSuite) TestCreateGetDeleteCollections() {
	ctx := context.Background()
	results, err := suite.coordinator.GetCollections(ctx, types.NilUniqueID(), nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})
	suite.Equal(suite.sampleCollections, results)

	// Duplicate create fails
	_, _, err = suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           suite.sampleCollections[0].ID,
		Name:         suite.sampleCollections[0].Name,
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
	})
	suite.Error(err)

	// Find by name
	for _, collection := range suite.sampleCollections {
		result, err := suite.coordinator.GetCollections(ctx, types.NilUniqueID(), &collection.Name, suite.tenantName, suite.databaseName, nil, nil)
		suite.NoError(err)
		suite.Equal([]*model.Collection{collection}, result)
	}

	// Find by id
	for _, collection := range suite.sampleCollections {
		result, err := suite.coordinator.GetCollections(ctx, collection.ID, nil, suite.tenantName, suite.databaseName, nil, nil)
		suite.NoError(err)
		suite.Equal([]*model.Collection{collection}, result)
	}

	// Delete
	c1 := suite.sampleCollections[0]
	deleteCollection := &model.DeleteCollection{
		ID:           c1.ID,
		DatabaseName: suite.databaseName,
		TenantID:     suite.tenantName,
	}
	err = suite.coordinator.DeleteCollection(ctx, deleteCollection)
	suite.NoError(err)

	results, err = suite.coordinator.GetCollections(ctx, types.NilUniqueID(), nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)

	suite.NotContains(results, c1)
	suite.Len(results, len(suite.sampleCollections)-1)
	suite.ElementsMatch(results, suite.sampleCollections[1:])
	byIDResult, err := suite.coordinator.GetCollections(ctx, c1.ID, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Empty(byIDResult)

	// Duplicate delete throws an exception
	err = suite.coordinator.DeleteCollection(ctx, deleteCollection)
	suite.Error(err)
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
	suite.Equal(coll, result)
	resultList, err := suite.coordinator.GetCollections(ctx, types.NilUniqueID(), &coll.Name, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Equal([]*model.Collection{coll}, resultList)

	// Update dimension
	newDimension := int32(128)
	coll.Dimension = &newDimension
	result, err = suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Dimension: coll.Dimension})
	suite.NoError(err)
	suite.Equal(coll, result)
	resultList, err = suite.coordinator.GetCollections(ctx, coll.ID, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Equal([]*model.Collection{coll}, resultList)

	// Reset the metadata
	newMetadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	newMetadata.Add("test_str2", &model.CollectionMetadataValueStringType{Value: "str2"})
	coll.Metadata = newMetadata
	result, err = suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Metadata: coll.Metadata})
	suite.NoError(err)
	suite.Equal(coll, result)
	resultList, err = suite.coordinator.GetCollections(ctx, coll.ID, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Equal([]*model.Collection{coll}, resultList)

	// Delete all metadata keys
	coll.Metadata = nil
	result, err = suite.coordinator.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Metadata: coll.Metadata, ResetMetadata: true})
	suite.NoError(err)
	suite.Equal(coll, result)
	resultList, err = suite.coordinator.GetCollections(ctx, coll.ID, nil, suite.tenantName, suite.databaseName, nil, nil)
	suite.NoError(err)
	suite.Equal([]*model.Collection{coll}, resultList)
}

func (suite *APIsTestSuite) TestGetOrCreateCollectionsTwice() {
	// GetOrCreateCollection already existing collection returns false for created
	ctx := context.Background()
	coll := suite.sampleCollections[0]
	_, created, err := suite.coordinator.CreateCollection(ctx, &model.CreateCollection{
		ID:           coll.ID,
		Name:         coll.Name,
		Metadata:     coll.Metadata,
		Dimension:    coll.Dimension,
		GetOrCreate:  true,
		TenantID:     coll.TenantID,
		DatabaseName: coll.DatabaseName,
	})
	suite.NoError(err)
	suite.False(created)
}

func (suite *APIsTestSuite) TestCreateUpdateWithDatabase() {
	ctx := context.Background()
	newDatabaseName := "test_apis_CreateUpdateWithDatabase"
	newDatabaseId := uuid.New().String()
	_, err := suite.coordinator.CreateDatabase(ctx, &model.CreateDatabase{
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
	result, err := suite.coordinator.GetCollections(ctx, suite.sampleCollections[1].ID, nil, suite.tenantName, suite.databaseName, nil, nil)
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
	result, err = suite.coordinator.GetCollections(ctx, suite.sampleCollections[0].ID, nil, suite.tenantName, newDatabaseName, nil, nil)
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
	result, err := suite.coordinator.GetCollections(ctx, types.NilUniqueID(), nil, suite.tenantName, newDatabaseName, nil, nil)
	suite.NoError(err)
	suite.Equal(len(suite.sampleCollections), len(result))
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	suite.Equal(suite.sampleCollections, result)

	result, err = suite.coordinator.GetCollections(ctx, types.NilUniqueID(), nil, suite.tenantName, suite.databaseName, nil, nil)
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
	result, err := suite.coordinator.GetCollections(ctx, types.NilUniqueID(), nil, newTenantName, newDatabaseName, nil, nil)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(expected[0], result[0])

	expected = []*model.Collection{suite.sampleCollections[1]}
	expected[0].TenantID = suite.tenantName
	expected[0].DatabaseName = newDatabaseName
	result, err = suite.coordinator.GetCollections(ctx, types.NilUniqueID(), nil, suite.tenantName, newDatabaseName, nil, nil)
	suite.NoError(err)
	suite.Len(result, 1)
	suite.Equal(expected[0], result[0])

	// A new tenant DOES NOT have a default database. This does not error, instead 0
	// results are returned
	result, err = suite.coordinator.GetCollections(ctx, types.NilUniqueID(), nil, newTenantName, suite.databaseName, nil, nil)
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
		{
			ID:           types.MustParse("22222222-d7d7-413b-92e1-731098a6e492"),
			Type:         "test_type_b",
			Scope:        "METADATA",
			CollectionID: types.NilUniqueID(),
			Metadata:     metadata3, // This segment is not assigned to any collection
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
		errSegmentCreation := c.CreateSegment(ctx, &model.CreateSegment{
			ID:           segment.ID,
			Type:         segment.Type,
			Scope:        segment.Scope,
			CollectionID: segment.CollectionID,
			Metadata:     segment.Metadata,
		})
		suite.NoError(errSegmentCreation)
	}

	var results []*model.Segment
	for _, segment := range sampleSegments {
		result, err := c.GetSegments(ctx, segment.ID, nil, nil, types.NilUniqueID())
		suite.NoError(err)
		suite.Equal([]*model.Segment{segment}, result)
		results = append(results, result...)
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID.String() < results[j].ID.String()
	})
	suite.Equal(sampleSegments, results)

	// Duplicate create fails
	err := c.CreateSegment(ctx, &model.CreateSegment{
		ID:           sampleSegments[0].ID,
		Type:         sampleSegments[0].Type,
		Scope:        sampleSegments[0].Scope,
		CollectionID: sampleSegments[0].CollectionID,
		Metadata:     sampleSegments[0].Metadata,
	})
	suite.Error(err)

	// Find by id
	for _, segment := range sampleSegments {
		result, err := c.GetSegments(ctx, segment.ID, nil, nil, types.NilUniqueID())
		suite.NoError(err)
		suite.Equal([]*model.Segment{segment}, result)
	}

	// Find by type
	testTypeA := "test_type_a"
	result, err := c.GetSegments(ctx, types.NilUniqueID(), &testTypeA, nil, types.NilUniqueID())
	suite.NoError(err)
	suite.Equal(sampleSegments[:1], result)

	testTypeB := "test_type_b"
	result, err = c.GetSegments(ctx, types.NilUniqueID(), &testTypeB, nil, types.NilUniqueID())
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

	// Delete
	s1 := sampleSegments[0]
	err = c.DeleteSegment(ctx, s1.ID, s1.CollectionID)
	suite.NoError(err)

	results, err = c.GetSegments(ctx, types.NilUniqueID(), nil, nil, types.NilUniqueID())
	suite.NoError(err)
	suite.NotContains(results, s1)
	suite.Len(results, len(sampleSegments)-1)
	suite.ElementsMatch(results, sampleSegments[1:])

	// Duplicate delete throws an exception
	err = c.DeleteSegment(ctx, s1.ID, s1.CollectionID)
	suite.Error(err)

	// clean up segments
	for _, segment := range sampleSegments {
		_ = c.DeleteSegment(ctx, segment.ID, segment.CollectionID)
	}
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
	errSegmentCreation := suite.coordinator.CreateSegment(ctx, &model.CreateSegment{
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
	result, err := suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, types.NilUniqueID())
	suite.NoError(err)
	suite.Equal([]*model.Segment{segment}, result)

	// Update a metadata key
	segment.Metadata.Set("test_str", &model.SegmentMetadataValueStringType{Value: "str3"})
	_, err = suite.coordinator.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Metadata:   segment.Metadata})
	suite.NoError(err)
	result, err = suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, types.NilUniqueID())
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
	result, err = suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, types.NilUniqueID())
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
	result, err = suite.coordinator.GetSegments(ctx, segment.ID, nil, nil, types.NilUniqueID())
	suite.NoError(err)
	suite.Equal([]*model.Segment{segment}, result)
}

func TestAPIsTestSuite(t *testing.T) {
	testSuite := new(APIsTestSuite)
	suite.Run(t, testSuite)
}
