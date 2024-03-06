package coordinator

import (
	"context"
	"sort"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"pgregory.net/rapid"
)

// TODO: This is not complete yet. We need to add more tests for the other APIs.
// We will deprecate the example based tests once we have enough tests here.
func testCollection(t *rapid.T) {
	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewSimpleAssignmentPolicy("test-tenant", "test-topic")
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
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

			_, err := c.CreateCollection(ctx, collection)
			if err != nil {
				if err == common.ErrCollectionNameEmpty && collection.Name == "" {
					t.Logf("expected error for empty collection name")
				} else if err == common.ErrCollectionTopicEmpty {
					t.Logf("expected error for empty collection topic")
				} else {
					t.Fatalf("error creating collection: %v", err)
				}
			}
			if err == nil {
				// verify the correctness
				collectionList, err := c.GetCollections(ctx, collection.ID, nil, nil, common.DefaultTenant, common.DefaultDatabase)
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
	assignmentPolicy := NewSimpleAssignmentPolicy("test-tenant", "test-topic")
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
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

	testTopic := "test-segment-topic"
	t.Repeat(map[string]func(*rapid.T){
		"create_segment": func(t *rapid.T) {
			segment := rapid.Custom[*model.CreateSegment](func(t *rapid.T) *model.CreateSegment {
				return &model.CreateSegment{
					ID:           types.MustParse(rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "segment_id")),
					Type:         "test-segment-type",
					Scope:        "test-segment-scope",
					Topic:        &testTopic,
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

func SampleCollections(t *testing.T, tenantID string, databaseName string) []*model.Collection {
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
			Topic:        "test_topic_1",
			Metadata:     metadata1,
			Dimension:    &dimension,
			TenantID:     tenantID,
			DatabaseName: databaseName,
		},
		{
			ID:           types.MustParse("f444f1d7-d06c-4357-ac22-5a4a1f92d761"),
			Name:         "test_collection_2",
			Topic:        "test_topic_2",
			Metadata:     metadata2,
			Dimension:    nil,
			TenantID:     tenantID,
			DatabaseName: databaseName,
		},
		{
			ID:           types.MustParse("43babc1a-e403-4a50-91a9-16621ba29ab0"),
			Name:         "test_collection_3",
			Topic:        "test_topic_3",
			Metadata:     metadata3,
			Dimension:    nil,
			TenantID:     tenantID,
			DatabaseName: databaseName,
		},
	}
	return sampleCollections
}

type MockAssignmentPolicy struct {
	collections []*model.Collection
}

func NewMockAssignmentPolicy(collecions []*model.Collection) *MockAssignmentPolicy {
	return &MockAssignmentPolicy{
		collections: collecions,
	}
}

func (m *MockAssignmentPolicy) AssignCollection(collectionID types.UniqueID) (string, error) {
	for _, collection := range m.collections {
		if collection.ID == collectionID {
			return collection.Topic, nil
		}
	}
	return "", common.ErrCollectionNotFound
}

func TestCreateGetDeleteCollections(t *testing.T) {

	sampleCollections := SampleCollections(t, common.DefaultTenant, common.DefaultDatabase)

	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewMockAssignmentPolicy(sampleCollections)
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	c.ResetState(ctx)

	for _, collection := range sampleCollections {
		c.CreateCollection(ctx, &model.CreateCollection{
			ID:           collection.ID,
			Name:         collection.Name,
			Topic:        collection.Topic,
			Metadata:     collection.Metadata,
			Dimension:    collection.Dimension,
			TenantID:     collection.TenantID,
			DatabaseName: collection.DatabaseName,
		})
	}

	results, err := c.GetCollections(ctx, types.NilUniqueID(), nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})

	assert.Equal(t, sampleCollections, results)

	// Duplicate create fails
	_, err = c.CreateCollection(ctx, &model.CreateCollection{
		ID:           sampleCollections[0].ID,
		Name:         sampleCollections[0].Name,
		TenantID:     common.DefaultTenant,
		DatabaseName: common.DefaultDatabase,
	})
	assert.Error(t, err)

	// Find by name
	for _, collection := range sampleCollections {
		result, err := c.GetCollections(ctx, types.NilUniqueID(), &collection.Name, nil, common.DefaultTenant, common.DefaultDatabase)
		assert.NoError(t, err)
		assert.Equal(t, []*model.Collection{collection}, result)
	}

	// Find by topic
	for _, collection := range sampleCollections {
		result, err := c.GetCollections(ctx, types.NilUniqueID(), nil, &collection.Topic, common.DefaultTenant, common.DefaultDatabase)
		assert.NoError(t, err)
		assert.Equal(t, []*model.Collection{collection}, result)
	}

	// Find by id
	for _, collection := range sampleCollections {
		result, err := c.GetCollections(ctx, collection.ID, nil, nil, common.DefaultTenant, common.DefaultDatabase)
		assert.NoError(t, err)
		assert.Equal(t, []*model.Collection{collection}, result)
	}

	// Find by id and topic (positive case)
	for _, collection := range sampleCollections {
		result, err := c.GetCollections(ctx, collection.ID, nil, &collection.Topic, common.DefaultTenant, common.DefaultDatabase)
		assert.NoError(t, err)
		assert.Equal(t, []*model.Collection{collection}, result)
	}

	// find by id and topic (negative case)
	for _, collection := range sampleCollections {
		otherTopic := "other topic"
		result, err := c.GetCollections(ctx, collection.ID, nil, &otherTopic, common.DefaultTenant, common.DefaultDatabase)
		assert.NoError(t, err)
		assert.Empty(t, result)
	}

	// Delete
	c1 := sampleCollections[0]
	deleteCollection := &model.DeleteCollection{
		ID:           c1.ID,
		DatabaseName: common.DefaultDatabase,
		TenantID:     common.DefaultTenant,
	}
	err = c.DeleteCollection(ctx, deleteCollection)
	assert.NoError(t, err)

	results, err = c.GetCollections(ctx, types.NilUniqueID(), nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)

	assert.NotContains(t, results, c1)
	assert.Len(t, results, len(sampleCollections)-1)
	assert.ElementsMatch(t, results, sampleCollections[1:])
	byIDResult, err := c.GetCollections(ctx, c1.ID, nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Empty(t, byIDResult)

	// Duplicate delete throws an exception
	err = c.DeleteCollection(ctx, deleteCollection)
	assert.Error(t, err)
}

func TestUpdateCollections(t *testing.T) {
	sampleCollections := SampleCollections(t, common.DefaultTenant, common.DefaultDatabase)

	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewMockAssignmentPolicy(sampleCollections)
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	c.ResetState(ctx)

	coll := &model.Collection{
		Name:         sampleCollections[0].Name,
		ID:           sampleCollections[0].ID,
		Topic:        sampleCollections[0].Topic,
		Metadata:     sampleCollections[0].Metadata,
		Dimension:    sampleCollections[0].Dimension,
		TenantID:     sampleCollections[0].TenantID,
		DatabaseName: sampleCollections[0].DatabaseName,
	}

	c.CreateCollection(ctx, &model.CreateCollection{
		ID:           coll.ID,
		Name:         coll.Name,
		Topic:        coll.Topic,
		Metadata:     coll.Metadata,
		Dimension:    coll.Dimension,
		TenantID:     coll.TenantID,
		DatabaseName: coll.DatabaseName,
	})

	// Update name
	coll.Name = "new_name"
	result, err := c.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Name: &coll.Name})
	assert.NoError(t, err)
	assert.Equal(t, coll, result)
	resultList, err := c.GetCollections(ctx, types.NilUniqueID(), &coll.Name, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Equal(t, []*model.Collection{coll}, resultList)

	// Update topic
	coll.Topic = "new_topic"
	result, err = c.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Topic: &coll.Topic})
	assert.NoError(t, err)
	assert.Equal(t, coll, result)
	resultList, err = c.GetCollections(ctx, types.NilUniqueID(), nil, &coll.Topic, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Equal(t, []*model.Collection{coll}, resultList)

	// Update dimension
	newDimension := int32(128)
	coll.Dimension = &newDimension
	result, err = c.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Dimension: coll.Dimension})
	assert.NoError(t, err)
	assert.Equal(t, coll, result)
	resultList, err = c.GetCollections(ctx, coll.ID, nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Equal(t, []*model.Collection{coll}, resultList)

	// Reset the metadata
	newMetadata := model.NewCollectionMetadata[model.CollectionMetadataValueType]()
	newMetadata.Add("test_str2", &model.CollectionMetadataValueStringType{Value: "str2"})
	coll.Metadata = newMetadata
	result, err = c.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Metadata: coll.Metadata})
	assert.NoError(t, err)
	assert.Equal(t, coll, result)
	resultList, err = c.GetCollections(ctx, coll.ID, nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Equal(t, []*model.Collection{coll}, resultList)

	// Delete all metadata keys
	coll.Metadata = nil
	result, err = c.UpdateCollection(ctx, &model.UpdateCollection{ID: coll.ID, Metadata: coll.Metadata, ResetMetadata: true})
	assert.NoError(t, err)
	assert.Equal(t, coll, result)
	resultList, err = c.GetCollections(ctx, coll.ID, nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Equal(t, []*model.Collection{coll}, resultList)
}

func TestCreateUpdateWithDatabase(t *testing.T) {
	sampleCollections := SampleCollections(t, common.DefaultTenant, common.DefaultDatabase)
	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewMockAssignmentPolicy(sampleCollections)
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	c.ResetState(ctx)
	_, err = c.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     types.MustParse("00000000-d7d7-413b-92e1-731098a6e492").String(),
		Name:   "new_database",
		Tenant: common.DefaultTenant,
	})
	assert.NoError(t, err)

	c.CreateCollection(ctx, &model.CreateCollection{
		ID:           sampleCollections[0].ID,
		Name:         sampleCollections[0].Name,
		Topic:        sampleCollections[0].Topic,
		Metadata:     sampleCollections[0].Metadata,
		Dimension:    sampleCollections[0].Dimension,
		TenantID:     sampleCollections[0].TenantID,
		DatabaseName: "new_database",
	})

	c.CreateCollection(ctx, &model.CreateCollection{
		ID:           sampleCollections[1].ID,
		Name:         sampleCollections[1].Name,
		Topic:        sampleCollections[1].Topic,
		Metadata:     sampleCollections[1].Metadata,
		Dimension:    sampleCollections[1].Dimension,
		TenantID:     sampleCollections[1].TenantID,
		DatabaseName: sampleCollections[1].DatabaseName,
	})

	newName1 := "new_name_1"
	c.UpdateCollection(ctx, &model.UpdateCollection{
		ID:   sampleCollections[1].ID,
		Name: &newName1,
	})

	result, err := c.GetCollections(ctx, sampleCollections[1].ID, nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "new_name_1", result[0].Name)

	newName0 := "new_name_0"
	c.UpdateCollection(ctx, &model.UpdateCollection{
		ID:   sampleCollections[0].ID,
		Name: &newName0,
	})
	result, err = c.GetCollections(ctx, sampleCollections[0].ID, nil, nil, common.DefaultTenant, "new_database")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "new_name_0", result[0].Name)
}

func TestGetMultipleWithDatabase(t *testing.T) {
	sampleCollections := SampleCollections(t, common.DefaultTenant, "new_database")
	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewMockAssignmentPolicy(sampleCollections)
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	c.ResetState(ctx)
	_, err = c.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     types.MustParse("00000000-d7d7-413b-92e1-731098a6e492").String(),
		Name:   "new_database",
		Tenant: common.DefaultTenant,
	})
	assert.NoError(t, err)

	for _, collection := range sampleCollections {
		c.CreateCollection(ctx, &model.CreateCollection{
			ID:           collection.ID,
			Name:         collection.Name,
			Topic:        collection.Topic,
			Metadata:     collection.Metadata,
			Dimension:    collection.Dimension,
			TenantID:     common.DefaultTenant,
			DatabaseName: "new_database",
		})
	}
	result, err := c.GetCollections(ctx, types.NilUniqueID(), nil, nil, common.DefaultTenant, "new_database")
	assert.NoError(t, err)
	assert.Equal(t, len(sampleCollections), len(result))
	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	assert.Equal(t, sampleCollections, result)

	result, err = c.GetCollections(ctx, types.NilUniqueID(), nil, nil, common.DefaultTenant, common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(result))
}

func TestCreateDatabaseWithTenants(t *testing.T) {
	sampleCollections := SampleCollections(t, common.DefaultTenant, common.DefaultDatabase)
	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewMockAssignmentPolicy(sampleCollections)
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	c.ResetState(ctx)

	// Create a new tenant
	_, err = c.CreateTenant(ctx, &model.CreateTenant{
		Name: "tenant1",
	})
	assert.NoError(t, err)

	// Create tenant that already exits and expect an error
	_, err = c.CreateTenant(ctx, &model.CreateTenant{
		Name: "tenant1",
	})
	assert.Error(t, err)

	// Create tenant that already exits and expect an error
	_, err = c.CreateTenant(ctx, &model.CreateTenant{
		Name: common.DefaultTenant,
	})
	assert.Error(t, err)

	// Create a new database within this tenant and also in the default tenant
	_, err = c.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     types.MustParse("33333333-d7d7-413b-92e1-731098a6e492").String(),
		Name:   "new_database",
		Tenant: "tenant1",
	})
	assert.NoError(t, err)

	_, err = c.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     types.MustParse("44444444-d7d7-413b-92e1-731098a6e492").String(),
		Name:   "new_database",
		Tenant: common.DefaultTenant,
	})
	assert.NoError(t, err)

	// Create a new collection in the new tenant
	_, err = c.CreateCollection(ctx, &model.CreateCollection{
		ID:           sampleCollections[0].ID,
		Name:         sampleCollections[0].Name,
		Topic:        sampleCollections[0].Topic,
		Metadata:     sampleCollections[0].Metadata,
		Dimension:    sampleCollections[0].Dimension,
		TenantID:     "tenant1",
		DatabaseName: "new_database",
	})
	assert.NoError(t, err)

	// Create a new collection in the default tenant
	c.CreateCollection(ctx, &model.CreateCollection{
		ID:           sampleCollections[1].ID,
		Name:         sampleCollections[1].Name,
		Topic:        sampleCollections[1].Topic,
		Metadata:     sampleCollections[1].Metadata,
		Dimension:    sampleCollections[1].Dimension,
		TenantID:     common.DefaultTenant,
		DatabaseName: "new_database",
	})

	// Check that both tenants have the correct collections
	expected := []*model.Collection{sampleCollections[0]}
	expected[0].TenantID = "tenant1"
	expected[0].DatabaseName = "new_database"
	result, err := c.GetCollections(ctx, types.NilUniqueID(), nil, nil, "tenant1", "new_database")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, expected[0], result[0])

	expected = []*model.Collection{sampleCollections[1]}
	expected[0].TenantID = common.DefaultTenant
	expected[0].DatabaseName = "new_database"
	result, err = c.GetCollections(ctx, types.NilUniqueID(), nil, nil, common.DefaultTenant, "new_database")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, expected[0], result[0])

	// A new tenant DOES NOT have a default database. This does not error, instead 0
	// results are returned
	result, err = c.GetCollections(ctx, types.NilUniqueID(), nil, nil, "tenant1", common.DefaultDatabase)
	assert.NoError(t, err)
	assert.Nil(t, result)
}

func TestCreateGetDeleteTenants(t *testing.T) {
	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewMockAssignmentPolicy(nil)
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	c.ResetState(ctx)

	// Create a new tenant
	_, err = c.CreateTenant(ctx, &model.CreateTenant{
		Name: "tenant1",
	})
	assert.NoError(t, err)

	// Create tenant that already exits and expect an error
	_, err = c.CreateTenant(ctx, &model.CreateTenant{
		Name: "tenant1",
	})
	assert.Error(t, err)

	// Create tenant that already exits and expect an error
	_, err = c.CreateTenant(ctx, &model.CreateTenant{
		Name: common.DefaultTenant,
	})
	assert.Error(t, err)

	// Get the tenant and check that it exists
	result, err := c.GetTenant(ctx, &model.GetTenant{Name: "tenant1"})
	assert.NoError(t, err)
	assert.Equal(t, "tenant1", result.Name)

	// Get a tenant that does not exist and expect an error
	_, err = c.GetTenant(ctx, &model.GetTenant{Name: "tenant2"})
	assert.Error(t, err)

	// Create a new database within this tenant
	_, err = c.CreateDatabase(ctx, &model.CreateDatabase{
		ID:     types.MustParse("33333333-d7d7-413b-92e1-731098a6e492").String(),
		Name:   "new_database",
		Tenant: "tenant1",
	})
	assert.NoError(t, err)

	// Get the database and check that it exists
	databaseResult, err := c.GetDatabase(ctx, &model.GetDatabase{
		Name:   "new_database",
		Tenant: "tenant1",
	})
	assert.NoError(t, err)
	assert.Equal(t, "new_database", databaseResult.Name)
	assert.Equal(t, "tenant1", databaseResult.Tenant)

	// Get a database that does not exist in a tenant that does exist and expect an error
	_, err = c.GetDatabase(ctx, &model.GetDatabase{
		Name:   "new_database1",
		Tenant: "tenant1",
	})
	assert.Error(t, err)

	// Get a database that does not exist in a tenant that does not exist and expect an
	// error
	_, err = c.GetDatabase(ctx, &model.GetDatabase{
		Name:   "new_database1",
		Tenant: "tenant2",
	})
	assert.Error(t, err)
}

func SampleSegments(t *testing.T, sampleCollections []*model.Collection) []*model.Segment {
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

	testTopic2 := "test_topic_2"
	testTopic3 := "test_topic_3"
	sampleSegments := []*model.Segment{
		{
			ID:           types.MustParse("00000000-d7d7-413b-92e1-731098a6e492"),
			Type:         "test_type_a",
			Topic:        nil,
			Scope:        "VECTOR",
			CollectionID: sampleCollections[0].ID,
			Metadata:     metadata1,
		},
		{
			ID:           types.MustParse("11111111-d7d7-413b-92e1-731098a6e492"),
			Type:         "test_type_b",
			Topic:        &testTopic2,
			Scope:        "VECTOR",
			CollectionID: sampleCollections[1].ID,
			Metadata:     metadata2,
		},
		{
			ID:           types.MustParse("22222222-d7d7-413b-92e1-731098a6e492"),
			Type:         "test_type_b",
			Topic:        &testTopic3,
			Scope:        "METADATA",
			CollectionID: types.NilUniqueID(),
			Metadata:     metadata3, // This segment is not assigned to any collection
		},
	}
	return sampleSegments
}

func TestCreateGetDeleteSegments(t *testing.T) {
	sampleCollections := SampleCollections(t, common.DefaultTenant, common.DefaultDatabase)

	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewMockAssignmentPolicy(sampleCollections)
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	c.ResetState(ctx)

	for _, collection := range sampleCollections {
		c.CreateCollection(ctx, &model.CreateCollection{
			ID:           collection.ID,
			Name:         collection.Name,
			Topic:        collection.Topic,
			Metadata:     collection.Metadata,
			Dimension:    collection.Dimension,
			TenantID:     collection.TenantID,
			DatabaseName: collection.DatabaseName,
		})
	}

	sampleSegments := SampleSegments(t, sampleCollections)
	for _, segment := range sampleSegments {
		c.CreateSegment(ctx, &model.CreateSegment{
			ID:           segment.ID,
			Type:         segment.Type,
			Topic:        segment.Topic,
			Scope:        segment.Scope,
			CollectionID: segment.CollectionID,
			Metadata:     segment.Metadata,
		})
	}

	results, err := c.GetSegments(ctx, types.NilUniqueID(), nil, nil, nil, types.NilUniqueID())
	sort.Slice(results, func(i, j int) bool {
		return results[i].ID.String() < results[j].ID.String()
	})
	assert.NoError(t, err)
	assert.Equal(t, sampleSegments, results)

	// Duplicate create fails
	err = c.CreateSegment(ctx, &model.CreateSegment{
		ID:           sampleSegments[0].ID,
		Type:         sampleSegments[0].Type,
		Topic:        sampleSegments[0].Topic,
		Scope:        sampleSegments[0].Scope,
		CollectionID: sampleSegments[0].CollectionID,
		Metadata:     sampleSegments[0].Metadata,
	})
	assert.Error(t, err)

	// Find by id
	for _, segment := range sampleSegments {
		result, err := c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
		assert.NoError(t, err)
		assert.Equal(t, []*model.Segment{segment}, result)
	}

	// Find by type
	testTypeA := "test_type_a"
	result, err := c.GetSegments(ctx, types.NilUniqueID(), &testTypeA, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Equal(t, sampleSegments[:1], result)

	testTypeB := "test_type_b"
	result, err = c.GetSegments(ctx, types.NilUniqueID(), &testTypeB, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.ElementsMatch(t, result, sampleSegments[1:])

	// Find by collection ID
	result, err = c.GetSegments(ctx, types.NilUniqueID(), nil, nil, nil, sampleCollections[0].ID)
	assert.NoError(t, err)
	assert.Equal(t, sampleSegments[:1], result)

	// Find by type and collection ID (positive case)
	result, err = c.GetSegments(ctx, types.NilUniqueID(), &testTypeA, nil, nil, sampleCollections[0].ID)
	assert.NoError(t, err)
	assert.Equal(t, sampleSegments[:1], result)

	// Find by type and collection ID (negative case)
	result, err = c.GetSegments(ctx, types.NilUniqueID(), &testTypeB, nil, nil, sampleCollections[0].ID)
	assert.NoError(t, err)
	assert.Empty(t, result)

	// Delete
	s1 := sampleSegments[0]
	err = c.DeleteSegment(ctx, s1.ID)
	assert.NoError(t, err)

	results, err = c.GetSegments(ctx, types.NilUniqueID(), nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.NotContains(t, results, s1)
	assert.Len(t, results, len(sampleSegments)-1)
	assert.ElementsMatch(t, results, sampleSegments[1:])

	// Duplicate delete throws an exception
	err = c.DeleteSegment(ctx, s1.ID)
	assert.Error(t, err)
}

func TestUpdateSegment(t *testing.T) {
	sampleCollections := SampleCollections(t, common.DefaultTenant, common.DefaultDatabase)

	db := dbcore.ConfigDatabaseForTesting()
	ctx := context.Background()
	assignmentPolicy := NewMockAssignmentPolicy(sampleCollections)
	c, err := NewCoordinator(ctx, assignmentPolicy, db, nil, nil)
	if err != nil {
		t.Fatalf("error creating coordinator: %v", err)
	}
	c.ResetState(ctx)

	testTopic := "test_topic_a"

	metadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	metadata.Set("test_str", &model.SegmentMetadataValueStringType{Value: "str1"})
	metadata.Set("test_int", &model.SegmentMetadataValueInt64Type{Value: 1})
	metadata.Set("test_float", &model.SegmentMetadataValueFloat64Type{Value: 1.3})

	segment := &model.Segment{
		ID:           types.UniqueID(uuid.New()),
		Type:         "test_type_a",
		Scope:        "VECTOR",
		Topic:        &testTopic,
		CollectionID: sampleCollections[0].ID,
		Metadata:     metadata,
	}

	for _, collection := range sampleCollections {
		_, err := c.CreateCollection(ctx, &model.CreateCollection{
			ID:           collection.ID,
			Name:         collection.Name,
			Topic:        collection.Topic,
			Metadata:     collection.Metadata,
			Dimension:    collection.Dimension,
			TenantID:     collection.TenantID,
			DatabaseName: collection.DatabaseName,
		})

		assert.NoError(t, err)
	}

	c.CreateSegment(ctx, &model.CreateSegment{
		ID:           segment.ID,
		Type:         segment.Type,
		Topic:        segment.Topic,
		Scope:        segment.Scope,
		CollectionID: segment.CollectionID,
		Metadata:     segment.Metadata,
	})

	// Update topic to new value
	collectionID := segment.CollectionID.String()
	newTopic := "new_topic"
	segment.Topic = &newTopic
	c.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Topic:      segment.Topic,
	})
	result, err := c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Equal(t, []*model.Segment{segment}, result)

	// Update topic to None
	segment.Topic = nil
	c.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Topic:      segment.Topic,
		ResetTopic: true,
	})
	result, err = c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Equal(t, []*model.Segment{segment}, result)

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
	c.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Metadata:   segment.Metadata})
	result, err = c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Equal(t, []*model.Segment{segment}, result)

	// Update a metadata key
	segment.Metadata.Set("test_str", &model.SegmentMetadataValueStringType{Value: "str3"})
	c.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Metadata:   segment.Metadata})
	result, err = c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Equal(t, []*model.Segment{segment}, result)

	// Delete a metadata key
	segment.Metadata.Remove("test_str")
	newMetadata := model.NewSegmentMetadata[model.SegmentMetadataValueType]()
	newMetadata.Set("test_str", nil)
	c.UpdateSegment(ctx, &model.UpdateSegment{
		Collection: &collectionID,
		ID:         segment.ID,
		Metadata:   newMetadata})
	result, err = c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Equal(t, []*model.Segment{segment}, result)

	// Delete all metadata keys
	segment.Metadata = nil
	c.UpdateSegment(ctx, &model.UpdateSegment{
		Collection:    &collectionID,
		ID:            segment.ID,
		Metadata:      segment.Metadata,
		ResetMetadata: true},
	)
	result, err = c.GetSegments(ctx, segment.ID, nil, nil, nil, types.NilUniqueID())
	assert.NoError(t, err)
	assert.Equal(t, []*model.Segment{segment}, result)
}
