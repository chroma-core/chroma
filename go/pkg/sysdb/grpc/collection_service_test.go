package grpc

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao/daotest"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	s3metastore "github.com/chroma-core/chroma/go/pkg/sysdb/metastore/s3"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/util/rand"
	"pgregory.net/rapid"
)

// TODO(eculver): replace most suite.NoError(err) with suite.Require().NoError(err) so the test
// stops running when the error is not nil instead of continuing and causing red herrings in test output

// TODO(eculver): replace calls to dao.NewDefaultTestCollection with daotest.NewTestCollection

type CollectionServiceTestSuite struct {
	suite.Suite
	catalog      *coordinator.Catalog
	db           *gorm.DB
	read_db      *gorm.DB
	s            *Server
	tenantName   string
	databaseName string
	databaseId   string
}

func (suite *CollectionServiceTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db, suite.read_db = dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		SystemCatalogProvider: "database",
		Testing:               true,
		MetaStoreConfig: s3metastore.S3MetaStoreConfig{
			BucketName:              "test-bucket",
			Region:                  "us-east-1",
			Endpoint:                "http://localhost:9000",
			AccessKeyID:             "minio",
			SecretAccessKey:         "minio123",
			ForcePathStyle:          true,
			CreateBucketIfNotExists: true,
		},
	}, grpcutils.Default)
	if err != nil {
		suite.T().Fatalf("error creating server: %v", err)
	}
	suite.s = s
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	suite.catalog = coordinator.NewTableCatalog(txnImpl, metaDomain, nil, false)
	suite.tenantName = "tenant_" + suite.T().Name()
	suite.databaseName = "database_" + suite.T().Name()
	DbId, err := dao.CreateTestTenantAndDatabase(suite.db, suite.tenantName, suite.databaseName)
	suite.NoError(err)
	suite.databaseId = DbId
}

func (suite *CollectionServiceTestSuite) TearDownSuite() {
	log.Info("teardown suite")
	err := dao.CleanUpTestDatabase(suite.db, suite.tenantName, suite.databaseName)
	suite.NoError(err)
	err = dao.CleanUpTestTenant(suite.db, suite.tenantName)
	suite.NoError(err)
}

// CreateCollection
// Collection created successfully are visible to ListCollections
// Collection created should have the right metadata, the metadata should be a flat map, with keys as strings and values as strings, ints, or floats
// Collection created should have the right name
// Collection created should have the right ID
// Collection created should have the right timestamp
func testCollection(t *rapid.T) {
	dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		SystemCatalogProvider: "memory",
		Testing:               true}, grpcutils.Default)
	if err != nil {
		t.Fatalf("error creating server: %v", err)
	}
	var state []*coordinatorpb.Collection
	var collectionsWithErrors []*coordinatorpb.Collection

	t.Repeat(map[string]func(*rapid.T){
		"create_get_collection": func(t *rapid.T) {
			stringValue := generateStringMetadataValue(t)
			intValue := generateInt64MetadataValue(t)
			floatValue := generateFloat64MetadataValue(t)
			getOrCreate := false

			collectionId := rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "collection_id")
			collectionName := rapid.String().Draw(t, "collection_name")

			createCollectionRequest := rapid.Custom[*coordinatorpb.CreateCollectionRequest](func(t *rapid.T) *coordinatorpb.CreateCollectionRequest {
				return &coordinatorpb.CreateCollectionRequest{
					Id:   collectionId,
					Name: collectionName,
					Metadata: &coordinatorpb.UpdateMetadata{
						Metadata: map[string]*coordinatorpb.UpdateMetadataValue{
							"string_value": stringValue,
							"int_value":    intValue,
							"float_value":  floatValue,
						},
					},
					GetOrCreate: &getOrCreate,
					Segments: []*coordinatorpb.Segment{
						{
							Id:         rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "metadata_segment_id"),
							Type:       "metadata_segment_type",
							Scope:      coordinatorpb.SegmentScope_METADATA,
							Collection: collectionId,
						},
						{
							Id:         rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "record_segment_id"),
							Type:       "record_segment_type",
							Scope:      coordinatorpb.SegmentScope_RECORD,
							Collection: collectionId,
						},
						{
							Id:         rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "vector_segment_id"),
							Type:       "vector_segment_type",
							Scope:      coordinatorpb.SegmentScope_VECTOR,
							Collection: collectionId,
						},
					},
				}
			}).Draw(t, "create_collection_request")

			ctx := context.Background()
			res, err := s.CreateCollection(ctx, createCollectionRequest)
			if err != nil {
				if err == common.ErrCollectionNameEmpty && createCollectionRequest.Name == "" {
					t.Logf("expected error for empty collection name")
					collectionsWithErrors = append(collectionsWithErrors, res.Collection)
				} else {
					t.Fatalf("error creating collection: %v", err)
					collectionsWithErrors = append(collectionsWithErrors, res.Collection)
				}
			}

			if err == nil {
				getCollectionsRequest := coordinatorpb.GetCollectionsRequest{
					Id: &createCollectionRequest.Id,
				}
				// verify the correctness
				getCollectionsResponse, err := s.GetCollections(ctx, &getCollectionsRequest)
				if err != nil {
					t.Fatalf("error getting collections: %v", err)
				}
				collectionList := getCollectionsResponse.GetCollections()
				if len(collectionList) != 1 {
					t.Fatalf("there should be exactly one matching collection given the collection id")
				}
				if collectionList[0].Id != createCollectionRequest.Id {
					t.Fatalf("collection id mismatch")
				}

				getCollectionWithSegmentsRequest := coordinatorpb.GetCollectionWithSegmentsRequest{
					Id: createCollectionRequest.Id,
				}

				getCollectionWithSegmentsResponse, err := s.GetCollectionWithSegments(ctx, &getCollectionWithSegmentsRequest)
				if err != nil {
					t.Fatalf("error getting collection with segments: %v", err)
				}

				if getCollectionWithSegmentsResponse.Collection.Id != res.Collection.Id {
					t.Fatalf("collection id mismatch")
				}

				if len(getCollectionWithSegmentsResponse.Segments) != 3 {
					t.Fatalf("unexpected number of segments in collection: %v", getCollectionWithSegmentsResponse.Segments)
				}

				scopeToSegmentMap := map[coordinatorpb.SegmentScope]*coordinatorpb.Segment{}
				for _, segment := range getCollectionWithSegmentsResponse.Segments {
					if segment.Collection != res.Collection.Id {
						t.Fatalf("invalid collection id in segment")
					}
					scopeToSegmentMap[segment.GetScope()] = segment
				}
				scopes := []coordinatorpb.SegmentScope{coordinatorpb.SegmentScope_METADATA, coordinatorpb.SegmentScope_RECORD, coordinatorpb.SegmentScope_VECTOR}
				for _, scope := range scopes {
					if _, exists := scopeToSegmentMap[scope]; !exists {
						t.Fatalf("collection segment scope not found: %s", scope.String())
					}
				}

				state = append(state, res.Collection)
			}
		},
	})
}

func generateStringMetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_StringValue{
			StringValue: rapid.String().Draw(t, "string_value"),
		},
	}
}

func generateInt64MetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_IntValue{
			IntValue: rapid.Int64().Draw(t, "int_value"),
		},
	}
}

func generateFloat64MetadataValue(t *rapid.T) *coordinatorpb.UpdateMetadataValue {
	return &coordinatorpb.UpdateMetadataValue{
		Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
			FloatValue: rapid.Float64().Draw(t, "float_value"),
		},
	}
}

func TestCollection(t *testing.T) {
	// rapid.Check(t, testCollection)
}

func validateDatabase(suite *CollectionServiceTestSuite, collectionId string, collection *coordinatorpb.Collection, filePaths map[string]map[string]*coordinatorpb.FilePaths) {
	getCollectionReq := coordinatorpb.GetCollectionsRequest{
		Id: &collectionId,
	}
	collectionsInDB, err := suite.s.GetCollections(context.Background(), &getCollectionReq)
	suite.NoError(err)
	suite.Len(collectionsInDB.Collections, 1)
	suite.Equal(collection.Id, collection.Id)
	suite.Equal(collection.Name, collection.Name)
	suite.Equal(collection.LogPosition, collection.LogPosition)
	suite.Equal(collection.Version, collection.Version)

	getSegmentReq := coordinatorpb.GetSegmentsRequest{
		Collection: collectionId,
	}
	segments, err := suite.s.GetSegments(context.Background(), &getSegmentReq)
	suite.NoError(err)
	for _, segment := range segments.Segments {
		for key, value := range filePaths[segment.Id] {
			suite.True(proto.Equal(value, segment.FilePaths[key]))
		}
		for key, value := range segment.FilePaths {
			suite.True(proto.Equal(value, filePaths[segment.Id][key]))
		}
	}
}

func (suite *CollectionServiceTestSuite) TestCreateCollection() {
	// Create a collection request
	collectionName := "test_create_collection"
	collectionID := types.UniqueID(uuid.New())
	getOrCreate := false

	segments := []*coordinatorpb.Segment{
		{
			Id:         types.UniqueID(uuid.New()).String(),
			Collection: collectionID.String(),
			Type:       "test_type_a",
		},
	}
	req := &coordinatorpb.CreateCollectionRequest{
		Id:       collectionID.String(),
		Name:     collectionName,
		Database: suite.databaseName,
		Tenant:   suite.tenantName,
		Metadata: &coordinatorpb.UpdateMetadata{
			Metadata: map[string]*coordinatorpb.UpdateMetadataValue{
				"string_key": {
					Value: &coordinatorpb.UpdateMetadataValue_StringValue{
						StringValue: "test_value",
					},
				},
				"int_key": {
					Value: &coordinatorpb.UpdateMetadataValue_IntValue{
						IntValue: 42,
					},
				},
				"float_key": {
					Value: &coordinatorpb.UpdateMetadataValue_FloatValue{
						FloatValue: 3.14,
					},
				},
			},
		},
		GetOrCreate: &getOrCreate,
		Segments:    segments,
	}

	// Create the collection
	resp, err := suite.s.CreateCollection(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.Equal(collectionID.String(), resp.Collection.Id)
	suite.Equal(collectionName, resp.Collection.Name)

	// Verify the collection exists by getting it
	collectionIDStr := collectionID.String()
	getReq := &coordinatorpb.GetCollectionsRequest{
		Id: &collectionIDStr,
	}
	getResp, err := suite.s.GetCollections(context.Background(), getReq)
	suite.NoError(err)
	suite.Len(getResp.Collections, 1)
	suite.Equal(collectionID.String(), getResp.Collections[0].Id)
	suite.Equal(collectionName, getResp.Collections[0].Name)

	// Verify the segments exist
	getSegmentsResp, err := suite.s.GetSegments(context.Background(), &coordinatorpb.GetSegmentsRequest{
		Collection: collectionID.String(),
	})
	suite.NoError(err)
	suite.Len(getSegmentsResp.Segments, 1)
	suite.Equal(segments[0].Id, getSegmentsResp.Segments[0].Id)
	suite.Equal(segments[0].Collection, getSegmentsResp.Segments[0].Collection)
	suite.Equal(segments[0].Type, getSegmentsResp.Segments[0].Type)

	// Clean up
	err = dao.CleanUpTestCollection(suite.db, collectionID.String())
	suite.NoError(err)

	// Create a collection on a database that does not exist.
	_, err = suite.s.CreateCollection(context.Background(), &coordinatorpb.CreateCollectionRequest{
		Id:       types.UniqueID(uuid.New()).String(),
		Name:     "test_collection",
		Database: "non_existent_database",
		Tenant:   suite.tenantName,
	})
	suite.Error(err)
	// Check that err is NOT_FOUND
	suite.Equal(status.Error(codes.Code(code.Code_NOT_FOUND), common.ErrDatabaseNotFound.Error()), err)
}

func (suite *CollectionServiceTestSuite) TestServer_GetCollection() {
	// Create a test collection with a name that should not already exist in the database
	collectionName := "test_get_collection"
	collectionID, err := dao.CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, 128, suite.databaseId, nil))
	suite.NoError(err)

	// Soft delete the collection
	err = suite.s.coordinator.SoftDeleteCollection(context.Background(), &model.DeleteCollection{
		ID:           types.MustParse(collectionID),
		DatabaseName: suite.databaseName,
		TenantID:     suite.tenantName,
	})
	suite.NoError(err)

	// Try to get the soft-deleted collection
	collectionIDStr := collectionID
	getReq := &coordinatorpb.GetCollectionRequest{
		Id:       collectionIDStr,
		Database: &suite.databaseName,
		Tenant:   &suite.tenantName,
	}
	_, err = suite.s.GetCollection(context.Background(), getReq)
	suite.Error(err)
	suite.Equal(status.Error(codes.FailedPrecondition, common.ErrCollectionSoftDeleted.Error()), err)

	// Clean up
	err = dao.CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func (suite *CollectionServiceTestSuite) TestServer_GetCollectionByResourceName() {
	tenantResourceName := "test_tenant_resource_name"
	// Does this need to match the daotest.TestTenantID?
	tenantID := "test_tenant_id"
	databaseName := "test_database"
	collectionName := "test_collection"
	dim := int32(128)

	databaseID, err := dao.CreateTestTenantAndDatabase(suite.db, tenantID, databaseName)
	suite.NoError(err)

	err = dao.SetTestTenantResourceName(suite.db, tenantID, tenantResourceName)
	suite.NoError(err)

	collectionID, err := dao.CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, dim, databaseID, nil))
	suite.NoError(err)

	req := &coordinatorpb.GetCollectionByResourceNameRequest{
		TenantResourceName: tenantResourceName,
		Database:           databaseName,
		Name:               collectionName,
	}
	resp, err := suite.s.GetCollectionByResourceName(context.Background(), req)
	suite.NoError(err)
	suite.NotNil(resp)
	suite.NotNil(resp.Collection)
	suite.Equal(collectionID, resp.Collection.Id)
	suite.Equal(collectionName, resp.Collection.Name)
	suite.Equal(tenantID, resp.Collection.Tenant)
	suite.Equal(databaseName, resp.Collection.Database)

	nonExistentCollectionName := "non_existent_collection"
	req = &coordinatorpb.GetCollectionByResourceNameRequest{
		TenantResourceName: tenantResourceName,
		Database:           databaseName,
		Name:               nonExistentCollectionName,
	}
	resp, err = suite.s.GetCollectionByResourceName(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp.Collection)

	nonExistentDatabaseName := "non_existent_database"
	req = &coordinatorpb.GetCollectionByResourceNameRequest{
		TenantResourceName: tenantResourceName,
		Database:           nonExistentDatabaseName,
		Name:               collectionName,
	}
	resp, err = suite.s.GetCollectionByResourceName(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp.Collection)

	nonExistentTenantResourceName := "non_existent_resource_name"
	req = &coordinatorpb.GetCollectionByResourceNameRequest{
		TenantResourceName: nonExistentTenantResourceName,
		Database:           databaseName,
		Name:               collectionName,
	}
	resp, err = suite.s.GetCollectionByResourceName(context.Background(), req)
	suite.Error(err)
	suite.Nil(resp.Collection)

	err = dao.CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
	err = dao.CleanUpTestDatabase(suite.db, tenantID, databaseName)
	suite.NoError(err)
	err = dao.CleanUpTestTenant(suite.db, tenantID)
	suite.NoError(err)
}

func (suite *CollectionServiceTestSuite) TestServer_FlushCollectionCompaction() {
	log.Info("TestServer_FlushCollectionCompaction")
	// create test collection
	collectionName := "collection_service_test_flush_collection_compaction"
	collectionID, err := dao.CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, 128, suite.databaseId, nil))
	suite.NoError(err)

	// flush collection compaction
	getSegmentReq := coordinatorpb.GetSegmentsRequest{
		Collection: collectionID,
	}
	segments, err := suite.s.GetSegments(context.Background(), &getSegmentReq)
	suite.NoError(err)

	flushInfo := make([]*coordinatorpb.FlushSegmentCompactionInfo, 0, len(segments.Segments))
	filePaths := make(map[string]map[string]*coordinatorpb.FilePaths, 0)
	testFilePathTypes := []string{"TypeA", "TypeB", "TypeC", "TypeD"}
	for _, segment := range segments.Segments {
		filePaths[segment.Id] = make(map[string]*coordinatorpb.FilePaths, 0)
		for i := 0; i < rand.Intn(len(testFilePathTypes)); i++ {
			filePathsThisSeg := make([]string, 0)
			for j := 0; j < rand.Intn(5); j++ {
				filePathsThisSeg = append(filePathsThisSeg, "test_file_path_"+strconv.Itoa(j+1))
			}
			filePathTypeI := rand.Intn(len(testFilePathTypes))
			filePaths[segment.Id][testFilePathTypes[filePathTypeI]] = &coordinatorpb.FilePaths{
				Paths: filePathsThisSeg,
			}
		}
		info := &coordinatorpb.FlushSegmentCompactionInfo{
			SegmentId: segment.Id,
			FilePaths: filePaths[segment.Id],
		}
		flushInfo = append(flushInfo, info)
	}

	req := &coordinatorpb.FlushCollectionCompactionRequest{
		TenantId:              suite.tenantName,
		CollectionId:          collectionID,
		LogPosition:           10,
		CollectionVersion:     0,
		SegmentCompactionInfo: flushInfo,
	}
	response, err := suite.s.FlushCollectionCompaction(context.Background(), req)
	t1 := time.Now().Unix()
	suite.NoError(err)
	suite.Equal(collectionID, response.CollectionId)
	suite.Equal(int32(1), response.CollectionVersion)
	suite.Less(int64(0), response.LastCompactionTime)
	suite.LessOrEqual(response.LastCompactionTime, t1)

	// validate database
	collection := &coordinatorpb.Collection{
		Id:          collectionID,
		LogPosition: int64(10),
		Version:     int32(1),
	}
	validateDatabase(suite, collectionID, collection, filePaths)

	// flush one segment
	filePaths[segments.Segments[0].Id][testFilePathTypes[0]] = &coordinatorpb.FilePaths{
		Paths: []string{"test_file_path_1"},
	}
	info := &coordinatorpb.FlushSegmentCompactionInfo{
		SegmentId: segments.Segments[0].Id,
		FilePaths: filePaths[segments.Segments[0].Id],
	}
	req = &coordinatorpb.FlushCollectionCompactionRequest{
		TenantId:              suite.tenantName,
		CollectionId:          collectionID,
		LogPosition:           100,
		CollectionVersion:     1,
		SegmentCompactionInfo: []*coordinatorpb.FlushSegmentCompactionInfo{info},
	}
	response, err = suite.s.FlushCollectionCompaction(context.Background(), req)
	t2 := time.Now().Unix()
	suite.NoError(err)
	suite.Equal(collectionID, response.CollectionId)
	suite.Equal(int32(2), response.CollectionVersion)
	suite.LessOrEqual(t1, response.LastCompactionTime)
	suite.LessOrEqual(response.LastCompactionTime, t2)

	// validate database
	collection = &coordinatorpb.Collection{
		Id:          collectionID,
		LogPosition: int64(100),
		Version:     int32(2),
	}
	validateDatabase(suite, collectionID, collection, filePaths)

	// test invalid log position
	req = &coordinatorpb.FlushCollectionCompactionRequest{
		TenantId:              suite.tenantName,
		CollectionId:          collectionID,
		LogPosition:           50,
		CollectionVersion:     2,
		SegmentCompactionInfo: []*coordinatorpb.FlushSegmentCompactionInfo{info},
	}
	response, err = suite.s.FlushCollectionCompaction(context.Background(), req)
	suite.Error(err)
	suite.Equal(status.Error(codes.Code(code.Code_INTERNAL), common.ErrCollectionLogPositionStale.Error()), err)
	// nothing should change in DB
	validateDatabase(suite, collectionID, collection, filePaths)

	// test invalid version
	req = &coordinatorpb.FlushCollectionCompactionRequest{
		TenantId:              suite.tenantName,
		CollectionId:          collectionID,
		LogPosition:           100,
		CollectionVersion:     1,
		SegmentCompactionInfo: []*coordinatorpb.FlushSegmentCompactionInfo{info},
	}
	response, err = suite.s.FlushCollectionCompaction(context.Background(), req)
	suite.Error(err)
	suite.Equal(status.Error(codes.Code(code.Code_INTERNAL), common.ErrCollectionVersionStale.Error()), err)
	// nothing should change in DB
	validateDatabase(suite, collectionID, collection, filePaths)

	req = &coordinatorpb.FlushCollectionCompactionRequest{
		TenantId:              suite.tenantName,
		CollectionId:          collectionID,
		LogPosition:           100,
		CollectionVersion:     5,
		SegmentCompactionInfo: []*coordinatorpb.FlushSegmentCompactionInfo{info},
	}
	response, err = suite.s.FlushCollectionCompaction(context.Background(), req)
	suite.Error(err)
	suite.Equal(status.Error(codes.Code(code.Code_INTERNAL), common.ErrCollectionVersionInvalid.Error()), err)
	// nothing should change in DB
	validateDatabase(suite, collectionID, collection, filePaths)

	// test empty segment compaction info
	// this happens when the compaction results in no delta for the collection
	req = &coordinatorpb.FlushCollectionCompactionRequest{
		TenantId:              suite.tenantName,
		CollectionId:          collectionID,
		LogPosition:           200,
		CollectionVersion:     2,
		SegmentCompactionInfo: []*coordinatorpb.FlushSegmentCompactionInfo{},
	}
	response, err = suite.s.FlushCollectionCompaction(context.Background(), req)
	suite.NoError(err)
	// log position and collection version should be updated
	collection = &coordinatorpb.Collection{
		Id:          collectionID,
		LogPosition: int64(200),
		Version:     int32(3),
	}
	// nothing else should change in DB
	validateDatabase(suite, collectionID, collection, filePaths)

	// Send FlushCollectionCompaction for a collection that is soft deleted.
	// It should fail with a failed precondition error.
	// Create collection and soft-delete it.
	collectionID, err = dao.CreateTestCollection(suite.db, daotest.NewDefaultTestCollection("test_flush_collection_compaction_soft_delete", 128, suite.databaseId, nil))
	suite.NoError(err)
	suite.s.coordinator.SoftDeleteCollection(context.Background(), &model.DeleteCollection{
		ID:           types.MustParse(collectionID),
		DatabaseName: suite.databaseName,
		TenantID:     suite.tenantName,
	})
	// Send FlushCollectionCompaction for the soft-deleted collection.
	// It should fail with a failed precondition error.
	req = &coordinatorpb.FlushCollectionCompactionRequest{
		TenantId:          suite.tenantName,
		CollectionId:      collectionID,
		LogPosition:       100,
		CollectionVersion: 1,
	}
	_, err = suite.s.FlushCollectionCompaction(context.Background(), req)
	suite.Error(err)
	suite.Equal(status.Error(codes.Code(code.Code_FAILED_PRECONDITION), common.ErrCollectionSoftDeleted.Error()), err)

	// clean up
	err = dao.CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func (suite *CollectionServiceTestSuite) TestServer_CheckCollections() {
	collectionName := "test_check_collections"
	collectionID, err := dao.CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, 128, suite.databaseId, nil))
	suite.NoError(err)

	request := &coordinatorpb.CheckCollectionsRequest{
		CollectionIds: []string{collectionID},
	}

	// Call the service method
	response, err := suite.s.CheckCollections(context.Background(), request)
	suite.NoError(err)

	suite.NotNil(response.GetDeleted(), "Deleted slice should not be nil.")
	suite.Len(response.GetDeleted(), 1)
	suite.False(response.GetDeleted()[0])

	suite.NotNil(response.GetLogPosition(), "LogPosition slice should not be nil.")
	suite.Len(response.GetLogPosition(), 1)
	suite.GreaterOrEqual(response.GetLogPosition()[0], int64(0))

	// clean up
	err = dao.CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func (suite *CollectionServiceTestSuite) TestGetCollectionSize() {
	collectionName := "collection_service_test_get_collection_size"
	collectionID, err := dao.CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, 128, suite.databaseId, nil))
	suite.NoError(err)

	req := coordinatorpb.GetCollectionSizeRequest{
		Id: collectionID,
	}
	res, err := suite.s.GetCollectionSize(context.Background(), &req)
	suite.NoError(err)
	suite.Equal(uint64(100), res.TotalRecordsPostCompaction)

	err = dao.CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func (suite *CollectionServiceTestSuite) TestCountForks() {
	collectionName := "collection_service_test_count_forks"
	collectionID, err := dao.CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, 128, suite.databaseId, nil))
	suite.NoError(err)

	req := coordinatorpb.CountForksRequest{
		SourceCollectionId: collectionID,
	}
	res, err := suite.s.CountForks(context.Background(), &req)
	suite.NoError(err)
	suite.Equal(uint64(0), res.Count)

	var forkedCollectionIDs []string

	// Create 5 forks
	for i := 0; i < 5; i++ {
		forkCollectionReq := &coordinatorpb.ForkCollectionRequest{
			SourceCollectionId:                   collectionID,
			SourceCollectionLogCompactionOffset:  0,
			SourceCollectionLogEnumerationOffset: 0,
			TargetCollectionId:                   types.NewUniqueID().String(),
			TargetCollectionName:                 fmt.Sprintf("test_fork_collection_fork_%d", i),
		}
		forkedCollection, err := suite.s.ForkCollection(context.Background(), forkCollectionReq)
		suite.NoError(err)
		forkedCollectionIDs = append(forkedCollectionIDs, forkedCollection.Collection.Id)
	}

	res, err = suite.s.CountForks(context.Background(), &req)
	suite.NoError(err)
	suite.Equal(uint64(5), res.Count)

	// Check that each forked collection has 5 forks as well
	for _, forkedCollectionID := range forkedCollectionIDs {
		res, err = suite.s.CountForks(context.Background(), &coordinatorpb.CountForksRequest{
			SourceCollectionId: forkedCollectionID,
		})
		suite.NoError(err)
		suite.Equal(uint64(5), res.Count)
	}

	err = dao.CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func (suite *CollectionServiceTestSuite) TestFork() {
	collectionName := "collection_service_test_forks"
	collectionID, err := dao.CreateTestCollection(suite.db, daotest.NewDefaultTestCollection(collectionName, 128, suite.databaseId, nil))
	suite.NoError(err)
	targetCollectionID := types.NewUniqueID()

	req := coordinatorpb.ForkCollectionRequest{
		SourceCollectionId:                   collectionID,
		SourceCollectionLogEnumerationOffset: 0,
		SourceCollectionLogCompactionOffset:  0,
		TargetCollectionId:                   targetCollectionID.String(),
		TargetCollectionName:                 "test_fork_collection",
	}
	res, err := suite.s.ForkCollection(context.Background(), &req)
	suite.NoError(err)
	suite.Equal(res.Collection.Id, targetCollectionID.String())
	suite.Equal(len(res.Segments), 2)

	fork2CollectionId := types.NewUniqueID()
	// Create fork of fork
	forkCollectionReq := &coordinatorpb.ForkCollectionRequest{
		SourceCollectionId:                   targetCollectionID.String(),
		SourceCollectionLogCompactionOffset:  0,
		SourceCollectionLogEnumerationOffset: 0,
		TargetCollectionId:                   fork2CollectionId.String(),
		TargetCollectionName:                 "test_fork_collection_fork",
	}
	forkedCollection2, err := suite.s.ForkCollection(context.Background(), forkCollectionReq)
	suite.NoError(err)
	suite.Equal(forkedCollection2.Collection.Id, fork2CollectionId.String())
	suite.Equal(len(forkedCollection2.Segments), 2)

	// Delete the root.
	deleteReq := model.DeleteCollection{
		ID:           types.MustParse(collectionID),
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
		Ts:           time.Now().Unix(),
	}
	err = suite.s.coordinator.SoftDeleteCollection(context.Background(), &deleteReq)
	suite.NoError(err)

	// Fork should still succeed.
	fork3CollectionId := types.NewUniqueID()
	fork3CollectionReq := &coordinatorpb.ForkCollectionRequest{
		SourceCollectionId:                   fork2CollectionId.String(),
		SourceCollectionLogCompactionOffset:  0,
		SourceCollectionLogEnumerationOffset: 0,
		TargetCollectionId:                   fork3CollectionId.String(),
		TargetCollectionName:                 "test_fork_collection_fork_fork",
	}
	forkedCollection3, err := suite.s.ForkCollection(context.Background(), fork3CollectionReq)
	suite.NoError(err)
	suite.Equal(forkedCollection3.Collection.Id, fork3CollectionId.String())
	suite.Equal(len(forkedCollection2.Segments), 2)

	// Deleting the source and fork should not succeed.
	deleteReq2 := model.DeleteCollection{
		ID:           fork3CollectionId,
		TenantID:     suite.tenantName,
		DatabaseName: suite.databaseName,
		Ts:           time.Now().Unix(),
	}
	err = suite.s.coordinator.SoftDeleteCollection(context.Background(), &deleteReq2)
	suite.NoError(err)

	// Fork should not succeed.
	fork4CollectionId := types.NewUniqueID()
	fork4CollectionReq := &coordinatorpb.ForkCollectionRequest{
		SourceCollectionId:                   fork3CollectionId.String(),
		SourceCollectionLogCompactionOffset:  0,
		SourceCollectionLogEnumerationOffset: 0,
		TargetCollectionId:                   fork4CollectionId.String(),
		TargetCollectionName:                 "test_fork_collection_fork_fork_fork",
	}
	_, err = suite.s.ForkCollection(context.Background(), fork4CollectionReq)
	suite.Error(err)
}

func TestCollectionServiceTestSuite(t *testing.T) {
	testSuite := new(CollectionServiceTestSuite)
	suite.Run(t, testSuite)
}
