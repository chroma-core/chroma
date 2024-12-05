package grpc

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
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

type CollectionServiceTestSuite struct {
	suite.Suite
	catalog      *coordinator.Catalog
	db           *gorm.DB
	s            *Server
	tenantName   string
	databaseName string
	databaseId   string
}

func (suite *CollectionServiceTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db = dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		SystemCatalogProvider: "database",
		Testing:               true}, grpcutils.Default, suite.db)
	if err != nil {
		suite.T().Fatalf("error creating server: %v", err)
	}
	suite.s = s
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	suite.catalog = coordinator.NewTableCatalog(txnImpl, metaDomain)
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
	db := dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		SystemCatalogProvider: "memory",
		Testing:               true}, grpcutils.Default, db)
	if err != nil {
		t.Fatalf("error creating server: %v", err)
	}
	var state []*coordinatorpb.Collection
	var collectionsWithErrors []*coordinatorpb.Collection

	t.Repeat(map[string]func(*rapid.T){
		"create_collection": func(t *rapid.T) {
			stringValue := generateStringMetadataValue(t)
			intValue := generateInt64MetadataValue(t)
			floatValue := generateFloat64MetadataValue(t)
			getOrCreate := false

			createCollectionRequest := rapid.Custom[*coordinatorpb.CreateCollectionRequest](func(t *rapid.T) *coordinatorpb.CreateCollectionRequest {
				return &coordinatorpb.CreateCollectionRequest{
					Id:   rapid.StringMatching(`[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}`).Draw(t, "collection_id"),
					Name: rapid.String().Draw(t, "collection_name"),
					Metadata: &coordinatorpb.UpdateMetadata{
						Metadata: map[string]*coordinatorpb.UpdateMetadataValue{
							"string_value": stringValue,
							"int_value":    intValue,
							"float_value":  floatValue,
						},
					},
					GetOrCreate: &getOrCreate,
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

			getCollectionsRequest := coordinatorpb.GetCollectionsRequest{
				Id: &createCollectionRequest.Id,
			}
			if err == nil {
				// verify the correctness
				GetCollectionsResponse, err := s.GetCollections(ctx, &getCollectionsRequest)
				if err != nil {
					t.Fatalf("error getting collections: %v", err)
				}
				collectionList := GetCollectionsResponse.GetCollections()
				if len(collectionList) != 1 {
					t.Fatalf("More than 1 collection with the same collection id")
				}
				for _, collection := range collectionList {
					if collection.Id != createCollectionRequest.Id {
						t.Fatalf("collection id is the right value")
					}
				}
				state = append(state, res.Collection)
			}
		},
		"get_collections": func(t *rapid.T) {
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
}

func (suite *CollectionServiceTestSuite) TestServer_FlushCollectionCompaction() {
	log.Info("TestServer_FlushCollectionCompaction")
	// create test collection
	collectionName := "collection_service_test_flush_collection_compaction"
	collectionID, err := dao.CreateTestCollection(suite.db, collectionName, 128, suite.databaseId)
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

	// clean up
	err = dao.CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func TestCollectionServiceTestSuite(t *testing.T) {
	testSuite := new(CollectionServiceTestSuite)
	suite.Run(t, testSuite)
}
