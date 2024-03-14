package grpc

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/grpcutils"
	"github.com/chroma-core/chroma/go/pkg/metastore/coordinator"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dao"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"gorm.io/gorm"
	"k8s.io/apimachinery/pkg/util/rand"
	"pgregory.net/rapid"
	"strconv"
	"testing"
	"time"
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
		AssignmentPolicy:          "simple",
		SystemCatalogProvider:     "database",
		NotificationStoreProvider: "memory",
		NotifierProvider:          "memory",
		Testing:                   true}, grpcutils.Default, suite.db)
	if err != nil {
		suite.T().Fatalf("error creating server: %v", err)
	}
	suite.s = s
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	suite.catalog = coordinator.NewTableCatalogWithNotification(txnImpl, metaDomain, nil)
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
// Collection created should have the right topic
// Collection created should have the right timestamp
func testCollection(t *rapid.T) {
	db := dbcore.ConfigDatabaseForTesting()
	s, err := NewWithGrpcProvider(Config{
		AssignmentPolicy:          "simple",
		SystemCatalogProvider:     "memory",
		NotificationStoreProvider: "memory",
		NotifierProvider:          "memory",
		Testing:                   true}, grpcutils.Default, db)
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
				} else if err == common.ErrCollectionTopicEmpty {
					t.Logf("expected error for empty collection topic")
					collectionsWithErrors = append(collectionsWithErrors, res.Collection)
					// TODO: check the topic name not empty
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

func validateDatabase(suite *CollectionServiceTestSuite, collectionId string, collection *coordinatorpb.Collection, filePaths map[string][]*coordinatorpb.FilePaths) {
	getCollectionReq := coordinatorpb.GetCollectionsRequest{
		Id: &collectionId,
	}
	collectionsInDB, err := suite.s.GetCollections(context.Background(), &getCollectionReq)
	suite.NoError(err)
	suite.Len(collectionsInDB.Collections, 1)
	suite.Equal(collection.Id, collection.Id)
	suite.Equal(collection.Name, collection.Name)
	suite.Equal(collection.Topic, collection.Topic)
	suite.Equal(collection.LogPosition, collection.LogPosition)
	suite.Equal(collection.Version, collection.Version)

	getSegmentReq := coordinatorpb.GetSegmentsRequest{
		Collection: &collectionId,
	}
	segments, err := suite.s.GetSegments(context.Background(), &getSegmentReq)
	suite.NoError(err)
	for _, segment := range segments.Segments {
		suite.ElementsMatch(filePaths[segment.Id], segment.FilePaths)
	}
}

func (suite *CollectionServiceTestSuite) TestServer_FlushCollectionCompaction() {
	log.Info("TestServer_FlushCollectionCompaction")
	// create test collection
	collectionName := "collection_service_test_flush_collection_compaction"
	collectionTopic := "collection_service_test_flush_collection_compaction_topic"
	collectionID, err := dao.CreateTestCollection(suite.db, collectionName, collectionTopic, 128, suite.databaseId)
	suite.NoError(err)

	// flush collection compaction
	getSegmentReq := coordinatorpb.GetSegmentsRequest{
		Collection: &collectionID,
	}
	segments, err := suite.s.GetSegments(context.Background(), &getSegmentReq)
	suite.NoError(err)

	flushInfo := make([]*coordinatorpb.FlushSegmentCompactionInfo, 0, len(segments.Segments))
	filePaths := make(map[string][]*coordinatorpb.FilePaths)
	for _, segment := range segments.Segments {
		filePaths[segment.Id] = make([]*coordinatorpb.FilePaths, 0)
		for i := 0; i < rand.Intn(len(coordinatorpb.FilePathType_value)); i++ {
			filePathsThisSeg := make([]string, 0)
			for j := 0; j < rand.Intn(5); j++ {
				filePathsThisSeg = append(filePathsThisSeg, "test_file_path_"+strconv.Itoa(j+1))
			}
			filePathTypeI := rand.Intn(len(coordinatorpb.FilePathType_value))
			filePaths[segment.Id] = append(filePaths[segment.Id], &coordinatorpb.FilePaths{
				Type:  coordinatorpb.FilePathType(filePathTypeI),
				Paths: filePathsThisSeg,
			})
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
	filePaths[segments.Segments[0].Id] = []*coordinatorpb.FilePaths{
		{
			Type:  coordinatorpb.FilePathType(0),
			Paths: []string{"test_file_path_1"},
		},
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
		LogPosition:           150,
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
		LogPosition:           150,
		CollectionVersion:     5,
		SegmentCompactionInfo: []*coordinatorpb.FlushSegmentCompactionInfo{info},
	}
	response, err = suite.s.FlushCollectionCompaction(context.Background(), req)
	suite.Error(err)
	suite.Equal(status.Error(codes.Code(code.Code_INTERNAL), common.ErrCollectionVersionInvalid.Error()), err)
	// nothing should change in DB
	validateDatabase(suite, collectionID, collection, filePaths)

	// clean up
	err = dao.CleanUpTestCollection(suite.db, collectionID)
	suite.NoError(err)
}

func TestCollectionServiceTestSuite(t *testing.T) {
	testSuite := new(CollectionServiceTestSuite)
	suite.Run(t, testSuite)
}
