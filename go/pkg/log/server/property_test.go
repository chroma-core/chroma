package server

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/log/repository"
	"github.com/chroma-core/chroma/go/pkg/proto/coordinatorpb"
	"github.com/chroma-core/chroma/go/pkg/proto/logservicepb"
	"github.com/chroma-core/chroma/go/pkg/types"
	libs2 "github.com/chroma-core/chroma/go/shared/libs"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"pgregory.net/rapid"
	"testing"
)

type ModelState struct {
	CollectionEnumerationOffset map[types.UniqueID]int64
	CollectionData              map[types.UniqueID][][]byte
	CollectionCompactionOffset  map[types.UniqueID]int64
}

type LogServerTestSuite struct {
	suite.Suite
	logServer logservicepb.LogServiceServer
	model     ModelState
	t         *testing.T
}

func (suite *LogServerTestSuite) SetupSuite() {
	ctx := context.Background()
	connectionString, err := libs2.StartPgContainer(ctx)
	assert.NoError(suite.t, err)
	assert.NoError(suite.t, err)
	var conn *pgx.Conn
	conn, err = libs2.NewPgConnection(ctx, connectionString)
	assert.NoError(suite.t, err)
	err = libs2.RunMigration(ctx, connectionString)
	assert.NoError(suite.t, err)
	lr := repository.NewLogRepository(conn)
	suite.logServer = NewLogServer(lr)
	suite.model = ModelState{
		CollectionData:             map[types.UniqueID][][]byte{},
		CollectionCompactionOffset: map[types.UniqueID]int64{},
	}
}

func (suite *LogServerTestSuite) TestRecordLogDb_PushLogs() {

	// Generate collection ids
	collections := make([]types.UniqueID, 10)
	for i := 0; i < len(collections); i++ {
		collections[i] = types.NewUniqueID()
	}

	logsGen := rapid.SliceOf(rapid.SliceOf(rapid.Byte()))

	gen := rapid.Custom(func(t *rapid.T) types.UniqueID {
		return collections[rapid.IntRange(0, len(collections)-1).Draw(t, "collectionId")]
	})

	rapid.Check(suite.t, func(t *rapid.T) {
		t.Repeat(map[string]func(*rapid.T){
			"pushLogs": func(t *rapid.T) {

				c := gen.Draw(t, "collectionPosition")
				data := logsGen.Draw(t, "logs")
				logs := make([]*coordinatorpb.OperationRecord, len(data))
				for i, record := range data {
					logs[i] = &coordinatorpb.OperationRecord{
						Vector: &coordinatorpb.Vector{
							Vector: record,
						},
					}
				}
				r, err := suite.logServer.PushLogs(context.Background(), &logservicepb.PushLogsRequest{
					CollectionId: c.String(),
					Records:      logs,
				})
				if err != nil {
					t.Fatal(err)
				}
				if int32(len(data)) != r.RecordCount {
					t.Fatal("record count mismatch", len(data), r.RecordCount)
				}
				suite.model.CollectionData[c] = append(suite.model.CollectionData[c], data...)
			},
			"getAllCollectionsToCompact": func(t *rapid.T) {
				result, err := suite.logServer.GetAllCollectionInfoToCompact(context.Background(), &logservicepb.GetAllCollectionInfoToCompactRequest{})
				assert.NoError(suite.t, err)
				for _, collection := range result.AllCollectionInfo {
					id, err := types.Parse(collection.CollectionId)
					if err != nil {
						t.Fatal(err)
					}
					newCompactationIndex := rapid.Int64Range(suite.model.CollectionCompactionOffset[id], int64(len(suite.model.CollectionData))).Draw(t, "new_position")
					_, err = suite.logServer.UpdateCollectionLogOffset(context.Background(), &logservicepb.UpdateCollectionLogOffsetRequest{
						CollectionId: id.String(),
						LogId:        newCompactationIndex,
					})
					if err != nil {
						t.Fatal(err)
					}
					suite.model.CollectionCompactionOffset[id] = newCompactationIndex
				}
			},
			"pullLogs": func(t *rapid.T) {
				c := gen.Draw(t, "collectionPosition")
				index := rapid.Int64Range(suite.model.CollectionCompactionOffset[c], int64(len(suite.model.CollectionData))).Draw(t, "id")
				response, err := suite.logServer.PullLogs(context.Background(), &logservicepb.PullLogsRequest{
					CollectionId:    c.String(),
					StartFromOffset: index,
					BatchSize:       10,
				})
				if err != nil {
					t.Fatal(err)
				}
				for _, log := range response.Records {
					expect := string(suite.model.CollectionData[c][log.LogOffset-1])
					result := string(log.Record.Vector.Vector)
					if expect != result {
						t.Fatalf("expect %s, got %s", expect, result)
					}
				}

			},
		})
	})
}

func TestLogServerTestSuite(t *testing.T) {
	testSuite := new(LogServerTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
