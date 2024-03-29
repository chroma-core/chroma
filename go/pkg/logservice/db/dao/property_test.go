package dao

import (
	dbcore2 "github.com/chroma-core/chroma/go/pkg/logservice/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"pgregory.net/rapid"
	"testing"
)

type ModelState struct {
	CollectionIndex              map[types.UniqueID]int64
	CollectionData               map[types.UniqueID][][]byte
	CollectionLastIndexCompacted map[types.UniqueID]int64
}

type CollectionPositionDbTestSuite struct {
	suite.Suite
	db                   *gorm.DB
	collectionPositionDb *collectionPositionDb
	recordLogDb          *recordLogDb
	model                ModelState
	t                    *testing.T
}

func (suite *CollectionPositionDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	db, err := dbcore2.ConfigDatabaseForTesting()
	assert.NoError(suite.t, err)
	suite.db = db
	suite.collectionPositionDb = &collectionPositionDb{
		db: suite.db,
	}
	suite.recordLogDb = &recordLogDb{
		db: suite.db,
	}
	suite.model = ModelState{
		CollectionIndex:              map[types.UniqueID]int64{},
		CollectionData:               map[types.UniqueID][][]byte{},
		CollectionLastIndexCompacted: map[types.UniqueID]int64{},
	}
}

func (suite *CollectionPositionDbTestSuite) TestRecordLogDb_PushLogs() {
	type CollectionPosition struct {
		collectionId types.UniqueID
		position     int64
	}
	// Generate candidate id
	candidates := make([]types.UniqueID, 100)
	for i := 0; i < len(candidates); i++ {
		candidates[i] = types.NewUniqueID()
	}

	logsGen := rapid.SliceOf(rapid.SliceOf(rapid.Byte()))

	gen := rapid.Custom(func(t *rapid.T) CollectionPosition {
		return CollectionPosition{
			collectionId: candidates[rapid.IntRange(0, len(candidates)-1).Draw(t, "collectionId")],
			position:     rapid.Int64Min(0).Draw(t, "position"),
		}
	})

	rapid.Check(suite.t, func(t *rapid.T) {
		t.Repeat(map[string]func(*rapid.T){
			"pushLogs": func(t *rapid.T) {
				c := gen.Draw(t, "collectionPosition")
				data := logsGen.Draw(t, "logs")
				inserted, err := suite.recordLogDb.PushLogs(c.collectionId, data)
				assert.Equal(suite.t, len(data), inserted)
				assert.NoError(suite.t, err)
				suite.model.CollectionData[c.collectionId] = append(suite.model.CollectionData[c.collectionId], data...)
			},
			"getAllCollectionsToCompact": func(t *rapid.T) {
				collections, err := suite.recordLogDb.GetAllCollectionsToCompact()
				assert.NoError(suite.t, err)
				for _, collection := range collections {
					id, err := types.Parse(*collection.CollectionID)
					assert.NoError(suite.t, err)
					newCompactationIndex := rapid.Int64Range(suite.model.CollectionLastIndexCompacted[id], int64(len(suite.model.CollectionData)+1)).Draw(t, "new_position")
					err = suite.collectionPositionDb.SetCollectionPosition(id, newCompactationIndex)
					assert.NoError(suite.t, err)
					suite.model.CollectionLastIndexCompacted[id] = newCompactationIndex
				}
			},
			"pullLogs": func(t *rapid.T) {
				c := gen.Draw(t, "collectionPosition")
				index := rapid.Int64Range(suite.model.CollectionLastIndexCompacted[c.collectionId], suite.model.CollectionIndex[c.collectionId]).Draw(t, "id")
				logs, err := suite.recordLogDb.PullLogs(c.collectionId, index, 1000)
				for i, log := range logs {
					expect := string(suite.model.CollectionData[c.collectionId][index+int64(i)])
					result := string(*log.Record)
					assert.Equal(suite.t, expect, result)
				}
				assert.NoError(suite.t, err)
			},
			"getCollectionPosition": func(t *rapid.T) {
				c := gen.Draw(t, "collectionPosition")
				var position int64
				position, err := suite.collectionPositionDb.GetCollectionPosition(c.collectionId)
				assert.NoError(suite.t, err)
				assert.Equal(suite.t, suite.model.CollectionLastIndexCompacted[c.collectionId], position)
			},
		})
	})
}

func TestCollectionPositionDbTestSuite(t *testing.T) {
	testSuite := new(CollectionPositionDbTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
