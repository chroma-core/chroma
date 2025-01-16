package coordinator

import (
	"context"
	"sort"

	"github.com/chroma-core/chroma/go/pkg/sysdb/coordinator/model"
	"github.com/chroma-core/chroma/go/pkg/types"
)

func (suite *APIsTestSuite) TestGetCollectionsRead() {
	ctx := context.Background()
	results, err := suite.readCoordinator.GetCollectionsRead(ctx, types.NilUniqueID(), nil, suite.tenantName, suite.databaseName)
	suite.NoError(err)

	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})
	suite.Equal(suite.sampleCollections, results)

	// Find by name
	for _, collection := range suite.sampleCollections {
		result, err := suite.readCoordinator.GetCollectionsRead(ctx, types.NilUniqueID(), &collection.Name, suite.tenantName, suite.databaseName)
		suite.NoError(err)
		suite.Equal([]*model.Collection{collection}, result)
	}

	// Find by id
	for _, collection := range suite.sampleCollections {
		result, err := suite.readCoordinator.GetCollectionsRead(ctx, collection.ID, nil, suite.tenantName, suite.databaseName)
		suite.NoError(err)
		suite.Equal([]*model.Collection{collection}, result)
	}
}
