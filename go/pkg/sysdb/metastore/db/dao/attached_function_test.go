package dao

import (
	"testing"

	"github.com/chroma-core/chroma/go/pkg/common"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/google/uuid"
	"github.com/pingcap/log"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
)

type AttachedFunctionDbTestSuite struct {
	suite.Suite
	db *gorm.DB
	Db *attachedFunctionDb
	t  *testing.T
}

func (suite *AttachedFunctionDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db, _ = dbcore.ConfigDatabaseForTesting()
	suite.Db = &attachedFunctionDb{
		db: suite.db,
	}

	// Seed functions for tests - these must match dbmodel/constants.go
	// This also serves as a validation that constants are correct
	functions := []dbmodel.Function{
		{
			ID:            dbmodel.FunctionRecordCounter,
			Name:          dbmodel.FunctionNameRecordCounter,
			IsIncremental: dbmodel.FunctionRecordCounterIsIncremental,
			ReturnType:    dbmodel.FunctionRecordCounterReturnType,
		},
	}
	for _, fn := range functions {
		suite.db.Where(dbmodel.Function{ID: fn.ID}).FirstOrCreate(&fn)
	}
}

func (suite *AttachedFunctionDbTestSuite) SetupTest() {
	log.Info("setup test")
}

func (suite *AttachedFunctionDbTestSuite) TearDownTest() {
	log.Info("teardown test")
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_Insert() {
	attachedFunctionID := uuid.New()
	functionID := dbmodel.FunctionRecordCounter

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID,
		Name:                    "test-insert-attachedFunction",
		FunctionID:              functionID,
		InputCollectionID:       "input_col_id",
		OutputCollectionName:    "output_col_name",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 true,
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	// Verify attached function was inserted
	var retrieved dbmodel.AttachedFunction
	err = suite.db.Where("name = ? AND tenant_id = ? AND database_id = ?", "test-insert-attachedFunction", "tenant1", "db1").First(&retrieved).Error
	suite.Require().NoError(err)
	suite.Require().Equal(attachedFunction.Name, retrieved.Name)
	suite.Require().Equal(attachedFunction.FunctionID, retrieved.FunctionID)
	suite.Require().False(retrieved.IsDeleted)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_Insert_DuplicateName() {
	attachedFunctionID1 := uuid.New()
	functionID1 := dbmodel.FunctionRecordCounter

	attachedFunction1 := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID1,
		Name:                    "test-attachedFunction-1",
		FunctionID:              functionID1,
		InputCollectionID:       "input1",
		OutputCollectionName:    "output1",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 true,
	}

	err := suite.Db.Insert(attachedFunction1)
	suite.Require().NoError(err)

	// Try to insert duplicate (same tenant, database, and name)
	attachedFunctionID2 := uuid.New()
	functionID2 := dbmodel.FunctionRecordCounter

	attachedFunction2 := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID2,
		Name:                    "test-attachedFunction-1", // Same name as attachedFunction1
		FunctionID:              functionID2,
		InputCollectionID:       "input1",
		OutputCollectionName:    "output1",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 true,
	}

	err = suite.Db.Insert(attachedFunction2)
	suite.Require().Error(err)
	suite.Require().Equal(common.ErrAttachedFunctionAlreadyExists, err)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction1.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByName() {
	attachedFunctionID := uuid.New()
	functionID := dbmodel.FunctionRecordCounter

	// Insert an attached function
	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID,
		Name:                    "test-get-attachedFunction",
		FunctionID:              functionID,
		InputCollectionID:       "input_col_id",
		OutputCollectionName:    "output_col_name",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 true,
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	// Retrieve by name
	name := "test-get-attachedFunction"
	inputColID := "input_col_id"
	results, err := suite.Db.GetAttachedFunctions(nil, &name, &inputColID, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 1)
	suite.Require().Equal(attachedFunction.ID, results[0].ID)
	suite.Require().Equal(attachedFunction.Name, results[0].Name)
	suite.Require().Equal(attachedFunction.FunctionID, results[0].FunctionID)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByName_NotReady() {
	attachedFunctionID := uuid.New()
	functionID := dbmodel.FunctionRecordCounter

	// Insert an attached function
	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID,
		Name:                    "test-get-attachedFunction",
		FunctionID:              functionID,
		InputCollectionID:       "input_col_id",
		OutputCollectionName:    "output_col_name",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 false,
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	// Retrieve by name
	name := "test-get-attachedFunction"
	inputColID := "input_col_id"
	results, err := suite.Db.GetAttachedFunctions(nil, &name, &inputColID, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 0)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByName_NotFound() {
	// Try to get non-existent attached function
	name := "nonexistent-attachedFunction"
	inputColID := "input_col_id"
	results, err := suite.Db.GetAttachedFunctions(nil, &name, &inputColID, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 0)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByName_IgnoresDeleted() {
	attachedFunctionID := uuid.New()
	functionID := dbmodel.FunctionRecordCounter

	// Insert an attached function
	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID,
		Name:                    "test-deleted-attachedFunction",
		FunctionID:              functionID,
		InputCollectionID:       "input1",
		OutputCollectionName:    "output1",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 true,
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	// Soft delete it
	err = suite.Db.SoftDelete("input1", "test-deleted-attachedFunction")
	suite.Require().NoError(err)

	// GetAttachedFunctions should not return it
	name := "test-deleted-attachedFunction"
	inputColID := "input1"
	results, err := suite.Db.GetAttachedFunctions(nil, &name, &inputColID, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 0)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_SoftDelete() {
	attachedFunctionID := uuid.New()
	functionID := dbmodel.FunctionRecordCounter

	// Insert an attached function
	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID,
		Name:                    "test-soft-delete",
		FunctionID:              functionID,
		InputCollectionID:       "input1",
		OutputCollectionName:    "output1",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 true,
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	// Soft delete
	err = suite.Db.SoftDelete("input1", "test-soft-delete")
	suite.Require().NoError(err)

	// Verify attached function is marked as deleted in DB
	var retrieved dbmodel.AttachedFunction
	err = suite.db.Unscoped().Where("id = ?", attachedFunction.ID).First(&retrieved).Error
	suite.Require().NoError(err)
	suite.Require().True(retrieved.IsDeleted)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_SoftDelete_NotFound() {
	// Try to delete non-existent attached function - should succeed but do nothing
	err := suite.Db.SoftDelete("input1", "nonexistent-attachedFunction")
	suite.Require().NoError(err)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_DeleteAll() {
	functionID := dbmodel.FunctionRecordCounter

	// Insert multiple attached functions
	attachedFunctions := []*dbmodel.AttachedFunction{
		{
			ID:                      uuid.New(),
			Name:                    "attachedFunction1",
			FunctionID:              functionID,
			InputCollectionID:       "input1",
			OutputCollectionName:    "output1",
			FunctionParams:          "{}",
			TenantID:                "tenant1",
			DatabaseID:              "db-delete-all",
			MinRecordsForInvocation: 100,
			IsReady:                 true,
		},
		{
			ID:                      uuid.New(),
			Name:                    "attachedFunction2",
			FunctionID:              functionID,
			InputCollectionID:       "input2",
			OutputCollectionName:    "output2",
			FunctionParams:          "{}",
			TenantID:                "tenant1",
			DatabaseID:              "db-delete-all",
			MinRecordsForInvocation: 100,
			IsReady:                 true,
		},
		{
			ID:                      uuid.New(),
			Name:                    "attachedFunction3",
			FunctionID:              functionID,
			InputCollectionID:       "input3",
			OutputCollectionName:    "output3",
			FunctionParams:          "{}",
			TenantID:                "tenant1",
			DatabaseID:              "db-delete-all",
			MinRecordsForInvocation: 100,
			IsReady:                 true,
		},
	}

	for _, attachedFunction := range attachedFunctions {
		err := suite.Db.Insert(attachedFunction)
		suite.Require().NoError(err)
	}

	// Delete all attached functions
	err := suite.Db.DeleteAll()
	suite.Require().NoError(err)

	// Verify all attached functions are deleted
	for _, attachedFunction := range attachedFunctions {
		name := attachedFunction.Name
		inputColID := attachedFunction.InputCollectionID
		results, err := suite.Db.GetAttachedFunctions(nil, &name, &inputColID, true)
		suite.Require().NoError(err)
		suite.Require().Len(results, 0)
	}

	// Cleanup
	for _, attachedFunction := range attachedFunctions {
		suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
	}
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByID() {
	attachedFunctionID := uuid.New()
	functionID := dbmodel.FunctionRecordCounter

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID,
		Name:                    "test-get-by-id-attachedFunction",
		FunctionID:              functionID,
		InputCollectionID:       "input_col_id",
		OutputCollectionName:    "output_col_name",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 true,
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	results, err := suite.Db.GetAttachedFunctions(&attachedFunctionID, nil, nil, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 1)
	suite.Require().Equal(attachedFunction.ID, results[0].ID)
	suite.Require().Equal(attachedFunction.Name, results[0].Name)
	suite.Require().Equal(attachedFunction.FunctionID, results[0].FunctionID)

	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByID_NoReady() {
	attachedFunctionID := uuid.New()
	functionID := dbmodel.FunctionRecordCounter

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID,
		Name:                    "test-get-by-id-attachedFunction",
		FunctionID:              functionID,
		InputCollectionID:       "input_col_id",
		OutputCollectionName:    "output_col_name",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 false,
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	retrieved, err := suite.Db.GetAttachedFunctions(&attachedFunctionID, nil, nil, false)
	suite.Require().NoError(err)
	suite.Require().Len(retrieved, 0)

	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByID_NotFound() {
	id := uuid.New()
	results, err := suite.Db.GetAttachedFunctions(&id, nil, nil, false)
	suite.Require().NoError(err)
	suite.Require().Len(results, 0)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByID_IgnoresDeleted() {
	attachedFunctionID := uuid.New()
	functionID := dbmodel.FunctionRecordCounter

	attachedFunction := &dbmodel.AttachedFunction{
		ID:                      attachedFunctionID,
		Name:                    "test-get-by-id-deleted",
		FunctionID:              functionID,
		InputCollectionID:       "input1",
		OutputCollectionName:    "output1",
		FunctionParams:          "{}",
		TenantID:                "tenant1",
		DatabaseID:              "db1",
		MinRecordsForInvocation: 100,
		IsReady:                 true,
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	err = suite.Db.SoftDelete("input1", "test-get-by-id-deleted")
	suite.Require().NoError(err)

	results, err := suite.Db.GetAttachedFunctions(&attachedFunctionID, nil, nil, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 0)

	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

// TestFunctionConstantsMatchSeededDatabase verifies that function constants in
// dbmodel/constants.go match what we seed in the test database (which should match migrations).
// This catches drift between constants and migrations at test time.
func (suite *AttachedFunctionDbTestSuite) TestFunctionConstantsMatchSeededDatabase() {
	// Map of function names to expected UUIDs from constants.go
	// When you add a new function:
	// 1. Add to migration
	// 2. Add to dbmodel/constants.go
	// 3. Add to SetupSuite() seed list
	// 4. Add here for validation
	expectedFunctions := map[string]uuid.UUID{
		dbmodel.FunctionNameRecordCounter: dbmodel.FunctionRecordCounter,
	}

	// Verify count matches
	var actualCount int64
	err := suite.db.Model(&dbmodel.Function{}).Count(&actualCount).Error
	suite.Require().NoError(err, "Failed to count functions")

	expectedCount := int64(len(expectedFunctions))
	suite.Require().Equal(expectedCount, actualCount,
		"Function count mismatch. Expected: %d, Actual: %d. "+
			"Did you forget to update SetupSuite() after adding a new function?",
		expectedCount, actualCount)

	// Verify each function
	for functionName, expectedUUID := range expectedFunctions {
		var function dbmodel.Function
		err := suite.db.Where("name = ?", functionName).First(&function).Error
		suite.Require().NoError(err, "Function '%s' not found", functionName)

		suite.Require().Equal(expectedUUID, function.ID,
			"Function '%s' UUID mismatch. Constant: %s, DB: %s",
			functionName, expectedUUID, function.ID)
	}
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetAttachedFunctions() {
	// Create test attached functions
	collectionID1 := "collection1"
	collectionID2 := "collection2"
	name1 := "function1"
	name2 := "function2"

	af1 := &dbmodel.AttachedFunction{
		ID:                   uuid.New(),
		Name:                 name1,
		FunctionID:           dbmodel.FunctionRecordCounter,
		InputCollectionID:    collectionID1,
		OutputCollectionName: "output1",
		FunctionParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		IsReady:              true,
	}
	af2 := &dbmodel.AttachedFunction{
		ID:                   uuid.New(),
		Name:                 name2,
		FunctionID:           dbmodel.FunctionRecordCounter,
		InputCollectionID:    collectionID1,
		OutputCollectionName: "output2",
		FunctionParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		IsReady:              true,
	}
	af3 := &dbmodel.AttachedFunction{
		ID:                   uuid.New(),
		Name:                 name1,
		FunctionID:           dbmodel.FunctionRecordCounter,
		InputCollectionID:    collectionID2,
		OutputCollectionName: "output3",
		FunctionParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		IsReady:              false, // Not ready
	}

	suite.Require().NoError(suite.Db.Insert(af1))
	suite.Require().NoError(suite.Db.Insert(af2))
	suite.Require().NoError(suite.Db.Insert(af3))

	defer func() {
		suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id IN ?", []uuid.UUID{af1.ID, af2.ID, af3.ID})
	}()

	// Test 1: Get by ID (ready only)
	results, err := suite.Db.GetAttachedFunctions(&af1.ID, nil, nil, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 1)
	suite.Require().Equal(af1.ID, results[0].ID)

	// Test 2: Get by ID (include not ready)
	results, err = suite.Db.GetAttachedFunctions(&af3.ID, nil, nil, false)
	suite.Require().NoError(err)
	suite.Require().Len(results, 1)
	suite.Require().Equal(af3.ID, results[0].ID)
	suite.Require().False(results[0].IsReady)

	// Test 3: Get by name and collection ID (ready only)
	results, err = suite.Db.GetAttachedFunctions(nil, &name1, &collectionID1, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 1)
	suite.Require().Equal(af1.ID, results[0].ID)

	// Test 4: Get by collection ID (ready only)
	results, err = suite.Db.GetAttachedFunctions(nil, nil, &collectionID1, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 2)

	// Test 5: Get by name (across collections, ready only)
	results, err = suite.Db.GetAttachedFunctions(nil, &name1, nil, true)
	suite.Require().NoError(err)
	suite.Require().Len(results, 1)
	suite.Require().Equal(collectionID1, results[0].InputCollectionID)

	// Test 6: Get by name (across collections, include not ready)
	results, err = suite.Db.GetAttachedFunctions(nil, &name1, nil, false)
	suite.Require().NoError(err)
	suite.Require().Len(results, 2)
}

func TestAttachedFunctionDbTestSuite(t *testing.T) {
	testSuite := new(AttachedFunctionDbTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
