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
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	// Retrieve by name
	retrieved, err := suite.Db.GetByName("input_col_id", "test-get-attachedFunction")
	suite.Require().NoError(err)
	suite.Require().NotNil(retrieved)
	suite.Require().Equal(attachedFunction.ID, retrieved.ID)
	suite.Require().Equal(attachedFunction.Name, retrieved.Name)
	suite.Require().Equal(attachedFunction.FunctionID, retrieved.FunctionID)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByName_NotFound() {
	// Try to get non-existent attached function
	retrieved, err := suite.Db.GetByName("input_col_id", "nonexistent-attachedFunction")
	suite.Require().NoError(err)
	suite.Require().Nil(retrieved)
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
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	// Soft delete it
	err = suite.Db.SoftDelete("input1", "test-deleted-attachedFunction")
	suite.Require().NoError(err)

	// GetByName should not return it
	retrieved, err := suite.Db.GetByName("input1", "test-deleted-attachedFunction")
	suite.Require().NoError(err)
	suite.Require().Nil(retrieved)

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
		retrieved, err := suite.Db.GetByName(attachedFunction.InputCollectionID, attachedFunction.Name)
		suite.Require().NoError(err)
		suite.Require().Nil(retrieved)
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
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	retrieved, err := suite.Db.GetByID(attachedFunctionID)
	suite.Require().NoError(err)
	suite.Require().NotNil(retrieved)
	suite.Require().Equal(attachedFunction.ID, retrieved.ID)
	suite.Require().Equal(attachedFunction.Name, retrieved.Name)
	suite.Require().Equal(attachedFunction.FunctionID, retrieved.FunctionID)

	suite.db.Unscoped().Delete(&dbmodel.AttachedFunction{}, "id = ?", attachedFunction.ID)
}

func (suite *AttachedFunctionDbTestSuite) TestAttachedFunctionDb_GetByID_NotFound() {
	retrieved, err := suite.Db.GetByID(uuid.New())
	suite.Require().NoError(err)
	suite.Require().Nil(retrieved)
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
	}

	err := suite.Db.Insert(attachedFunction)
	suite.Require().NoError(err)

	err = suite.Db.SoftDelete("input1", "test-get-by-id-deleted")
	suite.Require().NoError(err)

	retrieved, err := suite.Db.GetByID(attachedFunctionID)
	suite.Require().NoError(err)
	suite.Require().Nil(retrieved)

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

func TestAttachedFunctionDbTestSuite(t *testing.T) {
	testSuite := new(AttachedFunctionDbTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
