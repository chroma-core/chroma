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

type TaskDbTestSuite struct {
	suite.Suite
	db *gorm.DB
	Db *taskDb
	t  *testing.T
}

func (suite *TaskDbTestSuite) SetupSuite() {
	log.Info("setup suite")
	suite.db, _ = dbcore.ConfigDatabaseForTesting()
	suite.Db = &taskDb{
		db: suite.db,
	}

	// Seed operators for tests - these must match dbmodel/constants.go
	// This also serves as a validation that constants are correct
	operators := []dbmodel.Operator{
		{
			OperatorID:    dbmodel.OperatorRecordCounter,
			OperatorName:  dbmodel.OperatorNameRecordCounter,
			IsIncremental: dbmodel.OperatorRecordCounterIsIncremental,
			ReturnType:    dbmodel.OperatorRecordCounterReturnType,
		},
	}
	for _, op := range operators {
		suite.db.Where(dbmodel.Operator{OperatorID: op.OperatorID}).FirstOrCreate(&op)
	}
}

func (suite *TaskDbTestSuite) SetupTest() {
	log.Info("setup test")
}

func (suite *TaskDbTestSuite) TearDownTest() {
	log.Info("teardown test")
}

func (suite *TaskDbTestSuite) TestTaskDb_Insert() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	nextNonce, _ := uuid.NewV7()

	task := &dbmodel.Task{
		ID:                   taskID,
		Name:                 "test-insert-task",
		OperatorID:           operatorID,
		InputCollectionID:    "input_col_id",
		OutputCollectionName: "output_col_name",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            nextNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	// Verify task was inserted
	var retrieved dbmodel.Task
	err = suite.db.Where("task_name = ? AND tenant_id = ? AND database_id = ?", "test-insert-task", "tenant1", "db1").First(&retrieved).Error
	suite.Require().NoError(err)
	suite.Require().Equal(task.Name, retrieved.Name)
	suite.Require().Equal(task.OperatorID, retrieved.OperatorID)
	suite.Require().False(retrieved.IsDeleted)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_Insert_DuplicateName() {
	taskID1 := uuid.New()
	operatorID1 := dbmodel.OperatorRecordCounter
	nextNonce1, _ := uuid.NewV7()

	task1 := &dbmodel.Task{
		ID:                   taskID1,
		Name:                 "test-task-1",
		OperatorID:           operatorID1,
		InputCollectionID:    "input1",
		OutputCollectionName: "output1",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            nextNonce1,
	}

	err := suite.Db.Insert(task1)
	suite.Require().NoError(err)

	// Try to insert duplicate (same tenant, database, and name)
	taskID2 := uuid.New()
	operatorID2 := dbmodel.OperatorRecordCounter
	nextNonce2, _ := uuid.NewV7()

	task2 := &dbmodel.Task{
		ID:                   taskID2,
		Name:                 "test-task-1", // Same name as task1
		OperatorID:           operatorID2,
		InputCollectionID:    "input1",
		OutputCollectionName: "output1",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            nextNonce2,
	}

	err = suite.Db.Insert(task2)
	suite.Require().Error(err)
	suite.Require().Equal(common.ErrTaskAlreadyExists, err)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task1.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_GetByName() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	nextNonce, _ := uuid.NewV7()

	// Insert a task
	task := &dbmodel.Task{
		ID:                   taskID,
		Name:                 "test-get-task",
		OperatorID:           operatorID,
		InputCollectionID:    "input_col_id",
		OutputCollectionName: "output_col_name",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            nextNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	// Retrieve by name
	retrieved, err := suite.Db.GetByName("input_col_id", "test-get-task")
	suite.Require().NoError(err)
	suite.Require().NotNil(retrieved)
	suite.Require().Equal(task.ID, retrieved.ID)
	suite.Require().Equal(task.Name, retrieved.Name)
	suite.Require().Equal(task.OperatorID, retrieved.OperatorID)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_GetByName_NotFound() {
	// Try to get non-existent task
	retrieved, err := suite.Db.GetByName("input_col_id", "nonexistent-task")
	suite.Require().NoError(err)
	suite.Require().Nil(retrieved)
}

func (suite *TaskDbTestSuite) TestTaskDb_GetByName_IgnoresDeleted() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	nextNonce, _ := uuid.NewV7()

	// Insert a task
	task := &dbmodel.Task{
		ID:                   taskID,
		Name:                 "test-deleted-task",
		OperatorID:           operatorID,
		InputCollectionID:    "input1",
		OutputCollectionName: "output1",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            nextNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	// Soft delete it
	err = suite.Db.SoftDelete("input1", "test-deleted-task")
	suite.Require().NoError(err)

	// GetByName should not return it
	retrieved, err := suite.Db.GetByName("input1", "test-deleted-task")
	suite.Require().NoError(err)
	suite.Require().Nil(retrieved)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_SoftDelete() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	nextNonce, _ := uuid.NewV7()

	// Insert a task
	task := &dbmodel.Task{
		ID:                   taskID,
		Name:                 "test-soft-delete",
		OperatorID:           operatorID,
		InputCollectionID:    "input1",
		OutputCollectionName: "output1",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            nextNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	// Soft delete
	err = suite.Db.SoftDelete("input1", "test-soft-delete")
	suite.Require().NoError(err)

	// Verify task is marked as deleted in DB
	var retrieved dbmodel.Task
	err = suite.db.Unscoped().Where("task_id = ?", task.ID).First(&retrieved).Error
	suite.Require().NoError(err)
	suite.Require().True(retrieved.IsDeleted)

	// Cleanup
	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_SoftDelete_NotFound() {
	// Try to delete non-existent task - should succeed but do nothing
	err := suite.Db.SoftDelete("input1", "nonexistent-task")
	suite.Require().NoError(err)
}

func (suite *TaskDbTestSuite) TestTaskDb_DeleteAll() {
	operatorID := dbmodel.OperatorRecordCounter

	// Insert multiple tasks
	tasks := []*dbmodel.Task{
		{
			ID:                   uuid.New(),
			Name:                 "task1",
			OperatorID:           operatorID,
			InputCollectionID:    "input1",
			OutputCollectionName: "output1",
			OperatorParams:       "{}",
			TenantID:             "tenant1",
			DatabaseID:           "db-delete-all",
			MinRecordsForTask:    100,
			NextNonce:            uuid.Must(uuid.NewV7()),
		},
		{
			ID:                   uuid.New(),
			Name:                 "task2",
			OperatorID:           operatorID,
			InputCollectionID:    "input2",
			OutputCollectionName: "output2",
			OperatorParams:       "{}",
			TenantID:             "tenant1",
			DatabaseID:           "db-delete-all",
			MinRecordsForTask:    100,
			NextNonce:            uuid.Must(uuid.NewV7()),
		},
		{
			ID:                   uuid.New(),
			Name:                 "task3",
			OperatorID:           operatorID,
			InputCollectionID:    "input3",
			OutputCollectionName: "output3",
			OperatorParams:       "{}",
			TenantID:             "tenant1",
			DatabaseID:           "db-delete-all",
			MinRecordsForTask:    100,
			NextNonce:            uuid.Must(uuid.NewV7()),
		},
	}

	for _, task := range tasks {
		err := suite.Db.Insert(task)
		suite.Require().NoError(err)
	}

	// Delete all tasks
	err := suite.Db.DeleteAll()
	suite.Require().NoError(err)

	// Verify all tasks are deleted
	for _, task := range tasks {
		retrieved, err := suite.Db.GetByName(task.InputCollectionID, task.Name)
		suite.Require().NoError(err)
		suite.Require().Nil(retrieved)
	}

	// Cleanup
	for _, task := range tasks {
		suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
	}
}

func (suite *TaskDbTestSuite) TestTaskDb_GetByID() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	nextNonce, _ := uuid.NewV7()

	task := &dbmodel.Task{
		ID:                   taskID,
		Name:                 "test-get-by-id-task",
		OperatorID:           operatorID,
		InputCollectionID:    "input_col_id",
		OutputCollectionName: "output_col_name",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            nextNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	retrieved, err := suite.Db.GetByID(taskID)
	suite.Require().NoError(err)
	suite.Require().NotNil(retrieved)
	suite.Require().Equal(task.ID, retrieved.ID)
	suite.Require().Equal(task.Name, retrieved.Name)
	suite.Require().Equal(task.OperatorID, retrieved.OperatorID)

	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_GetByID_NotFound() {
	retrieved, err := suite.Db.GetByID(uuid.New())
	suite.Require().NoError(err)
	suite.Require().Nil(retrieved)
}

func (suite *TaskDbTestSuite) TestTaskDb_GetByID_IgnoresDeleted() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	nextNonce, _ := uuid.NewV7()

	task := &dbmodel.Task{
		ID:                   taskID,
		Name:                 "test-get-by-id-deleted",
		OperatorID:           operatorID,
		InputCollectionID:    "input1",
		OutputCollectionName: "output1",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            nextNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	err = suite.Db.SoftDelete("input1", "test-get-by-id-deleted")
	suite.Require().NoError(err)

	retrieved, err := suite.Db.GetByID(taskID)
	suite.Require().NoError(err)
	suite.Require().Nil(retrieved)

	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_AdvanceTask() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	originalNonce, _ := uuid.NewV7()

	task := &dbmodel.Task{
		ID:                   taskID,
		Name:                 "test-advance-task",
		OperatorID:           operatorID,
		InputCollectionID:    "input_col_id",
		OutputCollectionName: "output_col_name",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            originalNonce,
		CurrentAttempts:      3,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	err = suite.Db.AdvanceTask(taskID, originalNonce)
	suite.Require().NoError(err)

	retrieved, err := suite.Db.GetByID(taskID)
	suite.Require().NoError(err)
	suite.Require().NotNil(retrieved)
	suite.Require().NotEqual(originalNonce, retrieved.NextNonce)
	suite.Require().NotNil(retrieved.LastRun)
	suite.Require().Equal(int32(0), retrieved.CurrentAttempts)

	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_AdvanceTask_InvalidNonce() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	correctNonce, _ := uuid.NewV7()
	wrongNonce, _ := uuid.NewV7()

	task := &dbmodel.Task{
		ID:                   taskID,
		Name:                 "test-advance-task-wrong-nonce",
		OperatorID:           operatorID,
		InputCollectionID:    "input_col_id",
		OutputCollectionName: "output_col_name",
		OperatorParams:       "{}",
		TenantID:             "tenant1",
		DatabaseID:           "db1",
		MinRecordsForTask:    100,
		NextNonce:            correctNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	err = suite.Db.AdvanceTask(taskID, wrongNonce)
	suite.Require().Error(err)
	suite.Require().Equal(common.ErrTaskNotFound, err)

	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_AdvanceTask_NotFound() {
	err := suite.Db.AdvanceTask(uuid.New(), uuid.Must(uuid.NewV7()), 0, 0)
	suite.Require().Error(err)
	suite.Require().Equal(common.ErrTaskNotFound, err)
}

func (suite *TaskDbTestSuite) TestTaskDb_UpdateCompletionOffset() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	originalNonce, _ := uuid.NewV7()

	task := &dbmodel.Task{
		ID:                  taskID,
		Name:                "test_update_completion_task",
		OperatorID:          operatorID,
		InputCollectionID:   "input_collection_1",
		OutputCollectionID:  nil,
		OutputCollectionName: "output_collection_1",
		TenantID:            "tenant_1",
		DatabaseID:          "database_1",
		CompletionOffset:    100,
		MinRecordsForTask:   10,
		NextNonce:           originalNonce,
		LowestLiveNonce:     &originalNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	// Update completion offset to 200
	err = suite.Db.UpdateCompletionOffset(taskID, originalNonce, 200)
	suite.Require().NoError(err)

	// Verify the update
	retrieved, err := suite.Db.GetByID(taskID)
	suite.Require().NoError(err)
	suite.Require().Equal(int64(200), retrieved.CompletionOffset)
	// next_nonce should remain unchanged
	suite.Require().Equal(originalNonce, retrieved.NextNonce)

	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

func (suite *TaskDbTestSuite) TestTaskDb_UpdateCompletionOffset_InvalidNonce() {
	taskID := uuid.New()
	operatorID := dbmodel.OperatorRecordCounter
	correctNonce, _ := uuid.NewV7()
	wrongNonce, _ := uuid.NewV7()

	task := &dbmodel.Task{
		ID:                  taskID,
		Name:                "test_update_wrong_nonce",
		OperatorID:          operatorID,
		InputCollectionID:   "input_collection_1",
		OutputCollectionID:  nil,
		OutputCollectionName: "output_collection_1",
		TenantID:            "tenant_1",
		DatabaseID:          "database_1",
		CompletionOffset:    100,
		MinRecordsForTask:   10,
		NextNonce:           correctNonce,
		LowestLiveNonce:     &correctNonce,
	}

	err := suite.Db.Insert(task)
	suite.Require().NoError(err)

	// Try to update with wrong nonce
	err = suite.Db.UpdateCompletionOffset(taskID, wrongNonce, 200)
	suite.Require().Error(err)
	suite.Require().Equal(common.ErrTaskNotFound, err)

	suite.db.Unscoped().Delete(&dbmodel.Task{}, "task_id = ?", task.ID)
}

// TestOperatorConstantsMatchSeededDatabase verifies that operator constants in
// dbmodel/constants.go match what we seed in the test database (which should match migrations).
// This catches drift between constants and migrations at test time.
func (suite *TaskDbTestSuite) TestOperatorConstantsMatchSeededDatabase() {
	// Map of operator names to expected UUIDs from constants.go
	// When you add a new operator:
	// 1. Add to migration
	// 2. Add to dbmodel/constants.go
	// 3. Add to SetupSuite() seed list
	// 4. Add here for validation
	expectedOperators := map[string]uuid.UUID{
		dbmodel.OperatorNameRecordCounter: dbmodel.OperatorRecordCounter,
	}

	// Verify count matches
	var actualCount int64
	err := suite.db.Model(&dbmodel.Operator{}).Count(&actualCount).Error
	suite.Require().NoError(err, "Failed to count operators")

	expectedCount := int64(len(expectedOperators))
	suite.Require().Equal(expectedCount, actualCount,
		"Operator count mismatch. Expected: %d, Actual: %d. "+
			"Did you forget to update SetupSuite() after adding a new operator?",
		expectedCount, actualCount)

	// Verify each operator
	for operatorName, expectedUUID := range expectedOperators {
		var operator dbmodel.Operator
		err := suite.db.Where("operator_name = ?", operatorName).First(&operator).Error
		suite.Require().NoError(err, "Operator '%s' not found", operatorName)

		suite.Require().Equal(expectedUUID, operator.OperatorID,
			"Operator '%s' UUID mismatch. Constant: %s, DB: %s",
			operatorName, expectedUUID, operator.OperatorID)
	}
}

func TestTaskDbTestSuite(t *testing.T) {
	testSuite := new(TaskDbTestSuite)
	testSuite.t = t
	suite.Run(t, testSuite)
}
