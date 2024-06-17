package notification

import (
	"context"
	"reflect"
	"testing"

	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/pkg/metastore/db/dbmodel/mocks"
	"github.com/chroma-core/chroma/go/pkg/model"
	"github.com/stretchr/testify/mock"
)

func TestDatabaseNotificationStore_GetAllPendingNotifications(t *testing.T) {
	// Create a mock implementation of dbmodel.IMetaDomain
	mockMetaDomain := &mocks.IMetaDomain{}

	// Create a mock implementation of dbmodel.ITransaction
	mockTxImpl := &mocks.ITransaction{}

	// Create a new instance of DatabaseNotificationStore
	store := NewDatabaseNotificationStore(mockTxImpl, mockMetaDomain)

	// Create a mock context
	ctx := context.Background()

	notification1 := model.Notification{ID: 1, CollectionID: "collection1", Status: model.NotificationStatusPending}
	notification2 := model.Notification{ID: 2, CollectionID: "collection1", Status: model.NotificationStatusPending}
	notification3 := model.Notification{ID: 3, CollectionID: "collection2", Status: model.NotificationStatusPending}
	// Define the expected result

	expectedResult := map[string][]model.Notification{
		"collection1": {notification1, notification2},
		"collection2": {notification3},
	}

	dbNotification1 := dbmodel.Notification{ID: 1, CollectionID: "collection1", Status: dbmodel.NotificationStatusPending}
	dbNotification2 := dbmodel.Notification{ID: 2, CollectionID: "collection1", Status: dbmodel.NotificationStatusPending}
	dbNotification3 := dbmodel.Notification{ID: 3, CollectionID: "collection2", Status: dbmodel.NotificationStatusPending}

	expectedDBResult := []*dbmodel.Notification{&dbNotification1, &dbNotification2, &dbNotification3}

	// Set up the mock implementation to return the expected result
	// mockTxImpl.On("Transaction", context.Background(), mock.Anything).Return(nil)
	mockMetaDomain.On("NotificationDb", context.Background()).Return(&mocks.INotificationDb{})
	mockMetaDomain.NotificationDb(context.Background()).(*mocks.INotificationDb).On("GetAllPendingNotifications").Return(expectedDBResult, nil)

	// Call the method under test
	result, err := store.GetAllPendingNotifications(ctx)

	// Assert the result
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(result) != len(expectedResult) {
		t.Errorf("Unexpected result length. Expected: %d, Got: %d", len(expectedResult), len(result))
	}

	// Compare the actual result with the expected result
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("Unexpected result. Got: %v, Want: %v", result, expectedResult)
	}

	// Verify that the mock implementation was called as expected
	mockMetaDomain.AssertExpectations(t)
	mockTxImpl.AssertExpectations(t)
}

func TestDatabaseNotificationStore_GetNotifications(t *testing.T) {
	// Create a mock implementation of dbmodel.IMetaDomain
	mockMetaDomain := &mocks.IMetaDomain{}

	// Create a mock implementation of dbmodel.ITransaction
	mockTxImpl := &mocks.ITransaction{}

	// Create a new instance of DatabaseNotificationStore
	store := NewDatabaseNotificationStore(mockTxImpl, mockMetaDomain)

	// Create a mock context
	ctx := context.Background()

	notification1 := model.Notification{ID: 1, CollectionID: "collection1", Status: model.NotificationStatusPending}
	notification2 := model.Notification{ID: 2, CollectionID: "collection1", Status: model.NotificationStatusPending}
	// Define the expected result

	expectedResult := []model.Notification{notification1, notification2}

	dbNotification1 := dbmodel.Notification{ID: 1, CollectionID: "collection1", Status: dbmodel.NotificationStatusPending}
	dbNotification2 := dbmodel.Notification{ID: 2, CollectionID: "collection1", Status: dbmodel.NotificationStatusPending}

	expectedDBResult := []*dbmodel.Notification{&dbNotification1, &dbNotification2}

	// Set up the mock implementation to return the expected result
	// mockTxImpl.On("Transaction", context.Background(), mock.Anything).Return(nil)
	mockMetaDomain.On("NotificationDb", context.Background()).Return(&mocks.INotificationDb{})
	mockMetaDomain.NotificationDb(context.Background()).(*mocks.INotificationDb).On("GetNotificationByCollectionID", "collection1").Return(expectedDBResult, nil)

	// Call the method under test
	result, err := store.GetNotifications(ctx, "collection1")

	// Assert the result
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if len(result) != len(expectedResult) {
		t.Errorf("Unexpected result length. Expected: %d, Got: %d", len(expectedResult), len(result))
	}

	// Compare the actual result with the expected result
	if !reflect.DeepEqual(result, expectedResult) {
		t.Errorf("Unexpected result. Got: %v, Want: %v", result, expectedResult)
	}

	// Verify that the mock implementation was called as expected
	mockMetaDomain.AssertExpectations(t)
	mockTxImpl.AssertExpectations(t)
}

func TestDatabaseNotificationStore_AddNotification(t *testing.T) {
	// Create a mock implementation of dbmodel.IMetaDomain
	mockMetaDomain := &mocks.IMetaDomain{}

	// Create a mock implementation of dbmodel.ITransaction
	mockTxImpl := &mocks.ITransaction{}

	// Create a new instance of DatabaseNotificationStore
	store := NewDatabaseNotificationStore(mockTxImpl, mockMetaDomain)

	// Create a mock context
	ctx := context.Background()

	notification1 := model.Notification{ID: 1, CollectionID: "collection1", Status: model.NotificationStatusPending}

	dbNotification1 := dbmodel.Notification{ID: 1, CollectionID: "collection1", Status: dbmodel.NotificationStatusPending}

	// Set up the mock implementation to return the expected result
	mockTxImpl.On("Transaction", context.Background(), mock.Anything).Return(nil)
	mockMetaDomain.On("NotificationDb", context.Background()).Return(&mocks.INotificationDb{})
	mockMetaDomain.NotificationDb(context.Background()).(*mocks.INotificationDb).On("AddNotification", &dbNotification1).Return(nil)

	// Call the method under test
	err := store.AddNotification(ctx, notification1)

	// Assert the result
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify that the mock implementation was called as expected
	mockMetaDomain.AssertExpectations(t)
	mockTxImpl.AssertExpectations(t)
}

func TestDatabaseNotificationStore_RemoveNotifications(t *testing.T) {
	// Create a mock implementation of dbmodel.IMetaDomain
	mockMetaDomain := &mocks.IMetaDomain{}

	// Create a mock implementation of dbmodel.ITransaction
	mockTxImpl := &mocks.ITransaction{}

	// Create a new instance of DatabaseNotificationStore
	store := NewDatabaseNotificationStore(mockTxImpl, mockMetaDomain)

	// Create a mock context
	ctx := context.Background()

	notification1 := model.Notification{ID: 1, CollectionID: "collection1", Status: model.NotificationStatusPending}
	notification2 := model.Notification{ID: 2, CollectionID: "collection1", Status: model.NotificationStatusPending}

	dbNotification1 := dbmodel.Notification{ID: 1, CollectionID: "collection1", Status: dbmodel.NotificationStatusPending}
	dbNotification2 := dbmodel.Notification{ID: 2, CollectionID: "collection1", Status: dbmodel.NotificationStatusPending}

	// Set up the mock implementation to return the expected result
	mockTxImpl.On("Transaction", context.Background(), mock.Anything).Return(nil)
	mockMetaDomain.On("NotificationDb", context.Background()).Return(&mocks.INotificationDb{})
	mockMetaDomain.NotificationDb(context.Background()).(*mocks.INotificationDb).On("DeleteNotification", &dbNotification1).Return(nil)
	mockMetaDomain.NotificationDb(context.Background()).(*mocks.INotificationDb).On("DeleteNotification", &dbNotification2).Return(nil)

	// Call the method under test
	err := store.RemoveNotifications(ctx, []model.Notification{notification1, notification2})

	// Assert the result
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Verify that the mock implementation was called as expected
	mockMetaDomain.AssertExpectations(t)
	mockTxImpl.AssertExpectations(t)
}
