package notification

import (
	"context"
	"reflect"
	"testing"

	"github.com/chroma-core/chroma/go/internal/model"
)

func TestMemoryNotificationStore_GetAllPendingNotifications(t *testing.T) {
	// Create a new MemoryNotificationStore
	store := NewMemoryNotificationStore()

	// Create some test notifications
	notification1 := model.Notification{ID: 1, CollectionID: "collection1", Status: model.NotificationStatusPending}
	notification2 := model.Notification{ID: 2, CollectionID: "collection1", Status: model.NotificationStatusPending}
	notification3 := model.Notification{ID: 3, CollectionID: "collection2", Status: model.NotificationStatusPending}

	// Add the test notifications to the store
	store.AddNotification(context.Background(), notification1)
	store.AddNotification(context.Background(), notification2)
	store.AddNotification(context.Background(), notification3)

	// Get all pending notifications
	notifications, err := store.GetAllPendingNotifications(context.Background())
	if err != nil {
		t.Errorf("Error getting pending notifications: %v", err)
	}

	// Define the expected result
	expected := map[string][]model.Notification{
		"collection1": {notification1, notification2},
		"collection2": {notification3},
	}

	// Compare the actual result with the expected result
	if !reflect.DeepEqual(notifications, expected) {
		t.Errorf("Unexpected result. Got: %v, Want: %v", notifications, expected)
	}
}

func TestMemoryNotificationStore_GetNotifications(t *testing.T) {
	// Create a new MemoryNotificationStore
	store := NewMemoryNotificationStore()

	// Create some test notifications
	notification1 := model.Notification{ID: 1, CollectionID: "collection1", Status: model.NotificationStatusPending}
	notification2 := model.Notification{ID: 2, CollectionID: "collection1", Status: model.NotificationStatusPending}
	notification3 := model.Notification{ID: 3, CollectionID: "collection2", Status: model.NotificationStatusPending}
	notification4 := model.Notification{ID: 4, CollectionID: "collection2", Status: model.NotificationStatusPending}

	// Add the test notifications to the store
	store.AddNotification(context.Background(), notification1)
	store.AddNotification(context.Background(), notification2)

	// Add the test notifications to the store, in reverse order
	store.AddNotification(context.Background(), notification4)
	store.AddNotification(context.Background(), notification3)

	// Get notifications for collection1
	notifications, err := store.GetNotifications(context.Background(), "collection1")
	if err != nil {
		t.Errorf("Error getting notifications: %v", err)
	}

	// Define the expected result
	expected := []model.Notification{notification1, notification2}

	// Compare the actual result with the expected result
	if !reflect.DeepEqual(notifications, expected) {
		t.Errorf("Unexpected result. Got: %v, Want: %v", notifications, expected)
	}

	// Get notifications for collection2
	notifications, err = store.GetNotifications(context.Background(), "collection2")
	if err != nil {
		t.Errorf("Error getting notifications: %v", err)
	}
	expected = []model.Notification{notification3, notification4}
	if !reflect.DeepEqual(notifications, expected) {
		t.Errorf("Unexpected result. Got: %v, Want: %v", notifications, expected)
	}
}

func TestMemoryNotificationStore_AddNotification(t *testing.T) {
	// Create a new MemoryNotificationStore
	store := NewMemoryNotificationStore()

	// Create a test notification
	notification := model.Notification{ID: 1, CollectionID: "collection1", Status: model.NotificationStatusPending}

	// Add the test notification to the store
	err := store.AddNotification(context.Background(), notification)
	if err != nil {
		t.Errorf("Error adding notification: %v", err)
	}

	// Get all pending notifications
	notifications, err := store.GetAllPendingNotifications(context.Background())
	if err != nil {
		t.Errorf("Error getting pending notifications: %v", err)
	}

	// Define the expected result
	expected := map[string][]model.Notification{
		"collection1": {notification},
	}

	// Compare the actual result with the expected result
	if !reflect.DeepEqual(notifications, expected) {
		t.Errorf("Unexpected result. Got: %v, Want: %v", notifications, expected)
	}
}

func TestMemoryNotificationStore_RemoveNotification(t *testing.T) {
	// Create a new MemoryNotificationStore
	store := NewMemoryNotificationStore()

	// Create a test notification
	notification := model.Notification{ID: 1, CollectionID: "collection1"}

	// Add the test notification to the store
	store.AddNotification(context.Background(), notification)

	// Remove the test notification from the store
	err := store.RemoveNotifications(context.Background(), []model.Notification{notification})
	if err != nil {
		t.Errorf("Error removing notification: %v", err)
	}

	// Get all pending notifications
	notifications, err := store.GetAllPendingNotifications(context.Background())
	if err != nil {
		t.Errorf("Error getting pending notifications: %v", err)
	}

	// Define the expected result
	expected := map[string][]model.Notification{}

	// Compare the actual result with the expected result
	if !reflect.DeepEqual(notifications, expected) {
		t.Errorf("Unexpected result. Got: %v, Want: %v", notifications, expected)
	}
}
