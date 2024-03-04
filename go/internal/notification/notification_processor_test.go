package notification

import (
	"context"
	"testing"

	"github.com/chroma-core/chroma/go/internal/metastore/db/dao"
	"github.com/chroma-core/chroma/go/internal/metastore/db/dbcore"
	"github.com/chroma-core/chroma/go/internal/metastore/db/dbmodel"
	"github.com/chroma-core/chroma/go/internal/model"
	"github.com/chroma-core/chroma/go/internal/proto/coordinatorpb"
	"google.golang.org/protobuf/proto"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSimpleNotificationProcessor(t *testing.T) {
	ctx := context.Background()
	db := setupDatabase()
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	notificationStore := NewDatabaseNotificationStore(txnImpl, metaDomain)
	notifier := NewMemoryNotifier()
	notificationProcessor := NewSimpleNotificationProcessor(ctx, notificationStore, notifier)
	notificationProcessor.Start()

	notification := model.Notification{
		CollectionID: "collection1",
		Type:         model.NotificationTypeDeleteCollection,
		Status:       model.NotificationStatusPending,
	}
	resultChan := make(chan error)
	triggerMsg := TriggerMessage{
		Msg:        notification,
		ResultChan: resultChan,
	}
	notificationStore.AddNotification(ctx, notification)
	notificationProcessor.Trigger(ctx, triggerMsg)

	// Wait for the notification to be processed.
	err := <-resultChan
	if err != nil {
		t.Errorf("Failed to process notification %v", err)
	}
	if len(notifier.queue) != 1 {
		t.Errorf("Notification is not sent by the notifier")
	}
	for _, msg := range notifier.queue {
		newMsgPb := coordinatorpb.Notification{}
		err := proto.Unmarshal(msg.Payload, &newMsgPb)
		if err != nil {
			t.Errorf("Failed to unmarshal message %v", err)
		}
		newMsg := model.Notification{
			CollectionID: newMsgPb.CollectionId,
			Type:         newMsgPb.Type,
			Status:       newMsgPb.Status,
		}
		if err != nil {
			t.Errorf("Failed to unmarshal message %v", err)
		}
		if newMsg.CollectionID != notification.CollectionID {
			t.Errorf("CollectionID is not equal %v, %v", newMsg.CollectionID, notification.CollectionID)
		}
		if newMsg.Type != notification.Type {
			t.Errorf("Type is not equal %v, %v", newMsg.Type, notification.Type)
		}
		if newMsg.Status != notification.Status {
			t.Errorf("Status is not equal, %v, %v", newMsg.Status, notification.Status)
		}
	}
	notificationProcessor.Stop()
	cleanupDatabase(db)
}

func TestSimpleNotificationProcessorWithExistingNotification(t *testing.T) {
	ctx := context.Background()
	db := setupDatabase()
	txnImpl := dbcore.NewTxImpl()
	metaDomain := dao.NewMetaDomain()
	notificationStore := NewDatabaseNotificationStore(txnImpl, metaDomain)
	notifier := NewMemoryNotifier()
	notificationProcessor := NewSimpleNotificationProcessor(ctx, notificationStore, notifier)

	notification := model.Notification{
		CollectionID: "collection1",
		Type:         model.NotificationTypeDeleteCollection,
		Status:       model.NotificationStatusPending,
	}
	// Only add to the notification store, but not trigger it.
	notificationStore.AddNotification(ctx, notification)

	notificationProcessor.Start()

	if len(notifier.queue) != 1 {
		t.Errorf("Notification is not sent by the notifier")
	}
	for _, msg := range notifier.queue {
		newMsgPb := coordinatorpb.Notification{}
		err := proto.Unmarshal(msg.Payload, &newMsgPb)
		if err != nil {
			t.Errorf("Failed to unmarshal message %v", err)
		}
		newMsg := model.Notification{
			CollectionID: newMsgPb.CollectionId,
			Type:         newMsgPb.Type,
			Status:       newMsgPb.Status,
		}
		if newMsg.CollectionID != notification.CollectionID {
			t.Errorf("CollectionID is not equal %v, %v", newMsg.CollectionID, notification.CollectionID)
		}
		if newMsg.Type != notification.Type {
			t.Errorf("Type is not equal %v, %v", newMsg.Type, notification.Type)
		}
		if newMsg.Status != notification.Status {
			t.Errorf("Status is not equal, %v, %v", newMsg.Status, notification.Status)
		}
	}
	notificationProcessor.Stop()
	cleanupDatabase(db)
}

func setupDatabase() *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	if err != nil {
		panic("failed to connect database")
	}
	dbcore.SetGlobalDB(db)
	db.Migrator().CreateTable(&dbmodel.Notification{})
	return db
}

func cleanupDatabase(db *gorm.DB) {
	db.Migrator().DropTable(&dbmodel.Notification{})
	dbcore.SetGlobalDB(nil)
}
