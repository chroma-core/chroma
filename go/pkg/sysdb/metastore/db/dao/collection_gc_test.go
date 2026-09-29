package dao

import (
	"fmt"
	"time"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/google/uuid"
)

func (suite *CollectionDbTestSuite) TestDeletedCollectionReservation() {
	for _, tc := range []struct {
		name               string
		live, deleted      int
		reservation, limit uint64
		want               []string
	}{
		{"reserved", 4, 3, 2, 4, []string{"deleted-0", "deleted-1", "live-0", "live-1"}},
		{"live_borrows", 4, 1, 2, 4, []string{"deleted-0", "live-0", "live-1", "live-2"}},
		{"deleted_borrows", 1, 4, 2, 4, []string{"deleted-0", "deleted-1", "live-0", "deleted-2"}},
		{"only_live", 4, 0, 2, 4, []string{"live-0", "live-1", "live-2", "live-3"}},
		{"only_deleted", 0, 4, 2, 4, []string{"deleted-0", "deleted-1", "deleted-2", "deleted-3"}},
		{"disabled", 4, 3, 0, 4, []string{"live-0", "live-1", "live-2", "live-3"}},
		{"reservation_exceeds_limit", 4, 3, 10, 2, []string{"deleted-0", "deleted-1"}},
		{"zero_limit", 4, 3, 2, 0, []string{}},
	} {
		suite.Run(tc.name, func() {
			tx := suite.db.Begin()
			defer tx.Rollback()
			dao := &collectionDb{db: tx, read_db: tx}
			tenant := uuid.NewString()
			old := time.Now().UTC().Add(-48 * time.Hour)
			for _, kind := range []string{"live", "deleted"} {
				n := tc.live
				if kind == "deleted" {
					n = tc.deleted
				}
				for i := 0; i < n; i++ {
					name := fmt.Sprintf("%s-%d", kind, i)
					versions := uint32(100 - i)
					if kind == "deleted" {
						versions = 1
					}
					row := dbmodel.Collection{ID: uuid.NewString(), Name: &name, DatabaseID: suite.databaseId,
						Tenant: tenant, IsDeleted: kind == "deleted", NumVersions: versions,
						VersionFileName: "version-file", OldestVersionTs: old, UpdatedAt: old.Add(time.Duration(i) * time.Hour)}
					suite.Require().NoError(tx.Create(&row).Error)
				}
			}
			cutoff, minVersions := uint64(time.Now().Add(-6*time.Hour).Unix()), uint64(6)
			rows, err := dao.ListCollectionsToGc(&cutoff, &tc.limit, &tenant, &minVersions, &tc.reservation)
			suite.Require().NoError(err)
			got := make([]string, 0, len(rows))
			for _, row := range rows {
				got = append(got, row.Name)
			}
			suite.Equal(tc.want, got)
		})
	}
}

func (suite *CollectionDbTestSuite) TestDeletedForkReservation() {
	tx := suite.db.Begin()
	defer tx.Rollback()
	dao := &collectionDb{db: tx, read_db: tx}
	tenant := uuid.NewString()
	old := time.Now().UTC().Add(-48 * time.Hour)
	rootID := uuid.NewString()
	for _, item := range []struct {
		name     string
		deleted  bool
		versions uint32
		root     *string
	}{
		{"root", false, 1, nil}, {"deleted-fork", true, 1, &rootID},
		{"busy-live", false, 100, nil},
	} {
		id := uuid.NewString()
		if item.name == "root" {
			id = rootID
		}
		row := dbmodel.Collection{ID: id, Name: &item.name, DatabaseID: suite.databaseId, Tenant: tenant,
			IsDeleted: item.deleted, NumVersions: item.versions, RootCollectionId: item.root,
			VersionFileName: "version-file", OldestVersionTs: old, UpdatedAt: old}
		suite.Require().NoError(tx.Create(&row).Error)
	}
	cutoff, limit, minVersions, reservation := uint64(time.Now().Unix()), uint64(2), uint64(6), uint64(1)
	rows, err := dao.ListCollectionsToGc(&cutoff, &limit, &tenant, &minVersions, &reservation)
	suite.Require().NoError(err)
	suite.Require().Len(rows, 2)
	suite.Equal(rootID, rows[0].ID)
	suite.Equal("busy-live", rows[1].Name)
}
