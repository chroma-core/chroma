package dao

import (
	"fmt"
	"time"

	"github.com/chroma-core/chroma/go/pkg/sysdb/metastore/db/dbmodel"
	"github.com/google/uuid"
)

func (suite *CollectionDbTestSuite) TestGcPolicyRoundRobin() {
	for _, tc := range []struct {
		name          string
		live, deleted int
		limit         uint64
		want          []string
	}{
		{"alternates", 4, 3, 4, []string{"live-0", "deleted-0", "live-1", "deleted-1"}},
		{"live_fills", 4, 1, 4, []string{"live-0", "deleted-0", "live-1", "live-2"}},
		{"deleted_fills", 1, 4, 4, []string{"live-0", "deleted-0", "deleted-1", "deleted-2"}},
		{"only_live", 4, 0, 4, []string{"live-0", "live-1", "live-2", "live-3"}},
		{"only_deleted_overlap", 0, 4, 4, []string{"deleted-0", "deleted-1", "deleted-2", "deleted-3"}},
		{"odd_limit", 4, 3, 3, []string{"live-0", "deleted-0", "live-1"}},
		{"single_slot", 4, 3, 1, []string{"live-0"}},
		{"zero_limit", 4, 3, 0, []string{}},
		{"empty", 0, 0, 4, []string{}},
		{"short_batch", 1, 1, 4, []string{"live-0", "deleted-0"}},
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
						versions = uint32(5 - i)
					}
					row := dbmodel.Collection{ID: uuid.NewString(), Name: &name, DatabaseID: suite.databaseId,
						Tenant: tenant, IsDeleted: kind == "deleted", NumVersions: versions,
						VersionFileName: "version-file", OldestVersionTs: old, UpdatedAt: old.Add(time.Duration(i) * time.Hour)}
					suite.Require().NoError(tx.Create(&row).Error)
				}
			}
			cutoff, minVersions := uint64(time.Now().Add(-6*time.Hour).Unix()), uint64(6)
			rows, err := dao.ListCollectionsToGc(&cutoff, &tc.limit, &tenant, &minVersions)
			suite.Require().NoError(err)
			got := make([]string, 0, len(rows))
			for _, row := range rows {
				got = append(got, row.Name)
			}
			suite.Equal(tc.want, got)
		})
	}
}

func (suite *CollectionDbTestSuite) TestDeletedForkRoundRobin() {
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
	cutoff, limit, minVersions := uint64(time.Now().Unix()), uint64(2), uint64(6)
	rows, err := dao.ListCollectionsToGc(&cutoff, &limit, &tenant, &minVersions)
	suite.Require().NoError(err)
	suite.Require().Len(rows, 2)
	suite.Equal("busy-live", rows[0].Name)
	suite.Equal(rootID, rows[1].ID)
}

// High-version deleted collections remain eligible for the normal policy, while
// the deletion policy reaches old low-version collections in the same batch.
func (suite *CollectionDbTestSuite) TestGcPoliciesOverlap() {
	tx := suite.db.Begin()
	defer tx.Rollback()
	dao := &collectionDb{db: tx, read_db: tx}
	tenant := uuid.NewString()
	old := time.Now().UTC().Add(-48 * time.Hour)
	for i, item := range []struct {
		name     string
		deleted  bool
		versions uint32
	}{
		{"old-deleted", true, 1},
		{"busy-deleted", true, 200},
		{"live", false, 100},
		{"new-deleted", true, 2},
	} {
		row := dbmodel.Collection{ID: uuid.NewString(), Name: &item.name,
			DatabaseID: suite.databaseId, Tenant: tenant, IsDeleted: item.deleted,
			NumVersions: item.versions, VersionFileName: "version-file",
			OldestVersionTs: old, UpdatedAt: old.Add(time.Duration(i) * time.Hour)}
		suite.Require().NoError(tx.Create(&row).Error)
	}
	cutoff, limit, minVersions := uint64(time.Now().Unix()), uint64(4), uint64(6)
	for _, batchLimit := range []*uint64{&limit, nil} {
		rows, err := dao.ListCollectionsToGc(&cutoff, batchLimit, &tenant, &minVersions)
		suite.Require().NoError(err)
		names := make([]string, 0, len(rows))
		for _, row := range rows {
			names = append(names, row.Name)
		}
		suite.Equal([]string{"busy-deleted", "old-deleted", "live", "new-deleted"}, names)
	}
}
