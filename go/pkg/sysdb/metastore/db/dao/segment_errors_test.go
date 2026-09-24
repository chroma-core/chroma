package dao

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/chroma-core/chroma/go/pkg/types"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestGetSegmentsRowFailures(t *testing.T) {
	columns := []string{"id", "collection_id", "type", "scope", "file_paths", "key", "str_value", "int_value", "float_value", "bool_value"}
	collectionID := types.NewUniqueID()
	segmentID := types.NewUniqueID().String()
	for _, tc := range []struct {
		name       string
		validFirst bool
		iteration  bool
	}{
		{name: "first row scan fails"},
		{name: "later row scan fails", validFirst: true},
		{name: "first row iteration fails", iteration: true},
		{name: "later row iteration fails", validFirst: true, iteration: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sqlDB, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = sqlDB.Close() })
			db, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{})
			require.NoError(t, err)
			rows := sqlmock.NewRows(columns)
			if tc.validFirst {
				rows.AddRow(segmentID, collectionID.String(), "test", "VECTOR", "{}", nil, nil, nil, nil, nil)
			}
			// NULL cannot scan into the ID string. A failed first scan leaves
			// the current segment unset, as a canceled database read can do.
			if tc.validFirst && !tc.iteration {
				rows.AddRow(segmentID, collectionID.String(), "test", "VECTOR", "{}", "count", nil, "invalid integer", nil, nil)
			} else {
				rows.AddRow(nil, collectionID.String(), "test", "VECTOR", "{}", nil, nil, nil, nil, nil)
			}
			if tc.iteration {
				index := 0
				if tc.validFirst {
					index = 1
				}
				rows.RowError(index, context.Canceled)
			}
			mock.ExpectQuery("SELECT .* FROM \"segments\"").WithArgs(collectionID.String()).WillReturnRows(rows).RowsWillBeClosed()
			require.NotPanics(t, func() {
				segments, err := (&segmentDb{db: db}).GetSegments(types.NilUniqueID(), nil, nil, collectionID)
				require.Error(t, err)
				require.Nil(t, segments, "failed reads must not return partial segment metadata")
				if tc.iteration {
					require.ErrorIs(t, err, context.Canceled)
				} else if tc.validFirst {
					require.ErrorContains(t, err, "invalid syntax")
				} else {
					require.ErrorContains(t, err, "converting NULL to string")
				}
			})
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
