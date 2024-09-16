package libs

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/log/configuration"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPgConnection(ctx context.Context, config *configuration.LogServiceConfiguration) (conn *pgxpool.Pool, err error) {
	conn, err = pgxpool.New(ctx, config.DATABASE_URL)
	return
}
