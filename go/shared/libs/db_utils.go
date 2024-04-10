package libs

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/log/configuration"
	"github.com/jackc/pgx/v5"
)

func NewPgConnection(ctx context.Context, config *configuration.LogServiceConfiguration) (conn *pgx.Conn, err error) {
	conn, err = pgx.Connect(ctx, config.DATABASE_URL)
	return
}
