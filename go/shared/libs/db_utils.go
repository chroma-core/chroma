package libs

import (
	"context"

	"github.com/chroma-core/chroma/go/pkg/log/configuration"
	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPgConnection(ctx context.Context, config *configuration.LogServiceConfiguration) (conn *pgxpool.Pool, err error) {
	var conf *pgxpool.Config
	conf, err = pgxpool.ParseConfig(config.DATABASE_URL)
	if err != nil {
	    return
	}
	conf.MaxConns = config.MAX_CONNS
	conn, err = pgxpool.NewWithConfig(ctx, conf)
	return
}
