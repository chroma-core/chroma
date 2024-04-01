package libs

import (
	"context"
	"github.com/jackc/pgx/v5"
)

func NewPgConnection(ctx context.Context, connectionString string) (conn *pgx.Conn, err error) {
	conn, err = pgx.Connect(context.Background(), connectionString)
	return
}
