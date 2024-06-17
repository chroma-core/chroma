package model

import "github.com/chroma-core/chroma/go/pkg/types"

type Database struct {
	ID     string
	Name   string
	Tenant string
	Ts     types.Timestamp
}

type CreateDatabase struct {
	ID     string
	Name   string
	Tenant string
	Ts     types.Timestamp
}

type GetDatabase struct {
	ID     string
	Name   string
	Tenant string
	Ts     types.Timestamp
}
