package model

import "github.com/chroma/chroma-coordinator/internal/types"

type Tenant struct {
	Name string
}

type CreateTenant struct {
	Name string
	Ts   types.Timestamp
}

type GetTenant struct {
	Name string
	Ts   types.Timestamp
}
