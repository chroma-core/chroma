package model

import "github.com/chroma-core/chroma/go/pkg/types"

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

type TenantLastCompactionTime struct {
	ID string
	Ts types.Timestamp
}
