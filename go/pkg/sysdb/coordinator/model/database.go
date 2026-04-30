package model

import "github.com/chroma-core/chroma/go/pkg/types"

type Database struct {
	ID       string
	Name     string
	Tenant   string
	Ts       types.Timestamp
	Metadata *CollectionMetadata[CollectionMetadataValueType]
}

type CreateDatabase struct {
	ID       string
	Name     string
	Tenant   string
	Ts       types.Timestamp
	Metadata *CollectionMetadata[CollectionMetadataValueType]
}

type GetDatabase struct {
	ID     string
	Name   string
	Tenant string
	Ts     types.Timestamp
}

type ListDatabases struct {
	Limit  *int32
	Offset *int32
	Tenant string
	Ts     types.Timestamp
}

type DeleteDatabase struct {
	ID     string
	Name   string
	Tenant string
	Ts     types.Timestamp
}
