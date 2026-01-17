package chroma

import (
	"encoding/json"

	"github.com/go-viper/mapstructure/v2"
	"github.com/pkg/errors"
)

type Tenant interface {
	Name() string
	String() string
	Database(dbName string) Database
	Validate() error
}

type Database interface {
	ID() string
	Name() string
	Tenant() Tenant
	String() string
	Validate() error
}

type Include string

const (
	IncludeMetadatas  Include = "metadatas"
	IncludeDocuments  Include = "documents"
	IncludeEmbeddings Include = "embeddings"
	IncludeDistances  Include = "distances"
	IncludeURIs       Include = "uris"
)

type Identity struct {
	UserID    string   `json:"user_id"`
	Tenant    string   `json:"tenant"`
	Databases []string `json:"databases"`
}

type TenantBase struct {
	TenantName string `json:"name"`
}

func (t *TenantBase) Name() string {
	return t.TenantName
}

func NewTenant(name string) Tenant {
	return &TenantBase{TenantName: name}
}

func NewTenantFromJSON(jsonString string) (Tenant, error) {
	tenant := &TenantBase{}
	err := json.Unmarshal([]byte(jsonString), tenant)
	if err != nil {
		return nil, err
	}
	return tenant, nil
}
func (t *TenantBase) String() string {
	return t.Name()
}

func (t *TenantBase) Validate() error {
	if t.TenantName == "" {
		return errors.New("tenant name cannot be empty")
	}
	return nil
}

// Database returns a new Database object that can be used for creating collections
func (t *TenantBase) Database(dbName string) Database {
	return NewDatabase(dbName, t)
}

// TODO this may fail for v1 API
// func (t *TenantBase) MarshalJSON() ([]byte, error) {
//	return []byte(`"` + t.Name() + `"`), nil
//}

func NewDefaultTenant() Tenant {
	return NewTenant(DefaultTenant)
}

type DatabaseBase struct {
	DBName     string `json:"name" mapstructure:"name"`
	DBID       string `json:"id,omitempty" mapstructure:"id"`
	TenantName string `json:"tenant,omitempty" mapstructure:"tenant"`
	tenant     Tenant
}

func (d DatabaseBase) Name() string {
	return d.DBName
}

func (d DatabaseBase) Tenant() Tenant {
	if d.tenant == nil && d.TenantName != "" {
		d.tenant = NewTenant(d.TenantName)
	}
	return d.tenant
}

func (d DatabaseBase) String() string {
	return d.Name()
}

func (d DatabaseBase) ID() string {
	return d.DBID
}
func (d DatabaseBase) Validate() error {
	if d.DBName == "" {
		return errors.New("database name cannot be empty")
	}
	if d.tenant == nil {
		return errors.New("tenant cannot be empty")
	}
	return nil
}

// TODO this may fail for v1 API
// func (d *DatabaseBase) MarshalJSON() ([]byte, error) {
//	return []byte(`"` + d.Name() + `"`), nil
//}

func NewDatabase(name string, tenant Tenant) Database {
	return &DatabaseBase{DBName: name, tenant: tenant}
}

func NewDatabaseFromJSON(jsonString string) (Database, error) {
	database := &DatabaseBase{}
	err := json.Unmarshal([]byte(jsonString), database)
	if err != nil {
		return nil, err
	}
	if database.TenantName != "" {
		database.tenant = NewTenant(database.TenantName)
	} else {
		database.tenant = NewDefaultTenant()
	}
	return database, nil
}

func NewDatabaseFromMap(data map[string]interface{}) (Database, error) {
	database := &DatabaseBase{}
	err := mapstructure.Decode(data, database)
	if err != nil {
		return nil, errors.Wrap(err, "error decoding database")
	}
	if database.TenantName != "" {
		database.tenant = NewTenant(database.TenantName)
	} else {
		database.tenant = NewDefaultTenant()
	}
	return database, nil
}

func NewDefaultDatabase() Database {
	return NewDatabase(DefaultDatabase, NewDefaultTenant())
}
