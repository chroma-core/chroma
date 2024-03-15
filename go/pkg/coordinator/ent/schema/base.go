package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"github.com/google/uuid"
	"time"
)

// Base holds the schema definition for the Base entity.
type Base struct {
	ent.Schema
}

// Fields of the Base.
func (Base) Fields() []ent.Field {
	return []ent.Field{
		field.UUID("parent_id", uuid.UUID{}).Immutable(),
		field.UUID("id", uuid.UUID{}).Default(uuid.New).Unique().Immutable(),
		field.String("name").MaxLen(255).Optional(),
		field.Uint("created_at").DefaultFunc(func() uint {
			return uint(time.Now().Unix())
		}),
		field.Uint("updated_at").DefaultFunc(func() uint {
			return uint(time.Now().Unix())
		}),
		field.Uint("deleted_at").Default(0),
		field.Int("version").Default(0),
	}
}

// Edges of the Base.
func (Base) Edges() []ent.Edge {
	return nil
}
