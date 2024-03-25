package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

// TestBase holds the schema definition for the TestBase entity.
type TestBase struct {
	ent.Schema
}

func (TestBase) Mixin() []ent.Mixin {
	return []ent.Mixin{
		Base{},
	}
}

// Fields of the TestBase.
func (TestBase) Fields() []ent.Field {
	return []ent.Field{
		field.String("text").MaxLen(255).Optional().Nillable(),
	}
}

// Edges of the TestBase.
func (TestBase) Edges() []ent.Edge {
	return nil
}
