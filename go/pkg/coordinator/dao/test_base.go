package dao

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/coordinator/ent"
)

type TestBase struct {
	Text string
}

func (*TestBase) Create(ctx context.Context, tx *ent.Tx, testBase *ent.TestBase) (*ent.TestBase, error) {
	testBase, err := tx.TestBase.Create().Set
	if err != nil {
		return nil, err
	}
	return testBase, nil
}
