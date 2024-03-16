package dao

import (
	"context"
	"github.com/chroma-core/chroma/go/pkg/coordinator/ent"
)

type TestBase struct {
	Dpo *ent.TestBase
}

type MSetTestBase interface {
	SetText(string)
	Text() (string, bool)
}

func SetTestBase(m MSetTestBase, testBase *ent.TestBase) {
	if _, exist := m.Text(); !exist {
		if testBase.Text != nil {
			m.SetText(*testBase.Text)
		}
	}
}

func Create(ctx context.Context, tx *ent.Tx, dpo *ent.TestBase) (*TestBase, error) {
	testBase := &TestBase{
		Dpo: dpo,
	}
	dpo, err := testBase.create(ctx, tx)
	if err != nil {
		return nil, err
	}
	testBase.Dpo = dpo
	return testBase, nil
}

func (t *TestBase) create(ctx context.Context, tx *ent.Tx) (*ent.TestBase, error) {
	creator := tx.TestBase.Create()
	err := SetBase(creator.Mutation(), t.Dpo)
	if err != nil {
		return nil, err
	}
	SetTestBase(creator.Mutation(), t.Dpo)
	testBase, err := creator.Save(ctx)
	if err != nil {
		return nil, err
	}
	return testBase, nil
}
