package chroma

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewPageDefaults(t *testing.T) {
	page := NewPage()
	require.Equal(t, 10, page.Size())
	require.Equal(t, 0, page.GetOffset())
	require.Equal(t, 0, page.Number())
}

func TestNewPageWithLimit(t *testing.T) {
	page := NewPage(Limit(20))
	require.Equal(t, 20, page.Size())
	require.Equal(t, 0, page.GetOffset())
}

func TestNewPageWithOffset(t *testing.T) {
	page := NewPage(Offset(40))
	require.Equal(t, 10, page.Size())
	require.Equal(t, 40, page.GetOffset())
}

func TestNewPageWithLimitAndOffset(t *testing.T) {
	page := NewPage(Limit(20), Offset(40))
	require.Equal(t, 20, page.Size())
	require.Equal(t, 40, page.GetOffset())
	require.Equal(t, 2, page.Number())
}

func TestLimitValidation(t *testing.T) {
	t.Run("zero limit returns error at apply time", func(t *testing.T) {
		page := NewPage(Limit(0))
		op := &CollectionGetOp{}
		err := page.ApplyToGet(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")
	})

	t.Run("negative limit returns error at apply time", func(t *testing.T) {
		page := NewPage(Limit(-1))
		req := &SearchRequest{}
		err := page.ApplyToSearchRequest(req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")
	})
}

func TestOffsetValidation(t *testing.T) {
	t.Run("negative offset returns error at apply time", func(t *testing.T) {
		page := NewPage(Offset(-1))
		op := &CollectionGetOp{}
		err := page.ApplyToGet(op)
		require.Error(t, err)
		require.Contains(t, err.Error(), "offset must be greater than or equal to 0")
	})

	t.Run("zero offset is allowed", func(t *testing.T) {
		page := NewPage(Offset(0))
		op := &CollectionGetOp{}
		err := page.ApplyToGet(op)
		require.NoError(t, err)
		require.Equal(t, 0, op.Offset)
	})
}

func TestPageNext(t *testing.T) {
	page := NewPage(Limit(20))
	require.Equal(t, 0, page.GetOffset())
	require.Equal(t, 0, page.Number())

	page = page.Next()
	require.Equal(t, 20, page.GetOffset())
	require.Equal(t, 1, page.Number())

	page = page.Next()
	require.Equal(t, 40, page.GetOffset())
	require.Equal(t, 2, page.Number())

	page = page.Next()
	require.Equal(t, 60, page.GetOffset())
	require.Equal(t, 3, page.Number())
}

func TestPageNextOverflowProtection(t *testing.T) {
	page := NewPage(Limit(20), Offset(math.MaxInt-10))
	next := page.Next()
	require.Equal(t, math.MaxInt, next.GetOffset(), "offset should be clamped to MaxInt on overflow")
	require.Equal(t, 20, next.Size(), "limit should be preserved")
}

func TestPagePrev(t *testing.T) {
	page := NewPage(Limit(20), Offset(60))
	require.Equal(t, 60, page.GetOffset())
	require.Equal(t, 3, page.Number())

	page = page.Prev()
	require.Equal(t, 40, page.GetOffset())
	require.Equal(t, 2, page.Number())

	page = page.Prev()
	require.Equal(t, 20, page.GetOffset())
	require.Equal(t, 1, page.Number())

	page = page.Prev()
	require.Equal(t, 0, page.GetOffset())
	require.Equal(t, 0, page.Number())
}

func TestPagePrevClampedToZero(t *testing.T) {
	page := NewPage(Limit(20))
	page = page.Prev()
	require.Equal(t, 0, page.GetOffset())

	page = NewPage(Limit(20), Offset(10))
	page = page.Prev()
	require.Equal(t, 0, page.GetOffset())
}

func TestPageNumber(t *testing.T) {
	t.Run("page 0", func(t *testing.T) {
		page := NewPage(Limit(20), Offset(0))
		require.Equal(t, 0, page.Number())
	})

	t.Run("page 1", func(t *testing.T) {
		page := NewPage(Limit(20), Offset(20))
		require.Equal(t, 1, page.Number())
	})

	t.Run("page 2", func(t *testing.T) {
		page := NewPage(Limit(20), Offset(40))
		require.Equal(t, 2, page.Number())
	})

	t.Run("non-aligned offset", func(t *testing.T) {
		page := NewPage(Limit(20), Offset(35))
		require.Equal(t, 1, page.Number())
	})
}

func TestPageApplyToGet(t *testing.T) {
	page := NewPage(Limit(20), Offset(40))

	op := &CollectionGetOp{}
	err := page.ApplyToGet(op)
	require.NoError(t, err)
	require.Equal(t, 20, op.Limit)
	require.Equal(t, 40, op.Offset)
}

func TestPageApplyToSearchRequest(t *testing.T) {
	page := NewPage(Limit(20), Offset(40))

	req := &SearchRequest{}
	err := page.ApplyToSearchRequest(req)
	require.NoError(t, err)
	require.NotNil(t, req.Limit)
	require.Equal(t, 20, req.Limit.Limit)
	require.Equal(t, 40, req.Limit.Offset)
}

func TestPageApplyToSearchRequestPreservesExistingLimit(t *testing.T) {
	page := NewPage(Limit(20), Offset(40))

	req := &SearchRequest{
		Limit: &SearchPage{Limit: 10, Offset: 0},
	}
	err := page.ApplyToSearchRequest(req)
	require.NoError(t, err)
	require.Equal(t, 20, req.Limit.Limit)
	require.Equal(t, 40, req.Limit.Offset)
}

func TestPageImmutability(t *testing.T) {
	original := NewPage(Limit(20), Offset(40))
	originalOffset := original.GetOffset()
	originalSize := original.Size()

	next := original.Next()
	require.Equal(t, originalOffset, original.GetOffset(), "original should not change after Next()")
	require.Equal(t, originalSize, original.Size(), "original size should not change after Next()")
	require.NotEqual(t, original.GetOffset(), next.GetOffset())

	prev := original.Prev()
	require.Equal(t, originalOffset, original.GetOffset(), "original should not change after Prev()")
	require.Equal(t, originalSize, original.Size(), "original size should not change after Prev()")
	require.NotEqual(t, original.GetOffset(), prev.GetOffset())
}

func TestPageInNewCollectionGetOp(t *testing.T) {
	page := NewPage(Limit(25), Offset(50))

	op, err := NewCollectionGetOp(
		WithIDs("id1", "id2"),
		page,
	)
	require.NoError(t, err)
	require.Equal(t, []DocumentID{"id1", "id2"}, op.Ids)
	require.Equal(t, 25, op.Limit)
	require.Equal(t, 50, op.Offset)
}

func TestPagePreservesLimitAfterNavigation(t *testing.T) {
	page := NewPage(Limit(25))
	require.Equal(t, 25, page.Size())

	for i := 0; i < 5; i++ {
		page = page.Next()
		require.Equal(t, 25, page.Size(), "limit should be preserved after Next()")
	}

	for i := 0; i < 3; i++ {
		page = page.Prev()
		require.Equal(t, 25, page.Size(), "limit should be preserved after Prev()")
	}
}

func TestPageInlineUsage(t *testing.T) {
	t.Run("inline in NewSearchRequest", func(t *testing.T) {
		req := &SearchRequest{}
		err := NewPage(Limit(20)).ApplyToSearchRequest(req)
		require.NoError(t, err)
		require.Equal(t, 20, req.Limit.Limit)
	})

	t.Run("inline in NewCollectionGetOp", func(t *testing.T) {
		op, err := NewCollectionGetOp(
			WithIDs("id1"),
			NewPage(Limit(50), Offset(100)),
		)
		require.NoError(t, err)
		require.Equal(t, 50, op.Limit)
		require.Equal(t, 100, op.Offset)
	})

	t.Run("invalid inline returns error at apply time", func(t *testing.T) {
		_, err := NewCollectionGetOp(
			WithIDs("id1"),
			NewPage(Limit(-1)),
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")
	})
}

func TestPageValidate(t *testing.T) {
	t.Run("valid page passes validation", func(t *testing.T) {
		page := NewPage(Limit(20), Offset(40))
		require.NoError(t, page.Validate())
	})

	t.Run("zero limit fails validation", func(t *testing.T) {
		page := NewPage(Limit(0))
		err := page.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")
	})

	t.Run("negative limit fails validation", func(t *testing.T) {
		page := NewPage(Limit(-5))
		err := page.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "limit must be greater than 0")
	})

	t.Run("negative offset fails validation", func(t *testing.T) {
		page := NewPage(Offset(-10))
		err := page.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "offset must be greater than or equal to 0")
	})

	t.Run("default page passes validation", func(t *testing.T) {
		page := NewPage()
		require.NoError(t, page.Validate())
	})
}
