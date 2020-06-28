package search

import (
	"testing"
	"time"

	"github.com/nick-jones/piscola/internal/gen-go/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatabase_Add(t *testing.T) {
	item := &service.Item{
		ID:    1,
		Time:  int32(time.Now().Second()),
		Value: "test",
	}

	db := newDatabase()
	assert.True(t, db.Add(item))
	assert.False(t, db.Add(item))
	assert.Equal(t, 1, db.Size())
}

func TestDatabase_Remove(t *testing.T) {
	item := &service.Item{
		ID:    1,
		Time:  int32(time.Now().Second()),
		Value: "test",
	}

	db := newDatabase()
	db.Add(item)
	assert.True(t, db.Remove(item.ID))
	assert.False(t, db.Remove(item.ID))
	assert.Equal(t, 0, db.Size())
	res, err := db.Lookup(&service.Query{Pattern: "*", Limit: 1})
	require.NoError(t, err)
	assert.Len(t, res.Ids, 0)
}

func TestDatabase_Put(t *testing.T) {
	item := &service.Item{
		ID:    1,
		Time:  int32(time.Now().Second()),
		Value: "test",
	}
	repl := &service.Item{
		ID:    1,
		Time:  int32(time.Now().Second()),
		Value: "test-2",
	}
	other := &service.Item{
		ID:    2,
		Time:  int32(time.Now().Second()),
		Value: "test",
	}

	db := newDatabase()
	db.Add(item)
	assert.True(t, db.Put(repl))
	assert.Equal(t, 1, db.Size())
	assert.True(t, db.Put(other))
	assert.Equal(t, 2, db.Size())
}

var fixtures = []*service.Item{
	{
		ID:    1,
		Time:  1001,
		Value: "test",
	},
	{
		ID:    2,
		Time:  1000,
		Value: "testing",
		Attributes: service.Attrs{"foo": true},
	},
	{
		ID:    3,
		Time:  1000,
		Value: "tester",
	},
	{
		ID:      4,
		Time:    1000,
		Value:   "tester-deleted",
		Deleted: true,
	},
	{
		ID:    5,
		Time:  1000,
		Value: "other",
	},
}

func TestDatabase_Lookup(t *testing.T) {
	db := newDatabase()

	for _, item := range fixtures {
		db.Add(item)
	}

	// basic search
	expected := &service.Result_{
		Ids:   []int32{1, 2, 3},
		Total: 3,
	}
	actual, err := db.Lookup(&service.Query{
		Pattern:       "*test*",
		Limit:         10,
		TotalRequired: true,
	})
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	// with limit 2 and total required
	expected = &service.Result_{
		Ids:   []int32{1, 2},
		Total: 3,
	}
	actual, err = db.Lookup(&service.Query{
		Pattern:       "*test*",
		Limit:         2,
		TotalRequired: true,
	})
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	// with limit 2 and no total required
	expected = &service.Result_{
		Ids:   []int32{1, 2},
		Total: 2,
	}
	actual, err = db.Lookup(&service.Query{
		Pattern:       "*test*",
		Limit:         2,
		TotalRequired: false,
	})
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}

func TestDatabase_LookupAdvanced(t *testing.T) {
	db := newDatabase()

	for _, item := range fixtures {
		db.Add(item)
	}

	// basic search
	expected := &service.Result_{
		Ids:   []int32{1, 2},
		Total: 2,
	}
	actual, err := db.LookupAdvanced(&service.AdvancedQuery{
		IncludePatterns: []string{"*est*", "t*"},
		ExcludePatterns: []string{"*er"},
		Limit:           10,
		TotalRequired:   true,
	})
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	// include deleted
	expected = &service.Result_{
		Ids:   []int32{1, 2, 4},
		Total: 3,
	}
	actual, err = db.LookupAdvanced(&service.AdvancedQuery{
		IncludePatterns: []string{"*est*", "t*"},
		ExcludePatterns: []string{"*er"},
		Limit:           10,
		TotalRequired:   true,
		IncludeDeleted:  true,
	})
	require.NoError(t, err)
	assert.Equal(t, expected, actual)

	// attributes specified
	expected = &service.Result_{
		Ids:   []int32{2},
		Total: 1,
	}
	actual, err = db.LookupAdvanced(&service.AdvancedQuery{
		IncludePatterns: []string{"*test*"},
		Limit:           10,
		WithAttributes: service.Attrs{"foo": true},
	})
	require.NoError(t, err)
	assert.Equal(t, expected, actual)
}
