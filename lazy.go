// A lazy row implementation.

package vfilter

import (
	"context"
	"sync"
)

type LazyRow struct {
	ctx     context.Context
	getters map[string]func(ctx context.Context, scope *Scope) Any

	// We need to maintain the order in which columns are added to
	// preserve column ordering.
	columns []string
	cache   *Dict

	mu sync.Mutex
}

func (self *LazyRow) AddColumn(
	name string, getter func(ctx context.Context, scope *Scope) Any) {
	self.getters[name] = getter
	self.columns = append(self.columns, name)
}

func NewLazyRow(ctx context.Context) *LazyRow {
	return &LazyRow{
		ctx:     ctx,
		getters: make(map[string]func(ctx context.Context, scope *Scope) Any),
		cache:   NewDict(),
	}
}

// Implement associative protocol.

type _LazyRowAssociative struct{}

func (self _LazyRowAssociative) Applicable(a Any, b Any) bool {
	switch a.(type) {
	case LazyRow, *LazyRow:
		break
	default:
		return false
	}

	switch b.(type) {
	case string:
		break
	default:
		return false
	}

	return true
}

// Associate object a with key b
func (self _LazyRowAssociative) Associative(scope *Scope, a Any, b Any) (Any, bool) {
	key := b.(string)
	var lazy_row *LazyRow

	switch t := a.(type) {
	case LazyRow:
		lazy_row = &t

	case *LazyRow:
		lazy_row = t

	default:
		return nil, false
	}

	res, pres := lazy_row.cache.Get(key)
	if pres {
		return res, true
	}

	// Not in cache, we need to get it.
	getter, pres := lazy_row.getters[key]
	if !pres {
		return Null{}, false
	}

	res = getter(lazy_row.ctx, scope)
	lazy_row.cache.Set(key, res)
	return res, true
}

func (self _LazyRowAssociative) GetMembers(scope *Scope, a Any) []string {
	var value *LazyRow
	switch t := a.(type) {
	case LazyRow:
		value = &t

	case *LazyRow:
		value = t

	default:
		return []string{}
	}

	return value.columns
}

func MaterializedLazyRow(row Row, scope *Scope) Row {
	lazy_row, ok := row.(*LazyRow)
	if ok {
		result := NewDict()
		// Preserve column ordering.
		for _, column := range lazy_row.columns {
			value, pres := lazy_row.cache.Get(column)
			if !pres {
				getter, _ := lazy_row.getters[column]
				value = getter(lazy_row.ctx, scope)
			}

			result.Set(column, value)
		}

		return result
	}
	return row
}
