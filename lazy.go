// A lazy row implementation.

package vfilter

import (
	"context"
	"sync"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils/dict"
)

// FIXME: Can this be refactored to use ordereddict?

// A LazyRow holds callbacks as columns. When a column is accessed,
// the LazyRow will call the callback to materialize it, then cache
// the results.  LazyRows are used to avoid calling expensive
// functions when the query does not need them - LazyRows are created
// in the SELECT transformer to delay evaluation of column specifiers
// until they are accessed.
type LazyRowImpl struct {
	// The scope over which the lazy row is evaluated
	ctx   context.Context
	scope types.Scope

	getters map[string]func(ctx context.Context, scope types.Scope) types.Any

	// We need to maintain the order in which columns are added to
	// preserve column ordering.
	columns []string
	cache   *ordereddict.Dict

	closer []func()

	mu sync.Mutex
}

func (self *LazyRowImpl) AddColumn(
	name string, getter func(ctx context.Context, scope types.Scope) types.Any) types.LazyRow {
	self.getters[name] = getter
	self.columns = append(self.columns, name)
	return self
}

func (self *LazyRowImpl) Has(key string) bool {
	_, pres := self.cache.Get(key)
	if pres {
		return true
	}

	_, pres = self.getters[key]
	if pres {
		return true
	}

	return false
}

func (self *LazyRowImpl) Get(key string) (types.Any, bool) {
	res, pres := self.cache.Get(key)
	if pres {
		return res, true
	}

	// Not in cache, we need to get it.
	getter, pres := self.getters[key]
	if !pres {
		return Null{}, false
	}

	res = getter(self.ctx, self.scope)
	self.cache.Set(key, res)

	return res, true
}

func (self *LazyRowImpl) Columns() []string {
	return self.columns
}

func NewLazyRow(ctx context.Context, scope types.Scope) *LazyRowImpl {
	return &LazyRowImpl{
		ctx:     ctx,
		scope:   scope,
		getters: make(map[string]func(ctx context.Context, scope types.Scope) types.Any),
		cache:   ordereddict.NewDict(),
	}
}

// Takes a row returned from a plugin and materialize it into basic
// types. Generally this should only be LazyRow as this is only called
// from the Transformer.  NOTE: This function only materialized the
// columns - it does not recursively materialize all objects.
func MaterializedLazyRow(ctx context.Context, row Row, scope types.Scope) *ordereddict.Dict {
	// If it is already materialized, just return what we have.
	switch t := row.(type) {
	case *ordereddict.Dict:
		return t

	case *LazyRowImpl:
		result := ordereddict.NewDict()
		// Preserve column ordering.
		for _, column := range t.columns {
			value, pres := t.cache.Get(column)
			if !pres {
				getter, _ := t.getters[column]
				value = getter(ctx, scope)
			}

			result.Set(column, value)
		}
		return result

	default:
		return dict.RowToDict(ctx, scope, row)
	}
}

// A LazyExpr may be passed into a plugin arg for later
// evaluation. The plugin may completely ignore the expression and so
// will not evaluate it at all. Once evaluated LazyExpr will cache the
// value and can be used again. NOTE that LazyExpr is used purely for
// caching and so it uses the local scope (at the point of definition)
// to evaluate the expression - not the scope at the point of
// reference!
type LazyExprImpl struct {
	Value types.Any // Used to cache
	Expr  *_AndExpression
	ctx   context.Context
	scope types.Scope
}

func NewLazyExpr(ctx context.Context,
	scope types.Scope, expr *_AndExpression) types.LazyExpr {
	return &LazyExprImpl{
		Expr:  expr,
		ctx:   ctx,
		scope: scope,
	}
}

func (self *LazyExprImpl) ReduceWithScope(
	ctx context.Context, scope types.Scope) types.Any {
	var result types.Any
	if self.Expr == nil {
		result = &Null{}
	} else {
		result = self.Expr.Reduce(self.ctx, self.scope)
	}

	switch t := result.(type) {

	case types.Materializer:
		return t.Materialize(ctx, scope)

	// StoredQuery objects are first class objects that can be
	// passed around into function args.
	case StoredQuery:
		result = t

	case func() types.Any:
		result = t()

	case types.LazyExpr:
		result = t.Reduce(ctx)
	}

	return result
}

func (self *LazyExprImpl) Reduce(ctx context.Context) types.Any {
	if self.Value != nil {
		return self.Value
	}
	self.Value = self.ReduceWithScope(ctx, self.scope)
	return self.Value
}
