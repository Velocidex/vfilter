// A lazy row implementation.

package vfilter

import (
	"context"
	"sync"

	"github.com/Velocidex/ordereddict"
)

// FIXME: Can this be refactored to use ordereddict?

// A LazyRow holds callbacks as columns. When a column is accessed,
// the LazyRow will call the callback to materialize it, then cache
// the results.  LazyRows are used to avoid calling expensive
// functions when the query does not need them - LazyRows are created
// in the SELECT transformer to delay evaluation of column specifiers
// until the are accessed.
type LazyRow struct {
	ctx     context.Context
	getters map[string]func(ctx context.Context, scope *Scope) Any

	// We need to maintain the order in which columns are added to
	// preserve column ordering.
	columns []string
	cache   *ordereddict.Dict

	closer []func()

	mu sync.Mutex
}

func (self *LazyRow) AddColumn(
	name string, getter func(ctx context.Context, scope *Scope) Any) *LazyRow {
	self.getters[name] = getter
	self.columns = append(self.columns, name)
	return self
}

func NewLazyRow(ctx context.Context) *LazyRow {
	return &LazyRow{
		ctx:     ctx,
		getters: make(map[string]func(ctx context.Context, scope *Scope) Any),
		cache:   ordereddict.NewDict(),
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

// Takes a row returned from a plugin and materialize it into basic
// types. Generally this should only be LazyRow as this is only called
// from the Transformer.  NOTE: This function only materialized the
// columns - it does not recursively materialize all objects.
func MaterializedLazyRow(ctx context.Context, row Row, scope *Scope) *ordereddict.Dict {
	// If it is already materialized, just return what we have.
	switch t := row.(type) {
	case *ordereddict.Dict:
		return t

	case *LazyRow:
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
		return RowToDict(ctx, scope, row)
	}
}

type LazyExpr struct {
	Value      Any
	Expr       *_AndExpression
	name       string
	ctx        context.Context
	parameters []string
	scope      *Scope
}

func (self *LazyExpr) ReduceWithScope(scope *Scope) Any {
	self.Value = nil
	self.scope = scope
	return self.Reduce()
}

func (self *LazyExpr) Reduce() Any {
	if self.Value == nil {
		if self.Expr == nil {
			self.Value = &Null{}
		} else {
			self.Value = self.Expr.Reduce(self.ctx, self.scope.Copy())
		}
	}

	switch t := self.Value.(type) {
	case StoredQuery:
		self.Value = Materialize(self.ctx, self.scope.Copy(), t)

	case LazyExpr:
		self.Value = t.Reduce()
	}

	return self.Value
}

// LazyExpr behaves like a function - calling it will just reduce it
// with the subscope.
func (self LazyExpr) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{}
}

func (self LazyExpr) Call(ctx context.Context, scope *Scope, args *ordereddict.Dict) Any {
	// Check the call is correct.
	self.checkCallingArgs(scope, args)

	// Create a sub scope to call the function.
	sub_scope := self.scope.Copy()
	sub_scope.AppendVars(args)

	callee := LazyExpr{
		Value:      nil, // Force calling the expression and not cache.
		Expr:       self.Expr,
		parameters: self.parameters,
		ctx:        self.ctx,
		name:       self.name,
		scope:      sub_scope,
	}
	return callee.Reduce()
}

// Convert the expression to a stored query without materializing
// it. If the expression is not already a query, we wrap it in a
// stored query wrapper so the caller receives a stored query.
func (self *LazyExpr) ToStoredQuery(scope *Scope) StoredQuery {
	if self.Value == nil {
		if self.Expr == nil {
			self.Value = &Null{}
		} else {
			self.Value = self.Expr.Reduce(self.ctx, scope)
		}
	}

	stored_query, ok := self.Value.(StoredQuery)
	if ok {
		return stored_query
	}

	return &StoredQueryWrapper{self.Value}
}

func (self *LazyExpr) checkCallingArgs(scope *Scope, args *ordereddict.Dict) {
	// No parameters - do not warn
	if self.parameters == nil {
		return
	}

	// Check that all parameters are properly called.
	seen_map := make(map[string]bool)
	for _, k := range args.Keys() {
		if !InString(&self.parameters, k) {
			scope.Log("Extra unrecognized arg %v when calling %v",
				k, self.name)
		}
		seen_map[k] = true
	}

	// Some args are missing.
	if len(seen_map) < len(self.parameters) {
		for _, k := range self.parameters {
			_, pres := seen_map[k]
			if !pres {
				scope.Log("Missing arg %v when calling %v",
					k, self.name)
			}
		}
	}
}
