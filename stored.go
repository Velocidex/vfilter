// A stored query encapsulates a VQL query which is yet to
// execute. Readers can request the query's channel and can read from
// it to drain its results.

// Stored queries implement the LET VQL directive. The LET keyword
// defines a stored query which is evaluated on demand. It looks just
// like a subselect but it is an efficient mechanism of passing the
// result of one query into another. Consider the following query:

// LET files = select * from glob(globs="/**") where Size < 100
// SELECT FullPath from files

// The LET keyword creates a stored query. This query does not
// immediately run until it is used as the subject of the second
// query. Most importantly, the second query does not need to wait for
// the stored query to completely produce its output. The first query
// can immediately feed rows to the second query for additional
// filtering. This leads to zero memory overhead as the rows do not
// need to be queued in memory.

package vfilter

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// A stored expression is stored in a LET clause either with or
// without parameters. e.g.:
// LET Y = SELECT * FROM plugin()
// LET Y(X) = SELECT * FROM plugin(foo=X)
type _StoredQuery struct {
	query      *_Select
	name       string
	parameters []string
}

func NewStoredQuery(query *_Select, name string) *_StoredQuery {
	return &_StoredQuery{
		query: query,
		name:  name,
	}
}

func (self *_StoredQuery) Eval(ctx context.Context, scope types.Scope) <-chan Row {
	new_scope := scope.Copy()
	return self.query.Eval(ctx, new_scope)
}

func (self *_StoredQuery) ToString(scope types.Scope) string {
	return self.query.ToString(scope)
}

func (self *_StoredQuery) GetParams() []string {
	return self.parameters
}

// Stored queries can also behave like plugins. This just means we
// evaluate it with a subscope built on top of the args.
func (self *_StoredQuery) Info(scope types.Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{}
}

func (self *_StoredQuery) Call(ctx context.Context,
	scope types.Scope, args *ordereddict.Dict) <-chan Row {
	self.checkCallingArgs(scope, args)

	sub_scope := scope.Copy()

	vars := ordereddict.NewDict()
	for _, k := range args.Keys() {
		v, _ := args.Get(k)
		switch t := v.(type) {
		case types.LazyExpr:
			v = t.Reduce(ctx)
		case types.StoredQuery:
			v = types.Materialize(ctx, scope, t)
		}
		vars.Set(k, v)
	}

	sub_scope.AppendVars(vars)

	return self.Eval(ctx, sub_scope)
}

func (self *_StoredQuery) checkCallingArgs(scope types.Scope, args *ordereddict.Dict) {
	// No parameters - do not warn
	if self.parameters == nil {
		return
	}

	// Check that all parameters are properly called.
	seen_map := make(map[string]bool)
	for _, k := range args.Keys() {
		if !utils.InString(&self.parameters, k) {
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

// A stored expression is stored in a LET clause either with or
// without parameters. e.g.:
// LET Y = count()
// LET Y(X) = format(format="Hello %v", args=[X])

// Unlike the LazyExpr the value of StoredExpression is not cached -
// this means each time it is evaluated, the expression is fully
// expanded. NOTE: The StoredExpression is evaluated at the point of
// reference not at the point of definition - therefore when
// evaluated, we must provide the scope at that point.
type StoredExpression struct {
	Expr       *_AndExpression
	name       string
	parameters []string
}

func (self *StoredExpression) ToString(scope types.Scope) string {
	return self.Expr.ToString(scope)
}

func (self *StoredExpression) GetParams() []string {
	return self.parameters
}

func (self *StoredExpression) Reduce(
	ctx context.Context, scope types.Scope) types.Any {
	return self.Expr.Reduce(ctx, scope)
}

func (self *StoredExpression) Call(ctx context.Context,
	scope types.Scope, args *ordereddict.Dict) types.Any {
	self.checkCallingArgs(scope, args)

	sub_scope := scope.Copy()
	defer sub_scope.Close()

	vars := ordereddict.NewDict()
	for _, k := range args.Keys() {
		v, _ := args.Get(k)
		switch t := v.(type) {
		case types.LazyExpr:
			v = t.Reduce(ctx)
		case types.StoredQuery:
			v = types.Materialize(ctx, scope, t)
		}
		vars.Set(k, v)
	}

	sub_scope.AppendVars(vars)

	return self.Reduce(ctx, sub_scope)
}

func (self *StoredExpression) checkCallingArgs(scope types.Scope, args *ordereddict.Dict) {
	// No parameters - do not warn
	if self.parameters == nil {
		return
	}

	// Check that all parameters are properly called.
	seen_map := make(map[string]bool)
	for _, k := range args.Keys() {
		if !utils.InString(&self.parameters, k) {
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

// A wrapper around a stored query which captures its call site's
// parameters in a new scope. When the wrapper is evaluated, the call
// site's scope will be used.
type StoredQueryCallSite struct {
	query StoredQuery
	scope Scope
}

func (self *StoredQueryCallSite) Eval(ctx context.Context, scope Scope) <-chan Row {
	// Use our embedded scope instead.
	return self.query.Eval(ctx, self.scope)
}
