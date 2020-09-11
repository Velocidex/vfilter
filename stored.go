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
	"reflect"

	"github.com/Velocidex/ordereddict"
)

// A plugin like object which takes no arguments but may be inserted
// into the scope to select from it.
type StoredQuery interface {
	Eval(ctx context.Context, scope *Scope) <-chan Row
	ToString(scope *Scope) string
}

type _StoredQuery struct {
	// Capture the scope at the point of definition. We will use
	// this scope when we run the query.
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

func (self *_StoredQuery) Eval(ctx context.Context, scope *Scope) <-chan Row {
	new_scope := scope.Copy()
	return self.query.Eval(ctx, new_scope)
}

func (self *_StoredQuery) ToString(scope *Scope) string {
	return self.query.ToString(scope)
}

// Stored queries can also behave like plugins. This just means we
// evaluate yet with a subscope built on top of the args.
func (self *_StoredQuery) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{}
}

func (self *_StoredQuery) Call(ctx context.Context,
	scope *Scope, args *ordereddict.Dict) <-chan Row {
	self.checkCallingArgs(scope, args)

	sub_scope := scope.Copy()

	vars := ordereddict.NewDict()
	for _, k := range args.Keys() {
		v, _ := args.Get(k)
		switch t := v.(type) {
		case LazyExpr:
			v = t.Reduce()
		case StoredQuery:
			v = Materialize(ctx, scope, t)
		}
		vars.Set(k, v)
	}

	sub_scope.AppendVars(vars)

	return self.Eval(ctx, sub_scope)
}

func (self *_StoredQuery) checkCallingArgs(scope *Scope, args *ordereddict.Dict) {
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

type _StoredQueryAssociative struct{}

func (self _StoredQueryAssociative) Applicable(a Any, b Any) bool {
	_, a_ok := a.(StoredQuery)
	return a_ok
}

func (self _StoredQueryAssociative) Associative(
	scope *Scope, a Any, b Any) (Any, bool) {

	var result []Any
	stored_query, ok := a.(StoredQuery)
	if ok {
		ctx := context.Background()
		new_scope := scope.Copy()
		from_chan := stored_query.Eval(ctx, new_scope)
		i := int64(0)
		int_b, b_is_int := to_int64(b)

		for row := range from_chan {
			// if b is an int then we are dereferencing a
			// stored query.
			if b_is_int && i == int_b {
				return row, true
			}

			item, pres := scope.Associative(row, b)
			if pres {
				result = append(result, item)
			}
			i++
		}
	}
	return result, true
}

func (self _StoredQueryAssociative) GetMembers(scope *Scope, a Any) []string {
	var result []string
	return result
}

type _StoredQueryBool struct{}

func (self _StoredQueryBool) Bool(scope *Scope, a Any) bool {
	stored_query, ok := a.(StoredQuery)
	if ok {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		new_scope := scope.Copy()
		from_chan := stored_query.Eval(ctx, new_scope)
		for {
			// As soon as a single result is returned we
			// can cancel the query.
			_, ok := <-from_chan
			if !ok {
				break
			}

			return true
		}
	}

	return false
}

func (self _StoredQueryBool) Applicable(a Any) bool {
	_, a_ok := a.(StoredQuery)
	return a_ok
}

type _StoredQueryAdd struct{}

func (self _StoredQueryAdd) Applicable(a Any, b Any) bool {
	_, a_ok := a.(StoredQuery)
	_, b_ok := b.(StoredQuery)
	return a_ok && b_ok
}

func (self _StoredQueryAdd) Add(scope *Scope, a Any, b Any) Any {
	ctx := context.Background()
	return append(Materialize(ctx, scope, a.(StoredQuery)),
		Materialize(ctx, scope, b.(StoredQuery))...)
}

// Wraps any object (e.g. a slice) into a StoredQuery object.
type StoredQueryWrapper struct {
	Delegate Any
}

func (self *StoredQueryWrapper) Eval(ctx context.Context, scope *Scope) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		delegate := self.Delegate

		lazy_arg, ok := delegate.(LazyExpr)
		if ok {
			delegate = lazy_arg.Reduce()
		}

		slice := reflect.ValueOf(delegate)
		if slice.Type().Kind() == reflect.Slice {
			for i := 0; i < slice.Len(); i++ {
				value := slice.Index(i).Interface()
				if !is_null_obj(value) {
					output_chan <- self.toRow(scope, value)
				}
			}
		} else {
			row_value := self.toRow(scope, self.Delegate)
			if !is_null_obj(row_value) {
				output_chan <- row_value
			}
		}
	}()
	return output_chan
}

func (self *StoredQueryWrapper) ToString(scope *Scope) string {
	stringer, ok := self.Delegate.(StringProtocol)
	if ok {
		return stringer.ToString(scope)
	}

	return ""
}

func (self *StoredQueryWrapper) toRow(scope *Scope, value Any) Row {
	if is_null_obj(value) {
		return Null{}
	}

	members := scope.GetMembers(value)
	if len(members) > 0 {
		return value
	}

	return ordereddict.NewDict().Set("_value", value)
}

func Materialize(ctx context.Context, scope *Scope, stored_query StoredQuery) []Row {
	result := []Row{}

	// Materialize both queries to an array.
	new_scope := scope.Copy()
	for item := range stored_query.Eval(ctx, new_scope) {
		result = append(result, item)
	}

	return result
}
