package protocols

import (
	"context"
	"reflect"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

type _SliceIterator struct{}

func (self _SliceIterator) Applicable(a types.Any) bool {
	a_value := reflect.Indirect(reflect.ValueOf(a))
	a_type := a_value.Type()
	return a_type.Kind() == reflect.Slice
}

func (self _SliceIterator) Iterate(ctx context.Context, scope types.Scope, a types.Any) <-chan types.Row {
	output_chan := make(chan types.Row)

	go func() {
		defer close(output_chan)

		a_value := reflect.Indirect(reflect.ValueOf(a))
		if a_value.Type().Kind() == reflect.Slice {
			for i := 0; i < a_value.Len(); i++ {
				value := a_value.Index(i).Interface()
				if is_null(value) {
					continue
				}

				_, ok := value.(*ordereddict.Dict)
				if ok {
					output_chan <- value
				} else {
					output_chan <- ordereddict.NewDict().
						Set("_value", value)
				}
			}
		}

	}()

	return output_chan
}

/*
// Iterating on a LazyExpr means to iterate on its reduced value.
type _LazyExprIterator struct{}

func (self _LazyExprIterator) Applicable(a types.Any) bool {
	_, ok := a.(types.LazyExpr)
	return ok
}

func (self _LazyExprIterator) Iterate(ctx context.Context, scope types.Scope, a types.Any) <-chan types.Row {
	lazy, ok := a.(types.LazyExpr)
	if !ok {
		output_chan := make(chan types.Row)
		close(output_chan)
		return output_chan
	}

	// Expand the lazy expression and cache the results.
	if lazy.Value == nil {
		if lazy.Expr == nil {
			lazy.Value = &Null{}
		} else {
			lazy.Value = lazy.Expr.Reduce(ctx, scope)
		}
	}

	return scope.Iterate(ctx, lazy.Value)
}

type _StoredQueryIterator struct{}

func (self _StoredQueryIterator) Applicable(a types.Any) bool {
	_, ok := a.(StoredQuery)
	return ok
}

func (self _StoredQueryIterator) Iterate(ctx context.Context, scope types.Scope, a types.Any) <-chan types.Row {
	stored_query, ok := a.(StoredQuery)
	if !ok {
		output_chan := make(chan types.Row)
		close(output_chan)
		return output_chan
	}

	return stored_query.Eval(ctx, scope)
}


// Iterating over a single dict just produces that same dict.
type _DictIterator struct{}

func (self _DictIterator) Applicable(a types.Any) bool {
	_, ok := a.(*ordereddict.Dict)

	return ok
}

func (self _DictIterator) Iterate(ctx context.Context, scope types.Scope, a types.Any) <-chan types.Row {
	output_chan := make(chan types.Row)

	go func() {
		defer close(output_chan)

		dict, ok := a.(*ordereddict.Dict)
		if ok {
			output_chan <- dict
		}
	}()
	return output_chan
}

*/
