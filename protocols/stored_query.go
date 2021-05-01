package protocols

import (
	"context"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

type _StoredQueryAssociative struct{}

func (self _StoredQueryAssociative) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := a.(types.StoredQuery)
	return a_ok
}

func (self _StoredQueryAssociative) Associative(
	scope types.Scope, a types.Any, b types.Any) (types.Any, bool) {

	var result []types.Any
	stored_query, ok := a.(types.StoredQuery)
	if ok {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		new_scope := scope.Copy()
		defer new_scope.Close()

		from_chan := stored_query.Eval(ctx, new_scope)
		i := int64(0)
		int_b, b_is_int := utils.ToInt64(b)

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

func (self _StoredQueryAssociative) GetMembers(scope types.Scope, a types.Any) []string {
	var result []string
	return result
}

type _StoredQueryBool struct{}

func (self _StoredQueryBool) Bool(scope types.Scope, a types.Any) bool {
	stored_query, ok := a.(types.StoredQuery)
	if ok {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		new_scope := scope.Copy()
		defer new_scope.Close()

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

func (self _StoredQueryBool) Applicable(a types.Any) bool {
	_, a_ok := a.(types.StoredQuery)
	return a_ok
}

type _StoredQueryAdd struct{}

func (self _StoredQueryAdd) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := a.(types.StoredQuery)
	_, b_ok := b.(types.StoredQuery)
	return a_ok && b_ok
}

func (self _StoredQueryAdd) Add(scope types.Scope, a types.Any, b types.Any) types.Any {
	// FIXME - this breaks cancellations.
	ctx := context.Background()
	return append(MaterializeToArray(ctx, scope, a.(types.StoredQuery)),
		MaterializeToArray(ctx, scope, b.(types.StoredQuery))...)
}

func MaterializeToArray(ctx context.Context, scope types.Scope,
	stored_query types.StoredQuery) []types.Row {
	result := []types.Row{}

	// Materialize both queries to an array.
	new_scope := scope.Copy()
	defer new_scope.Close()

	for item := range stored_query.Eval(ctx, new_scope) {
		result = append(result, item)
	}

	return result
}
