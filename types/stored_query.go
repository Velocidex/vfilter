package types

import (
	"context"
)

// A plugin like object which takes no arguments but may be inserted
// into the scope to select from it.
type StoredQuery interface {
	Eval(ctx context.Context, scope Scope) <-chan Row
}

// Materialize a stored query into a set of rows.
func Materialize(ctx context.Context, scope Scope, stored_query StoredQuery) []Row {
	result := []Row{}
	var warned bool

	// Materialize both queries to an array.
	new_scope := scope.Copy()
	defer new_scope.Close()

	for item := range stored_query.Eval(ctx, new_scope) {
		result = append(result, item)

		if !warned && len(result) > 10000 {
			scope.Log("WARN:During Materialize of StoredQuery %s: Expand larger than 10,000 rows!",
				ToString(ctx, scope, stored_query))
			warned = true
		}
	}

	return result
}
