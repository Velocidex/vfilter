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

	// Materialize both queries to an array.
	new_scope := scope.Copy()
	defer new_scope.Close()

	for item := range stored_query.Eval(ctx, new_scope) {
		result = append(result, item)
	}

	return result
}
