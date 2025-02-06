package types

import (
	"context"
)

// A FrozenStoredQuery is a stored query which will be evaluated
// inside the defined scope instead of the calling scope.
type FrozenStoredQuery struct {
	query         StoredQuery
	defined_scope Scope
}

func (self FrozenStoredQuery) Query() StoredQuery {
	return self.query
}

func (self FrozenStoredQuery) Eval(
	ctx context.Context, scope Scope) <-chan Row {
	return self.query.Eval(ctx, self.defined_scope)
}

func NewFrozenStoredQuery(
	query StoredQuery, scope Scope) StoredQuery {
	return &FrozenStoredQuery{query: query, defined_scope: scope}
}
