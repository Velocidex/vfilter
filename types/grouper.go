package types

import (
	"context"

	"github.com/Velocidex/ordereddict"
)

// The GroupbyActor is passed to the grouper by the caller. The
// Grouper will then use it to create the result set. It is a way of
// delegating just the functionality required by the grouper to the
// query without exposing the internals of the query engine to the
// grouper.
type GroupbyActor interface {
	// Just receive new rows. Return EOF when no more rows exist. Returns
	// 1. The next row to group
	// 2. The group by bin index
	// 3. The scope over which the query is materialized.
	// 4. An error (Usually EOF if the stream is exhausted).
	//
	// Note that rows returned here are Lazy and are not
	// materialized. The grouper will materialize the row after
	// installing the correct bin context in the scope.
	GetNextRow(ctx context.Context, scope Scope) (LazyRow, string, Scope, error)

	// Materialize the row on the scope provided in the previous
	// call. The scope should contains the correct bin context
	// over which aggregate functions will be evaluated.
	MaterializeRow(ctx context.Context, row Row, scope Scope) *ordereddict.Dict
}

// A grouper receives rows and groups them into groups. Callers must
// provide a valid actor. Results are not sorted but the order is
// stable.
type Grouper interface {
	Group(ctx context.Context, scope Scope, actor GroupbyActor) <-chan Row
}
