package types

import (
	"context"
)

// A Lazy row holds column values without evaluating them. We call the
// act of evaluating a column, we materialize the column into a proper
// type.
type LazyRow interface {
	// Add a lazy evaluator to the column.
	AddColumn(name string, getter func(ctx context.Context, scope Scope) Any) LazyRow

	// Materialize the value at a column
	Get(name string) (Any, bool)

	// Return all the columns
	Columns() []string
}

// When types implement a lazy interface we need to know all their
// columns. The Memberer interface allows the type to tell us all its
// members. This is a convenience to having to implement the
// GetMembers() protocol.
type Memberer interface {
	Members() []string
}

type Materializer interface {
	Materialize(ctx context.Context, scope Scope) Any
}

// A LazyExpr has a reduce method that allows it to be materialized.
type LazyExpr interface {
	// Reduce with the scope captured at point of definition.
	Reduce(ctx context.Context) Any

	// Reduce with a new scope.
	ReduceWithScope(ctx context.Context, scope Scope) Any
}

type StoredExpression interface {
	Reduce(ctx context.Context, scope Scope) Any
}
