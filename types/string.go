package types

import (
	"context"
	"fmt"
)

type StringProtocol interface {
	ToString(scope Scope) string
}

// Try very hard to convert to a string
func ToString(ctx context.Context,
	scope Scope, x interface{}) string {
	switch t := x.(type) {
	case fmt.Stringer:
		return t.String()

		// Reduce any LazyExpr to materialized types
	case LazyExpr:
		return ToString(ctx, scope, t.Reduce(ctx))

	case Materializer:
		return ToString(ctx, scope, t.Materialize(ctx, scope))

		// Materialize stored queries into an array.
	case StoredQuery:
		return ToString(ctx, scope, Materialize(ctx, scope, t))

		// A dict may expose a callable as a member - we just
		// call it lazily if it is here.
	case func() Any:
		return ToString(ctx, scope, t())

	case StringProtocol:
		return t.ToString(scope)

	case string:
		return t

	case *string:
		return *t

	case []byte:
		return string(t)

	default:
		return fmt.Sprintf("%v", t)
	}
}
