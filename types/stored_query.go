package types

import "context"

// A plugin like object which takes no arguments but may be inserted
// into the scope to select from it.
type StoredQuery interface {
	Eval(ctx context.Context, scope Scope) <-chan Row
}
