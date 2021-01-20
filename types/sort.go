package types

import "context"

// A Sorter is a pluggable way for VQL to sort an incoming set of rows.
type Sorter interface {
	Sort(ctx context.Context,
		scope Scope,
		input <-chan Row,
		key string,
		desc bool) <-chan Row
}
