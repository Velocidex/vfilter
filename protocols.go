package vfilter

import (
	"context"

	"www.velocidex.com/golang/vfilter/types"
)

// Allow types to enumerate members
type Memberer interface {
	Members() []string
}

type Materializer interface {
	Materialize(ctx context.Context, scope types.Scope) Any
}
