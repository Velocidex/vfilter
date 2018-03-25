package vfilter

import (
	"context"
)

type PluginGeneratorInterface interface {
	Call(ctx context.Context, scope *Scope, args Row) <-chan Row
	Name() string
}
