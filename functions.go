package vfilter

import (
	"context"
	"time"
)

type FunctionInterface interface {
	Call(ctx context.Context, scope *Scope, row Row) Any
	Name() string
}

// A helper function to build a dict within the query.
// e.g. dict(foo=5, bar=6)
type _DictFunc struct{}

func (self _DictFunc) Name() string {
	return "dict"
}

func (self _DictFunc) Call(ctx context.Context, scope *Scope, row Row) Any {
	return row
}

type _SleepPlugin struct{}

func (self _SleepPlugin) Name() string {
	return "sleep"
}

func (self _SleepPlugin) Call(ctx context.Context, scope *Scope, row Row) Any {
	time.Sleep(10000 * time.Millisecond)
	return true
}
