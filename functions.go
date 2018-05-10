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

type _Timestamp struct{}

func (self _Timestamp) Name() string {
	return "timestamp"
}

func (self _Timestamp) Call(ctx context.Context, scope *Scope, row Row) Any {
	epoch_arg, ok := scope.Associative(row, "epoch")
	if ok {
		epoch, ok := to_float(epoch_arg)
		if ok {
			return time.Unix(int64(epoch), 0)
		}
	}

	return false
}

type _SubSelectFunction struct{}

func (self _SubSelectFunction) Name() string {
	return "query"
}

func (self _SubSelectFunction) Call(ctx context.Context, scope *Scope, row Row) Any {
	if value, pres := scope.Associative(row, "vql"); pres {
		return value
	} else {
		Debug("Query function must take arg: 'vql'")
	}
	return false
}
