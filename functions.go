package vfilter

import (
	"context"
	"time"
)

type FunctionInterface interface {
	Call(ctx context.Context, scope *Scope, args *Dict) Any
	Name() string
}

// A helper function to build a dict within the query.
// e.g. dict(foo=5, bar=6)
type _DictFunc struct{}

func (self _DictFunc) Name() string {
	return "dict"
}

func (self _DictFunc) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	return args
}

type _SleepPlugin struct{}

func (self _SleepPlugin) Name() string {
	return "sleep"
}

func (self _SleepPlugin) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	time.Sleep(10000 * time.Millisecond)
	return true
}

type _Timestamp struct{}

func (self _Timestamp) Name() string {
	return "timestamp"
}

func (self _Timestamp) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	var epoch float64
	if !ExtractFloat(&epoch, "epoch", args) {
		return false
	}

	return time.Unix(int64(epoch), 0)
}

type _SubSelectFunction struct{}

func (self _SubSelectFunction) Name() string {
	return "query"
}

func (self _SubSelectFunction) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	if value, pres := args.Get("vql"); pres {
		return value
	} else {
		Debug("Query function must take arg: 'vql'")
	}
	return false
}
