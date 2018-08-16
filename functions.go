package vfilter

import (
	"context"
	"strings"
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
	if value, pres := ExtractStoredQuery(scope, "vql", args); pres {
		return Materialize(scope, value)
	} else {
		Debug("Query function must take arg: 'vql'")
	}
	return false
}

type _SplitFunction struct{}

func (self _SplitFunction) Name() string {
	return "split"
}

func (self _SplitFunction) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	str, pres := ExtractString("string", args)
	if pres {
		seperator := ","
		sep, pres := ExtractString("sep", args)
		if pres {
			seperator = *sep
		}

		return strings.Split(*str, seperator)
	}

	return Null{}
}

type _IfFunctionArgs struct {
	Condition Any `vfilter:"required,field=condition"`
	Then      Any `vfilter:"required,field=then"`
	Else      Any `vfilter:"optional,field=else"`
}

type _IfFunction struct{}

func (self _IfFunction) Name() string {
	return "if"
}

func (self _IfFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) Any {
	arg := &_IfFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("%s: %s", self.Name(), err.Error())
		return Null{}
	}

	if scope.Bool(arg.Condition) {
		return arg.Then
	} else {
		if arg.Else != nil {
			return arg.Else
		}
		return Null{}
	}
}

type _GetFunctionArgs struct {
	Item   Any    `vfilter:"required,field=item"`
	Member string `vfilter:"required,field=member"`
}

type _GetFunction struct{}

func (self _GetFunction) Name() string {
	return "get"
}

func (self _GetFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) Any {
	arg := &_GetFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("%s: %s", self.Name(), err.Error())
		return Null{}
	}

	if arg.Member != "" {
		result, pres := scope.Associative(arg.Item, arg.Member)
		if pres {
			return result
		}
	}
	return Null{}
}
