package vfilter

import (
	"context"
	"strings"
	"time"
)

type FunctionInterface interface {
	Call(ctx context.Context, scope *Scope, args *Dict) Any
	Info(type_map *TypeMap) *FunctionInfo
}

// A helper function to build a dict within the query.
// e.g. dict(foo=5, bar=6)
type _DictFunc struct{}

func (self _DictFunc) Info(type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "dict",
		Doc:  "Construct a dict from arbitrary keyword args.",
	}
}

func (self _DictFunc) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	return args
}

type _TimestampArg struct {
	Epoch int64 `vfilter:"required,field=epoch"`
}
type _Timestamp struct{}

func (self _Timestamp) Info(type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "timestamp",
		Doc:     "Convert seconds from epoch into a string.",
		ArgType: type_map.AddType(_TimestampArg{}),
	}
}

func (self _Timestamp) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	arg := &_TimestampArg{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("timestamp: %s", err.Error())
		return Null{}
	}

	return time.Unix(arg.Epoch, 0)
}

type _SubSelectFunctionArgs struct {
	VQL Any `vfilter:"required,field=vql"`
}

type _SubSelectFunction struct{}

func (self _SubSelectFunction) Info(type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "query",
		Doc:     "Launch a subquery and materialize it into a list of rows.",
		ArgType: type_map.AddType(_TimestampArg{}),
	}
}

func (self _SubSelectFunction) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	stored_query, ok := ExtractStoredQuery(scope, "vql", args)
	if !ok {
		scope.Log("query: vql must be a stored query.")
		return Null{}
	}

	return Materialize(scope, stored_query)
}

type _SplitFunctionArgs struct {
	String string `vfilter:"required,field=string"`
	Sep    string `vfilter:"required,field=sep"`
}
type _SplitFunction struct{}

func (self _SplitFunction) Info(type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "split",
		Doc:     "Splits a string into an array.",
		ArgType: type_map.AddType(_TimestampArg{}),
	}
}

func (self _SplitFunction) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	arg := &_SplitFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("split: %s", err.Error())
		return Null{}
	}
	return strings.Split(arg.String, arg.Sep)
}

type _IfFunctionArgs struct {
	Condition Any `vfilter:"required,field=condition"`
	Then      Any `vfilter:"required,field=then"`
	Else      Any `vfilter:"optional,field=else"`
}

type _IfFunction struct{}

func (self _IfFunction) Info(type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "if",
		Doc:     "If condition is true, return the 'then' value otherwise the 'else' value.",
		ArgType: type_map.AddType(_IfFunctionArgs{}),
	}
}

func (self _IfFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) Any {
	arg := &_IfFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("if: %s", err.Error())
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

func (self _GetFunction) Info(type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "get",
		Doc:     "Gets the member field from item.",
		ArgType: type_map.AddType(_GetFunctionArgs{}),
	}
}

func (self _GetFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) Any {
	arg := &_GetFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("get: %s", err.Error())
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
