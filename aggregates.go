// VQL functions to deal with aggregates. This is mostly useful with
// group by clause.
package vfilter

import (
	"context"
	"reflect"
)

type _CountFunctionArgs struct {
	Items Any `vfilter:"required,field=items"`
}

type _CountFunction struct{}

func (self _CountFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:        "count",
		Doc:         "Counts the items.",
		ArgType:     type_map.AddType(scope, _CountFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _CountFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) Any {
	arg := &_CountFunctionArgs{}
	err := ExtractArgs(scope, args, arg)

	if err != nil {
		scope.Log("count: %s", err.Error())
		return Null{}
	}

	slice := reflect.ValueOf(arg.Items)
	if slice.Type().Kind() == reflect.Slice {
		return slice.Len()
	}

	return Null{}
}

type _MinFunction struct{}

func (self _MinFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:        "min",
		Doc:         "Finds the smallest item in the aggregate.",
		ArgType:     type_map.AddType(scope, _CountFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _MinFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) Any {
	arg := &_CountFunctionArgs{}
	err := ExtractArgs(scope, args, arg)

	if err != nil {
		scope.Log("min: %s", err.Error())
		return Null{}
	}

	var result Any = nil
	slice := reflect.ValueOf(arg.Items)
	if slice.Type().Kind() == reflect.Slice {
		for i := 0; i < slice.Len(); i++ {
			value := slice.Index(i).Interface()
			if result == nil || scope.Lt(value, result) {
				result = value
			}
		}
	}

	if result == nil {
		return Null{}
	}

	return result
}

type _MaxFunction struct{}

func (self _MaxFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:        "max",
		Doc:         "Finds the largest item in the aggregate.",
		ArgType:     type_map.AddType(scope, _CountFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _MaxFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) Any {
	arg := &_CountFunctionArgs{}
	err := ExtractArgs(scope, args, arg)

	if err != nil {
		scope.Log("min: %s", err.Error())
		return Null{}
	}

	var result Any = nil
	slice := reflect.ValueOf(arg.Items)
	if slice.Type().Kind() == reflect.Slice {
		for i := 0; i < slice.Len(); i++ {
			value := slice.Index(i).Interface()
			if result == nil || !scope.Lt(value, result) {
				result = value
			}
		}
	}

	if result == nil {
		return Null{}
	}

	return result
}
