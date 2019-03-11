// VQL functions to deal with aggregates. This is mostly useful with
// group by clause.
package vfilter

import (
	"context"
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

	count := uint64(0)
	previous_value_any := scope.GetContext(GetID(self))
	if previous_value_any != nil {
		count = previous_value_any.(uint64)
	}

	count += 1
	scope.SetContext(GetID(self), count)

	return count
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

	var min_value Any = arg.Items
	previous_value := scope.GetContext(GetID(self))

	if previous_value != nil && !scope.Lt(min_value, previous_value) {
		min_value = previous_value
	}

	scope.SetContext(GetID(self), min_value)

	return min_value
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

	var max_value Any = arg.Items
	previous_value := scope.GetContext(GetID(self))
	if previous_value != nil && scope.Lt(max_value, previous_value) {
		max_value = previous_value
	}

	scope.SetContext(GetID(self), max_value)

	return max_value
}

type _EnumerateFunction struct{}

func (self _EnumerateFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:        "enumerate",
		Doc:         "Collect all the items in each group by bin.",
		ArgType:     type_map.AddType(scope, _CountFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _EnumerateFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) Any {
	arg := &_CountFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("enumerate: %s", err.Error())
		return Null{}
	}

	var value Any
	previous_value, ok := scope.GetContext(GetID(self)).([]Any)
	if ok {
		value = append(previous_value, arg.Items)
	} else {
		value = []Any{arg.Items}
	}

	scope.SetContext(GetID(self), value)

	return value
}
