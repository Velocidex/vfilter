// VQL functions to deal with aggregates. This is mostly useful with
// group by clause.
package functions

import (
	"context"
	"fmt"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

type _CountFunctionArgs struct {
	Items types.Any `vfilter:"optional,field=items"`
}

type _CountFunction struct{}

func (self _CountFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:        "count",
		Doc:         "Counts the items.",
		ArgType:     type_map.AddType(scope, _CountFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _CountFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_CountFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("count: %s", err.Error())
		return types.Null{}
	}

	count := uint64(0)
	previous_value_any, pres := scope.GetContext(GetID(&self))
	if pres {
		count = previous_value_any.(uint64)
	}

	count += 1
	scope.SetContext(GetID(&self), count)

	return count
}

type _MinFunction struct{}

func (self _MinFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:        "min",
		Doc:         "Finds the smallest item in the aggregate.",
		ArgType:     type_map.AddType(scope, _CountFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _MinFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_CountFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("min: %s", err.Error())
		return types.Null{}
	}

	var min_value types.Any = arg.Items
	previous_value, pres := scope.GetContext(GetID(self))
	if pres && !scope.Lt(min_value, previous_value) {
		min_value = previous_value
	}

	scope.SetContext(GetID(self), min_value)

	return min_value
}

type _MaxFunction struct{}

func (self _MaxFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:        "max",
		Doc:         "Finds the largest item in the aggregate.",
		ArgType:     type_map.AddType(scope, _CountFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _MaxFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_CountFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("min: %s", err.Error())
		return types.Null{}
	}

	var max_value types.Any = arg.Items
	previous_value, pres := scope.GetContext(GetID(self))
	if pres && scope.Lt(max_value, previous_value) {
		max_value = previous_value
	}

	scope.SetContext(GetID(self), max_value)

	return max_value
}

type _EnumerateFunction struct{}

func (self _EnumerateFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:        "enumerate",
		Doc:         "Collect all the items in each group by bin.",
		ArgType:     type_map.AddType(scope, _CountFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _EnumerateFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_CountFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("enumerate: %s", err.Error())
		return types.Null{}
	}

	var value types.Any
	previous_value, ok := scope.GetContext(GetID(self))
	if ok {
		previous_value_array, ok := previous_value.([]types.Any)
		if ok {
			value = append(previous_value_array, arg.Items)
		}
	} else {
		value = []types.Any{arg.Items}
	}

	scope.SetContext(GetID(self), value)

	return value
}

// Returns a unique ID for the object.
func GetID(obj types.Any) string {
	return fmt.Sprintf("%p", obj)
}
