// VQL functions to deal with aggregates. This is mostly useful with
// group by clause.

// Aggregate functions store state between invocations in a way that
// is compatible with the GROUP BY clause. The state is stored in the
// scope but it is unique to the specific instance of the aggregate
// function. For example consider:
//
// SELECT sum(item=X), sum(item=Y) FROM ... GROUP BY ...
//
// Each instance of sum() will keep its own aggregate constant within
// the scope. Similarly the group by clause will force different
// aggregates to use a different scope context, therefore the result
// will be the sum over each group.

package functions

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

var (
	// Atomically incremented global id to give aggregate
	// functions.
	id uint64
)

// All aggregate functions need to embed the Aggregator. Aggregators
// store their state in the scope context so they can retrieve it next
// time they are evaluated.
type Aggregator string

func (self Aggregator) GetContext(scope types.Scope) (types.Any, bool) {
	return scope.GetContext(string(self))
}

func (self Aggregator) SetContext(scope types.Scope, value types.Any) {
	scope.SetContext(string(self), value)
}

// Sets a new aggregator if possible
func (self *Aggregator) SetNewAggregator() {
	new_id := atomic.AddUint64(&id, 1)
	new_str := fmt.Sprintf("__aggr_id_%v", new_id)

	*self = Aggregator(new_str)
}

type AggregatorInterface interface {
	SetNewAggregator()
}

type _CountFunctionArgs struct {
	Items types.Any `vfilter:"optional,field=items,doc=Not used anymore"`
}

type _CountFunction struct {
	Aggregator
}

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
	previous_value_any, pres := self.GetContext(scope)
	if pres {
		var ok bool
		count, ok = previous_value_any.(uint64)
		if !ok {
			scope.Log("sum: unexpected previous value type %T", previous_value_any)
			return types.Null{}
		}
	}

	count += 1
	self.SetContext(scope, count)

	return count
}

type _SumFunctionArgs struct {
	Item int64 `vfilter:"required,field=item"`
}

type _SumFunction struct {
	Aggregator
}

func (self _SumFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:        "sum",
		Doc:         "Sums the items.",
		ArgType:     type_map.AddType(scope, _SumFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _SumFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_SumFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("sum: %s", err.Error())
		return types.Null{}
	}

	sum := int64(0)
	previous_value_any, pres := self.GetContext(scope)
	if pres {
		var ok bool
		sum, ok = previous_value_any.(int64)
		if !ok {
			scope.Log("sum: unexpected previous value type %T", previous_value_any)
			return types.Null{}
		}
	}

	sum += arg.Item
	self.SetContext(scope, sum)

	return sum
}

type _MinFunctionArgs struct {
	Item int64 `vfilter:"required,field=item"`
}

type _MinFunction struct {
	Aggregator
}

func (self _MinFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:        "min",
		Doc:         "Finds the smallest item in the aggregate.",
		ArgType:     type_map.AddType(scope, _MinFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _MinFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_MinFunctionArgs{}

	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("min: %s", err.Error())
		return types.Null{}
	}

	var min_value types.Any = arg.Item
	previous_value, pres := self.GetContext(scope)
	if pres && !scope.Lt(min_value, previous_value) {
		min_value = previous_value
	}

	self.SetContext(scope, min_value)

	return min_value
}

type _MaxFunction struct {
	Aggregator
}

func (self _MaxFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:        "max",
		Doc:         "Finds the largest item in the aggregate.",
		ArgType:     type_map.AddType(scope, _MinFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _MaxFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_MinFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("min: %s", err.Error())
		return types.Null{}
	}

	var max_value types.Any = arg.Item
	previous_value, pres := self.GetContext(scope)
	if pres && scope.Lt(max_value, previous_value) {
		max_value = previous_value
	}

	self.SetContext(scope, max_value)

	return max_value
}

type _EnumerateFunction struct {
	Aggregator
}

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
	previous_value, ok := self.GetContext(scope)
	if ok {
		previous_value_array, ok := previous_value.([]types.Any)
		if ok {
			value = append(previous_value_array, arg.Items)
		}
	} else {
		value = []types.Any{arg.Items}
	}

	self.SetContext(scope, value)

	return value
}
