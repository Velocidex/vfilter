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

// There are several parts to aggregate support:

//  1. Each aggregate function instance in the AST carries a unique
//     ID. This allows the function to store its own state in the
//     aggregate context without interference from other instances of
//     the same function (e.g. having two count() instaces is OK)

//  2. The scope may contain a reference to an AggregatorCtx
//     object. This object manages access to the aggregate
//     context. The main method that should be used is Modify() which
//     mofidies the context under lock.

//  3. When the scope spawns a child scope, the child scope does not
//     have its own AggregatorCtx, instead chasing its parent to find
//     one. This allows aggregate functions within the scope to see
//     the wider AggregatorCtx which controls the entire query clause.

//  4. When the query runs in an isolated context, the AggregatorCtx
//     is recreated at the calling scope. This allows isolated scopes
//     to reset the AggregatorCtx. For example, when calling a LET
//     defined function, a new context is created.

//  5. If a GROUP BY query, the Grouper will create a new
//     AggregatorCtx for each bin. This allows aggregate functions to
//     apply on each group separately.

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
type Aggregator struct {
	id string
}

func (self Aggregator) GetContext(scope types.Scope) (res types.Any, res_pres bool) {
	return scope.GetAggregatorCtx().Modify(self.id,
		func(previous_value_any types.Any, pres bool) types.Any {
			res_pres = pres
			return previous_value_any
		}), res_pres
}

func (self Aggregator) SetContext(scope types.Scope, value types.Any) {
	scope.GetAggregatorCtx().Modify(self.id,
		func(previous_value_any types.Any, pres bool) types.Any {
			return value
		})
}

// Sets a new aggregator if possible
func NewAggregator() Aggregator {
	new_id := atomic.AddUint64(&id, 1)
	return Aggregator{
		id: fmt.Sprintf("id_%v", new_id),
	}
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

// Aggregate functions must be copiable.
func (self _CountFunction) Copy() types.FunctionInterface {
	return _CountFunction{
		Aggregator: NewAggregator(),
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

	// Modify the aggregator under lock
	return scope.GetAggregatorCtx().Modify(self.id,
		func(previous_value_any types.Any, pres bool) types.Any {
			count := uint64(0)

			if pres {
				var ok bool
				count, ok = previous_value_any.(uint64)
				if !ok {
					scope.Log("count: unexpected previous value type %T",
						previous_value_any)
					return types.Null{}
				}
			}

			return count + 1
		})
}

type _SumFunctionArgs struct {
	Item int64 `vfilter:"required,field=item"`
}

type _SumFunction struct {
	Aggregator
}

// Aggregate functions must be copiable.
func (self _SumFunction) Copy() types.FunctionInterface {
	return _SumFunction{
		Aggregator: NewAggregator(),
	}
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

	return scope.GetAggregatorCtx().Modify(self.id,
		func(previous_value_any types.Any, pres bool) types.Any {
			sum := int64(0)
			if pres {
				var ok bool
				sum, ok = previous_value_any.(int64)
				if !ok {
					scope.Log("sum: unexpected previous value type %T", previous_value_any)
					return types.Null{}
				}
			}

			sum += arg.Item
			return sum

		})
}

type _MinFunctionArgs struct {
	Item types.LazyExpr `vfilter:"required,field=item"`
}

type _MinFunction struct {
	Aggregator
}

// Aggregate functions must be copiable.
func (self _MinFunction) Copy() types.FunctionInterface {
	return &_MinFunction{
		Aggregator: NewAggregator(),
	}
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

	var min_value types.Any = arg.Item.Reduce(ctx)

	return scope.GetAggregatorCtx().Modify(self.id,
		func(previous_value_any types.Any, pres bool) types.Any {
			if pres && !scope.Lt(min_value, previous_value_any) {
				min_value = previous_value_any
			}

			return min_value
		})
}

type _MaxFunction struct {
	Aggregator
}

// Aggregate functions must be copiable.
func (self _MaxFunction) Copy() types.FunctionInterface {
	return _MaxFunction{
		Aggregator: NewAggregator(),
	}
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

	var max_value types.Any = arg.Item.Reduce(ctx)

	return scope.GetAggregatorCtx().Modify(self.id,
		func(previous_value_any types.Any, pres bool) types.Any {
			if pres && scope.Lt(max_value, previous_value_any) {
				max_value = previous_value_any
			}

			return max_value
		})
}

type _EnumeateFunctionArgs struct {
	Items types.Any `vfilter:"optional,field=items,doc=The items to enumerate"`
}

type _EnumerateFunction struct {
	Aggregator
}

// Aggregate functions must be copiable.
func (self _EnumerateFunction) Copy() types.FunctionInterface {
	return _EnumerateFunction{
		Aggregator: NewAggregator(),
	}
}

func (self _EnumerateFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:        "enumerate",
		Doc:         "Collect all the items in each group by bin.",
		ArgType:     type_map.AddType(scope, _EnumeateFunctionArgs{}),
		IsAggregate: true,
	}
}

func (self _EnumerateFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_EnumeateFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("enumerate: %s", err.Error())
		return types.Null{}
	}

	return scope.GetAggregatorCtx().Modify(self.id,
		func(previous_value_any types.Any, pres bool) types.Any {
			var value types.Any
			if pres {
				previous_value_array, ok := previous_value_any.([]types.Any)
				if ok {
					value = append(previous_value_array, arg.Items)
				}
			} else {
				value = []types.Any{arg.Items}
			}

			return value
		})
}
