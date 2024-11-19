package functions

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

type _IfFunctionArgs struct {
	Condition types.Any     `vfilter:"required,field=condition"`
	Then      types.LazyAny `vfilter:"optional,field=then"`
	Else      types.LazyAny `vfilter:"optional,field=else"`
}

type _IfFunction struct{}

func (self _IfFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:    "if",
		Doc:     "If condition is true, return the 'then' value otherwise the 'else' value.",
		ArgType: type_map.AddType(scope, _IfFunctionArgs{}),
	}
}

func (self _IfFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {

	arg := &_IfFunctionArgs{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
	if err != nil {
		scope.Log("if: %v", err)
		return types.Null{}
	}

	if scope.Bool(arg.Condition) {
		if types.IsNil(arg.Then) {
			return &types.Null{}
		}

		lazy_expr, ok := arg.Then.(types.LazyExpr)
		if ok {
			arg.Then = lazy_expr.ReduceWithScope(ctx, scope)
		}

		switch t := arg.Then.(type) {
		case types.StoredQuery:
			// If Function with subqueries should return a lazy subquery
			return t

		case types.LazyExpr:
			exp := t.ReduceWithScope(ctx, scope)
			s, ok := exp.(types.StoredQuery)
			if ok {
				return s
			}

		default:
			return t
		}
	}
	if types.IsNil(arg.Else) {
		return &types.Null{}
	}

	lazy_expr, ok := arg.Else.(types.LazyExpr)
	if ok {
		arg.Else = lazy_expr.ReduceWithScope(ctx, scope)
	}

	switch t := arg.Else.(type) {
	case types.StoredQuery:
		// If Function with subqueries should return a lazy subquery
		return t

	case types.LazyExpr:
		return t.ReduceWithScope(ctx, scope)

	default:
		return t
	}
}
