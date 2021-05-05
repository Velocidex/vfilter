package functions

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

type _IfFunctionArgs struct {
	Condition types.Any `vfilter:"required,field=condition"`
	Then      types.Any `vfilter:"optional,field=then"`
	Else      types.Any `vfilter:"optional,field=else"`
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
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("if: %s", err.Error())
		return types.Null{}
	}

	if scope.Bool(arg.Condition) {
		if utils.IsNil(arg.Then) {
			return &types.Null{}
		}

		switch t := arg.Then.(type) {
		case types.StoredQuery:
			return types.Materialize(ctx, scope, t)

		default:
			expr := arg_parser.ToLazyExpr(scope, arg.Then)
			return expr.Reduce()
		}
	}

	if utils.IsNil(arg.Else) {
		return &types.Null{}
	}

	switch t := arg.Else.(type) {
	case types.StoredQuery:
		return types.Materialize(ctx, scope, t)
	default:
		expr := arg_parser.ToLazyExpr(scope, arg.Else)
		return expr.Reduce()
	}
}
