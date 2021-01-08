package plugins

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

type _IfPluginArg struct {
	Condition types.Any         `vfilter:"required,field=condition"`
	Then      types.StoredQuery `vfilter:"required,field=then"`
	Else      types.StoredQuery `vfilter:"optional,field=else"`
}

type _IfPlugin struct{}

func (self _IfPlugin) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) <-chan types.Row {
	output_chan := make(chan types.Row)

	arg := &_IfPluginArg{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("if: %s", err.Error())
		close(output_chan)
		return output_chan
	}

	if scope.Bool(arg.Condition) {
		return arg.Then.Eval(ctx, scope)
	} else if arg.Else != nil {
		return arg.Else.Eval(ctx, scope)
	}

	close(output_chan)
	return output_chan
}

func (self _IfPlugin) Info(scope types.Scope, type_map *types.TypeMap) *types.PluginInfo {
	return &types.PluginInfo{
		Name: "if",
		Doc:  "Conditional execution of query",

		ArgType: type_map.AddType(scope, &_IfPluginArg{}),
	}
}
