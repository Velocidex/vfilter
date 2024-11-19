package scope

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

// A helper function to build a dict within the query.
// e.g. dict(foo=5, bar=6)
type _GetVersion struct {
	Function string `vfilter:"optional,field=function"`
	Plugin   string `vfilter:"optional,field=plugin"`
}

func (self _GetVersion) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:    "version",
		Doc:     "Gets the version of a VQL plugin or function.",
		ArgType: type_map.AddType(scope, &_GetVersion{}),
	}
}

func (self _GetVersion) Call(ctx context.Context,
	scope_int types.Scope, args *ordereddict.Dict) types.Any {
	arg := &_GetVersion{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope_int, args, arg)
	if err != nil {
		scope_int.Log("version: %s", err.Error())
		return types.Null{}
	}

	// We need internal access to the scope so dereference from
	// the interface.
	scope, ok := scope_int.(*Scope)
	if !ok {
		return types.Null{}
	}

	if arg.Plugin != "" {
		scope.dispatcher.Lock()
		plugin, pres := scope.dispatcher.plugins[arg.Plugin]
		scope.dispatcher.Unlock()

		if pres {
			return plugin.Info(scope, nil).Version
		}
		return types.Null{}

	} else if arg.Function != "" {
		scope.dispatcher.Lock()
		function, pres := scope.dispatcher.functions[arg.Function]
		scope.dispatcher.Unlock()
		if pres {
			return function.Info(scope, nil).Version
		}
		return types.Null{}
	}
	scope.Log("version: One of plugin or function should be specified")

	return 0
}
