package vfilter

import (
	"context"

	"github.com/Velocidex/ordereddict"
)

// A helper function to build a dict within the query.
// e.g. dict(foo=5, bar=6)
type _GetVersion struct {
	Function string `vfilter:"optional,field=function"`
	Plugin   string `vfilter:"optional,field=plugin"`
}

func (self _GetVersion) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "version",
		Doc:     "Gets the version of a VQL plugin or function.",
		ArgType: type_map.AddType(scope, &_GetVersion{}),
	}
}

func (self _GetVersion) Call(ctx context.Context, scope *Scope, args *ordereddict.Dict) Any {
	arg := &_GetVersion{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("version: %s", err.Error())
		return Null{}
	}

	if arg.Plugin != "" {
		plugin, pres := scope.plugins[arg.Plugin]
		if pres {
			return plugin.Info(scope, nil).Version
		}
		return Null{}

	} else if arg.Function != "" {
		function, pres := scope.functions[arg.Function]
		if pres {
			return function.Info(scope, nil).Version
		}
		return Null{}
	}
	scope.Log("version: One of plugin or function should be specified")

	return 0
}
