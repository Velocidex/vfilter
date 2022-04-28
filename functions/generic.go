package functions

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

// Generic synchronous plugins just return all their rows at once.
type GenericFunctionInterface func(ctx context.Context, scope types.Scope, args *ordereddict.Dict) types.Any

// A generic plugin based on a function returning a slice of
// rows. Many simpler plugins do not need an asynchronous interface
// because they may obtain all their rows in one operation. This
// helper plugin allows callers to use these within VFilter
// easily. Example:

// scope.AppendPlugins(GenericListPlugin{
//   PluginName: "my_plugin",
//   Function: func(args types.Row) []types.Row {
//        ....
//   }
// })
type GenericFunction struct {
	FunctionName string
	Doc          string
	Function     GenericFunctionInterface

	ArgType types.Any
}

func (self GenericFunction) XCopy() types.FunctionInterface {
	return self
}

func (self GenericFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {

	return self.Function(ctx, scope, args)
}

func (self GenericFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	result := &types.FunctionInfo{
		Name: self.FunctionName,
		Doc:  self.Doc,
	}

	if self.ArgType != nil {
		result.ArgType = type_map.AddType(scope, self.ArgType)
	}

	return result
}
