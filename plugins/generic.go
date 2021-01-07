package plugins

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

// Generic synchronous plugins just return all their rows at once.
type FunctionPlugin func(scope types.Scope, args *ordereddict.Dict) []types.Row

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
type GenericListPlugin struct {
	PluginName string
	Doc        string
	Function   FunctionPlugin

	ArgType types.Any
}

func (self GenericListPlugin) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) <-chan types.Row {
	output_chan := make(chan types.Row)

	go func() {
		defer close(output_chan)

		for _, item := range self.Function(scope, args) {
			select {
			case <-ctx.Done():
				return
			case output_chan <- item:
			}
		}
	}()

	return output_chan
}

func (self GenericListPlugin) Name() string {
	return self.PluginName
}

func (self GenericListPlugin) Info(scope types.Scope, type_map *types.TypeMap) *types.PluginInfo {
	result := &types.PluginInfo{
		Name: self.PluginName,
		Doc:  self.Doc,
	}

	if self.ArgType != nil {
		result.ArgType = type_map.AddType(scope, self.ArgType)
	}

	return result
}
