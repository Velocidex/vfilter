package vfilter

import (
	"context"
)

type PluginGeneratorInterface interface {
	Call(ctx context.Context, scope *Scope, args Dict) <-chan Row
	Name() string
	Info(type_map *TypeMap) *PluginInfo
}


// Generic synchronous plugins just return all their rows at once.
type FunctionPlugin func(args Dict) []Row


// A generic plugin based on a function returning a slice of
// rows. Many simpler plugins do not need an asynchronous interface
// because they may obtain all their rows in one operation. This
// helper plugin allows callers to use these within VFilter
// easily. Example:

// scope.AppendPlugins(GenericListPlugin{
//   PluginName: "my_plugin",
//   Function: func(args Row) []Row {
//        ....
//   }
// })
type GenericListPlugin struct {
	PluginName string
	Description string
	Function FunctionPlugin
	RowType Any
}

func (self GenericListPlugin) Call(
	ctx context.Context,
	scope *Scope,
	args Dict) <- chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		for _, item := range self.Function(args) {
			output_chan <- item
		}
	}()

	return output_chan
}

func (self GenericListPlugin) Name() string {
	return self.PluginName
}

func (self GenericListPlugin) Info(type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: self.PluginName,
		Doc: self.Description,
		RowType: type_map.AddType(self.RowType),
	}
}
