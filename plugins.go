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


type SubSelectFunction struct {
	PluginName string
	Description string
	SubSelect *VQL
	RowType Any
}

func (self SubSelectFunction) Name() string {
	return self.PluginName
}

func (self SubSelectFunction) Call(
	ctx context.Context,
	scope *Scope,
	args Dict) <- chan Row {

	// Make a local copy of the scope with the args added as local
	// variables. This allows the query to refer to args.
	new_scope := *scope
	new_scope.AppendVars(args)
	in_chan := self.SubSelect.Eval(ctx, &new_scope)
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		for item := range in_chan {
			output_chan <- item
		}
	}()

	return output_chan
}

func (self SubSelectFunction) Info(type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: self.PluginName,
		Doc: self.Description,
		RowType: type_map.AddType(self.RowType),
	}
}
