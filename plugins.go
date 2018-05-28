package vfilter

import (
	"context"
)

type PluginGeneratorInterface interface {
	Call(ctx context.Context, scope *Scope, args *Dict) <-chan Row
	Name() string
	Info(type_map *TypeMap) *PluginInfo
}

// Generic synchronous plugins just return all their rows at once.
type FunctionPlugin func(scope *Scope, args *Dict) []Row

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
	PluginName  string
	Description string
	Function    FunctionPlugin

	// An exemplar instance of the type returned by this
	// plugin. All rows must be the same type. If this is nil, we
	// use the first row returned as the exemplar (this is useful
	// for dynamic plugins).

	// This exemplar is needed to generate the list of columns for
	// documentation. Therefore, dynamic plugins do not contain
	// documentation of their returned columns.
	RowType Any
}

func (self GenericListPlugin) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		for _, item := range self.Function(scope, args) {
			output_chan <- item
		}
	}()

	return output_chan
}

func (self GenericListPlugin) Name() string {
	return self.PluginName
}

func (self GenericListPlugin) Info(type_map *TypeMap) *PluginInfo {
	result := &PluginInfo{
		Name: self.PluginName,
		Doc:  self.Description,
	}

	if self.RowType != nil {
		result.RowType = type_map.AddType(self.RowType)
	}

	return result
}

type SubSelectFunction struct {
	PluginName  string
	Description string
	SubSelect   *VQL
	RowType     Any
}

func (self SubSelectFunction) Name() string {
	return self.PluginName
}

func (self SubSelectFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) <-chan Row {

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
		Name:    self.PluginName,
		Doc:     self.Description,
		RowType: type_map.AddType(self.RowType),
	}
}

func _MakeQueryPlugin() GenericListPlugin {
	plugin := GenericListPlugin{
		PluginName: "query",
		RowType:    nil,
	}

	plugin.Function = func(scope *Scope, args *Dict) []Row {
		var result []Row
		// Extract the glob from the args.
		hits, ok := args.Get("vql")
		if ok {
			switch t := hits.(type) {
			case []Any:
				for _, item := range t {
					plugin.RowType = item
					result = append(result, item)
				}
			default:
				return result
			}
		}
		return result
	}

	return plugin
}
