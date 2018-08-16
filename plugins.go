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
	new_scope := scope.Copy()
	new_scope.AppendVars(args)
	in_chan := self.SubSelect.Eval(ctx, new_scope)
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

type _IfPlugin struct{}

func (self _IfPlugin) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) <-chan Row {
	output_chan := make(chan Row)

	condition, pres := args.Get("condition")
	if !pres {
		scope.Log("Expecting 'condition' parameter")
		close(output_chan)
		return output_chan
	}

	// else_query is optional.
	else_query, _ := ExtractStoredQuery(scope, "else", args)

	query, pres := ExtractStoredQuery(scope, "query", args)
	if !pres {
		scope.Log("Expecting 'query' parameter")
		close(output_chan)
		return output_chan
	}

	go func() {
		defer close(output_chan)

		var from_chan <-chan Row
		if scope.Bool(condition) {
			from_chan = query.Eval(ctx, scope)
		} else if else_query != nil {
			from_chan = else_query.Eval(ctx, scope)
		} else {
			return
		}

		for {
			item, ok := <-from_chan
			if !ok {
				return
			}
			output_chan <- item
		}
	}()

	return output_chan
}

func (self _IfPlugin) Name() string {
	return "if"
}

func (self _IfPlugin) Info(type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "if",
		Doc:  "Conditional execution of query",

		// Our type is not known - it depends on the
		// delegate's type.
		RowType: "",
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
		hits, ok := ExtractStoredQuery(scope, "vql", args)
		if ok {
			for _, item := range Materialize(scope, hits) {
				result = append(result, item)
			}
		}
		return result
	}

	return plugin
}
