package vfilter

import (
	"context"
	"sort"
)

type PluginGeneratorInterface interface {
	Call(ctx context.Context, scope *Scope, args *Dict) <-chan Row
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
	PluginName string
	Doc        string
	Function   FunctionPlugin

	// An exemplar instance of the type returned by this
	// plugin. All rows must be the same type. If this is nil, we
	// use the first row returned as the exemplar (this is useful
	// for dynamic plugins).

	// This exemplar is needed to generate the list of columns for
	// documentation. Therefore, dynamic plugins do not contain
	// documentation of their returned columns.
	RowType Any

	ArgType Any
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
		Doc:  self.Doc,
	}

	if self.RowType != nil {
		result.RowType = type_map.AddType(self.RowType)
	}

	if self.ArgType != nil {
		result.ArgType = type_map.AddType(self.ArgType)
	}

	return result
}

type SubSelectFunction struct {
	PluginName  string
	Description string
	SubSelect   *VQL
	RowType     Any
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

type _IfPluginArg struct {
	Condition Any         `vfilter:"required,field=condition"`
	Then      StoredQuery `vfilter:"required,field=then"`
	Else      StoredQuery `vfilter:"optional,field=else"`
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

	query, pres := ExtractStoredQuery(scope, "then", args)
	if !pres {
		scope.Log("Expecting 'then' parameter")
		Debug(args)
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

func (self _IfPlugin) Info(type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "if",
		Doc:  "Conditional execution of query",

		// Our type is not known - it depends on the
		// delegate's type.
		RowType: "",
		ArgType: type_map.AddType(&_IfPluginArg{}),
	}
}

type _ChainPlugin struct{}

func (self _ChainPlugin) Info(type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "chain",
		Doc: "Chain the output of several queries into the same table." +
			"This plugin takes any args and chains them.",
	}
}

func (self _ChainPlugin) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) <-chan Row {
	output_chan := make(chan Row)

	queries := []StoredQuery{}
	members := scope.GetMembers(args)
	sort.Strings(members)

	for _, member := range members {
		query, pres := ExtractStoredQuery(scope, member, args)
		if !pres {
			scope.Log("Parameter " + member + " should be a query")
			close(output_chan)
			return output_chan
		}

		queries = append(queries, query)
	}

	go func() {
		defer close(output_chan)

		for _, query := range queries {
			new_scope := scope.Copy()
			in_chan := query.Eval(ctx, new_scope)
			for item := range in_chan {
				output_chan <- item
			}
		}
	}()

	return output_chan

}
