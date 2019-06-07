package vfilter

import (
	"context"
	"sort"
)

type PluginGeneratorInterface interface {
	Call(ctx context.Context, scope *Scope, args *Dict) <-chan Row
	Info(scope *Scope, type_map *TypeMap) *PluginInfo
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

func (self GenericListPlugin) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	result := &PluginInfo{
		Name: self.PluginName,
		Doc:  self.Doc,
	}

	if self.RowType != nil {
		result.RowType = type_map.AddType(scope, self.RowType)
	}

	if self.ArgType != nil {
		result.ArgType = type_map.AddType(scope, self.ArgType)
	}

	return result
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

	arg := &_IfPluginArg{}
	err := ExtractArgs(scope, args, arg)
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

func (self _IfPlugin) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "if",
		Doc:  "Conditional execution of query",

		// Our type is not known - it depends on the
		// delegate's type.
		RowType: "",
		ArgType: type_map.AddType(scope, &_IfPluginArg{}),
	}
}

type _ChainPlugin struct{}

func (self _ChainPlugin) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
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

	go func() {
		defer close(output_chan)

		for _, member := range members {
			member_obj, _ := args.Get(member)
			lazy_v, ok := member_obj.(LazyExpr)
			if ok {
				member_obj = lazy_v.Reduce()
			}

			query, ok := member_obj.(StoredQuery)
			if !ok {
				scope.Log("Parameter " + member +
					" should be a query")
				return
			}

			queries = append(queries, query)
		}

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
