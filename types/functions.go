package types

import (
	"context"

	"github.com/Velocidex/ordereddict"
)

type FunctionInterface interface {
	Call(ctx context.Context, scope Scope, args *ordereddict.Dict) Any
	Info(scope Scope, type_map *TypeMap) *FunctionInfo
}

type PluginGeneratorInterface interface {
	Call(ctx context.Context, scope Scope, args *ordereddict.Dict) <-chan Row
	Info(scope Scope, type_map *TypeMap) *PluginInfo
}

// Describes the specific plugin.
type PluginInfo struct {
	// The name of the plugin.
	Name string

	// A helpful description about the plugin.
	Doc string

	ArgType string

	// A version of this plugin. VQL queries can target certain
	// versions of this plugin if needed.
	Version int
}

// Describe functions.
type FunctionInfo struct {
	Name    string
	Doc     string
	ArgType string

	// This is true for functions which operate on aggregates
	// (i.e. group by). For any columns which contains such a
	// function, vfilter will first run the group by clause then
	// re-evaluate the function on the aggregate column.
	IsAggregate bool

	// A version of this plugin. VQL queries can target certain
	// versions of this function if needed.
	Version int
}

// Describe a type. This is meant for human consumption so it does not
// need to be so accurate. Fields is a map between the Associative
// member and the type that is supposed to be returned. Note that
// Velocifilter automatically calls accessor methods so they look like
// standard exported fields.
type TypeDescription struct {
	Fields *ordereddict.Dict
}

// This describes what type is returned when we reference this field
// from the TypeDescription.
type TypeReference struct {
	Target   string
	Repeated bool
	Tag      string
}

// Map between type name and its description.
type TypeMap struct {
	desc *ordereddict.Dict
}
