package vfilter

import (
	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/plugins"
	"www.velocidex.com/golang/vfilter/scope"
	"www.velocidex.com/golang/vfilter/types"
)

// Aliases to public types.
type Any = types.Any
type Row = types.Row

type Scope = types.Scope

type FunctionInterface = types.FunctionInterface
type FunctionInfo = types.FunctionInfo

type PluginInfo = types.PluginInfo
type PluginGeneratorInterface = types.PluginGeneratorInterface

type GenericListPlugin = plugins.GenericListPlugin

type TypeMap = types.TypeMap

type Null = types.Null

type LazyExpr = types.LazyExpr
type StoredQuery = types.StoredQuery

func NewScope() types.Scope {
	return scope.NewScope()
}

func ExtractArgs(scope types.Scope, args *ordereddict.Dict, value interface{}) error {
	return arg_parser.ExtractArgs(scope, args, value)
}
