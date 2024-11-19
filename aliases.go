package vfilter

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/functions"
	"www.velocidex.com/golang/vfilter/plugins"
	"www.velocidex.com/golang/vfilter/scope"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils/dict"
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
type GenericFunction = functions.GenericFunction

type TypeMap = types.TypeMap

type Null = types.Null

type LazyExpr = types.LazyExpr
type StoredQuery = types.StoredQuery

type ScopeUnmarshaller = scope.ScopeUnmarshaller

func NewScope() types.Scope {
	return scope.NewScope()
}

func RowToDict(
	ctx context.Context,
	scope types.Scope, row types.Row) *ordereddict.Dict {
	return dict.RowToDict(ctx, scope, row)
}
