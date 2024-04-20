package functions

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

type _Scope struct{}

func (self _Scope) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {

	return scope
}

func (self _Scope) Info(scope types.Scope,
	type_map *types.TypeMap) *types.FunctionInfo {

	return &types.FunctionInfo{
		Name: "scope",
		Doc:  "return the scope.",
	}
}
