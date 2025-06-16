package plugins

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

type _ChainPlugin struct{}

func (self _ChainPlugin) Info(scope types.Scope, type_map *types.TypeMap) *types.PluginInfo {
	return &types.PluginInfo{
		Name: "chain",
		Doc: "Chain the output of several queries into the same table." +
			"This plugin takes any args and chains them.",
	}
}

func (self _ChainPlugin) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) <-chan types.Row {
	output_chan := make(chan types.Row)

	queries := []types.StoredQuery{}

	// Maintain definition order for the chain plugin.
	members := scope.GetMembers(args)

	go func() {
		defer close(output_chan)

		for _, member := range members {
			member_obj, pres := args.Get(member)
			if pres {
				queries = append(queries, arg_parser.ToStoredQuery(ctx, member_obj))
			}
		}

		for _, query := range queries {
			new_scope := scope.Copy()

			in_chan := query.Eval(ctx, new_scope)
			for item := range in_chan {
				select {
				case <-ctx.Done():
					new_scope.Close()
					return

				case output_chan <- item:
				}
			}

			new_scope.Close()
		}
	}()

	return output_chan

}
