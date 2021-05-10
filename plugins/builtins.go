package plugins

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

func GetBuiltinPlugins() []types.PluginGeneratorInterface {
	return []types.PluginGeneratorInterface{
		_IfPlugin{},
		_FlattenPluginImpl{},
		_ChainPlugin{},
		_ForeachPluginImpl{},
		RangePlugin{},
		&GenericListPlugin{
			PluginName: "scope",
			Function: func(ctx context.Context,
				scope types.Scope, args *ordereddict.Dict) []types.Row {
				return []types.Row{scope}
			},
		},
	}
}
