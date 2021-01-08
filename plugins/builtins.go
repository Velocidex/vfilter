package plugins

import (
	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

func GetBuiltinPlugins() []types.PluginGeneratorInterface {
	return []types.PluginGeneratorInterface{
		_IfPlugin{},
		_FlattenPluginImpl{},
		_ChainPlugin{},
		_ForeachPluginImpl{},
		&GenericListPlugin{
			PluginName: "scope",
			Function: func(scope types.Scope, args *ordereddict.Dict) []types.Row {
				return []types.Row{scope}
			},
		},
	}
}
