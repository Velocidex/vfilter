package plugins

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

type RangePluginArgs struct {
	Start int64 `vfilter:"optional,field=start,doc=Start index (0 based - default 0)"`
	End   int64 `vfilter:"required,field=end,doc=End index (0 based)"`
	Step  int64 `vfilter:"optional,field=step,doc=Step (default 1)"`
}

type RangePlugin struct{}

func (self RangePlugin) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) <-chan types.Row {
	output_chan := make(chan types.Row)

	go func() {
		defer close(output_chan)

		arg := &RangePluginArgs{}
		err := arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
		if err != nil {
			scope.Log("range: %v", err)
			return
		}

		if arg.Step == 0 {
			arg.Step = 1
		}

		for i := arg.Start; i < arg.End; i += arg.Step {
			select {
			case <-ctx.Done():
				return

			case output_chan <- ordereddict.NewDict().Set("_value", i):
			}
		}
	}()

	return output_chan
}

func (self RangePlugin) Info(scope types.Scope, type_map *types.TypeMap) *types.PluginInfo {
	return &types.PluginInfo{
		Name:    "range",
		Doc:     "Iterate over range.",
		ArgType: type_map.AddType(scope, &RangePluginArgs{}),
	}
}
