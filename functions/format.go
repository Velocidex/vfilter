package functions

import (
	"context"
	"fmt"
	"reflect"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

type FormatArgs struct {
	Format string    `vfilter:"required,field=format,doc=Format string to use"`
	Args   types.Any `vfilter:"optional,field=args,doc=An array of elements to apply into the format string."`
}

type FormatFunction struct{}

func (self FormatFunction) Call(ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &FormatArgs{}
	err := arg_parser.ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("format: %s", err.Error())
		return false
	}

	var format_args []interface{}

	if arg.Args != nil {
		slice := reflect.ValueOf(arg.Args)

		// A slice of strings.
		if slice.Type().Kind() != reflect.Slice {
			format_args = append(format_args, arg.Args)
		} else {
			for i := 0; i < slice.Len(); i++ {
				value := slice.Index(i).Interface()
				format_args = append(format_args, value)
			}
		}
	}
	return fmt.Sprintf(arg.Format, format_args...)
}

func (self FormatFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:    "format",
		Doc:     "Format one or more items according to a format string.",
		ArgType: type_map.AddType(scope, &FormatArgs{}),
	}
}
