package arg_parser

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

func GetStringArg(
	ctx context.Context, scope types.Scope, args *ordereddict.Dict, field string) string {
	any_, pres := args.Get(field)
	if !pres {
		return ""
	}

	lazy_, pres := any_.(types.LazyExpr)
	if pres {
		any_ = lazy_.ReduceWithScope(ctx, scope)
	}

	result, _ := any_.(string)
	return result
}
