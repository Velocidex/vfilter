package protocols

import (
	"context"

	"www.velocidex.com/golang/vfilter/types"
)

func maybeReduce(a types.Any) types.Any {
	lazy_expr, ok := a.(types.LazyExpr)
	if ok {
		return lazy_expr.Reduce(context.Background())
	}
	return a
}
