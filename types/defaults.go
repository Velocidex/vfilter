package types

import (
	"context"

	"github.com/Velocidex/ordereddict"
)

type DefaultArgInterface interface {
	ApplyDefaults(ctx context.Context, scope Scope, vars *ordereddict.Dict)
}

func MaybeApplyDefaultArgs(callable interface{},
	ctx context.Context, scope Scope, args *ordereddict.Dict) {

	defaults, ok := callable.(DefaultArgInterface)
	if ok {
		defaults.ApplyDefaults(ctx, scope, args)
	}
}
