package types

import (
	"fmt"
)

type StringProtocol interface {
	ToString(scope Scope) string
}

func ToString(a Any, scope Scope) string {
	stinger, ok := a.(StringProtocol)
	if ok {
		return stinger.ToString(scope)
	}

	return fmt.Sprintf("%v", a)
}
