package protocols

import (
	"time"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Less than protocol
type GtProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Gt(scope types.Scope, a types.Any, b types.Any) bool
}

type GtDispatcher struct {
	impl []GtProtocol
}

func (self GtDispatcher) Copy() GtDispatcher {
	return GtDispatcher{
		append([]GtProtocol{}, self.impl...)}
}

func intGt(lhs int64, b types.Any) bool {
	switch b.(type) {
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		rhs, _ := utils.ToInt64(b)
		return lhs > rhs
	case float64, float32:
		rhs, _ := utils.ToFloat(b)
		return float64(lhs) > rhs
	}
	return false
}

func (self GtDispatcher) Gt(scope types.Scope, a types.Any, b types.Any) bool {
	a = maybeReduce(a)
	b = maybeReduce(b)

	switch t := a.(type) {
	case types.Null, *types.Null, nil:
		return false

	case string:
		rhs, ok := b.(string)
		if ok {
			return t > rhs
		}

		// If it is integer like, coerce to int.
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		lhs, ok := utils.ToInt64(t)
		if ok {
			return intGt(lhs, b)
		}

	case float64:
		rhs, ok := utils.ToFloat(b)
		if ok {
			return t > rhs
		}

	case time.Time:
		rhs, ok := toTime(b)
		if ok {
			return t.After(*rhs)
		}

	case *time.Time:
		rhs, ok := toTime(b)
		if ok {
			return t.After(*rhs)
		}
	}

	switch t := b.(type) {
	case types.Null, *types.Null, nil:
		return false

	case string:
		lhs, ok := a.(string)
		if ok {
			return lhs > t
		}

		// If it is integer like, coerce to int.
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		rhs, ok := utils.ToInt64(t)
		if ok {
			if intLt(rhs, a) {
				return false
			}
			if intEq(rhs, a) {
				return false
			}
			return true
		}

	case float64:
		lhs, ok := utils.ToFloat(a)
		if ok {
			return lhs > t
		}

	case time.Time:
		lhs, ok := toTime(a)
		if ok {
			return t.Before(*lhs)
		}

	case *time.Time:
		lhs, ok := toTime(a)
		if ok {
			return t.Before(*lhs)
		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Gt(scope, a, b)
		}
	}

	return false
}

func (self *GtDispatcher) AddImpl(elements ...GtProtocol) {
	for _, impl := range elements {
		self.impl = append([]GtProtocol{impl}, self.impl...)
	}
}
