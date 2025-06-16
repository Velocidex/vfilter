package protocols

import (
	"math"
	"time"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Less than protocol
type LtProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Lt(scope types.Scope, a types.Any, b types.Any) bool
}

type LtDispatcher struct {
	impl []LtProtocol
}

func (self LtDispatcher) Copy() LtDispatcher {
	return LtDispatcher{
		append([]LtProtocol{}, self.impl...)}
}

// Comparison table
// LHS   RHS  -> Promoted
// int   int  -> lhs < rhs
// int   float -> float(lhs) < rhs
// float int  -> lhs < float(rhs)
// float float -> lhs < lhs

func intLt(lhs int64, b types.Any) bool {
	switch b.(type) {
	case bool, int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		rhs, _ := utils.ToInt64(b)
		return lhs < rhs
	case float64, float32:
		rhs, _ := utils.ToFloat(b)
		return float64(lhs) < rhs
	}
	return false
}

func intEq(lhs int64, b types.Any) bool {
	switch b.(type) {
	case bool, int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		rhs, _ := utils.ToInt64(b)
		return lhs == rhs
	case float64, float32:
		rhs, _ := utils.ToFloat(b)
		return float64(lhs) == rhs
	}
	return false
}

func (self LtDispatcher) Lt(scope types.Scope, a types.Any, b types.Any) bool {
	a = maybeReduce(a)
	b = maybeReduce(b)

	switch t := a.(type) {
	case types.Null, *types.Null, nil:
		return false

	case string:
		// Let string comparisons with time fall through to protocol
		// selection.
		if !isTime(b) {
			rhs, ok := b.(string)
			if ok {
				return t < rhs
			}
		}

		// If it is integer like, coerce to int.
	case bool, int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		if isTime(b) {
			lhs, ok := utils.ToInt64(t)
			if ok {
				rhs, ok := toTime(b)
				if ok {
					return time.Unix(lhs, 0).Before(*rhs)
				}
			}
		}
		lhs, ok := utils.ToInt64(t)
		if ok {
			return intLt(lhs, b)
		}

	case float64:
		rhs, ok := utils.ToFloat(b)
		if ok {
			return t < rhs
		}

	case time.Time:
		rhs, ok := toTime(b)
		if ok {
			return t.Before(*rhs)
		}

	case *time.Time:
		rhs, ok := toTime(b)
		if ok {
			return t.Before(*rhs)
		}
	}

	switch t := b.(type) {
	case types.Null, *types.Null, nil:
		return false

		// If it is integer like, coerce to int.
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		rhs, ok := utils.ToInt64(t)
		if ok {
			if intGt(rhs, a) {
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
			return lhs < t
		}

	case time.Time:
		lhs, ok := toTime(a)
		if ok {
			return t.After(*lhs)
		}

	case *time.Time:
		lhs, ok := toTime(a)
		if ok {
			return t.After(*lhs)
		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Lt(scope, a, b)
		}
	}

	return false
}

func isTime(a types.Any) bool {
	switch a.(type) {
	case time.Time:
		return true
	case *time.Time:
		return true
	}
	return false
}

func toTime(a types.Any) (*time.Time, bool) {
	switch t := a.(type) {
	case time.Time:
		return &t, true

	case *time.Time:
		return t, true

	case float64:
		sec_f, dec_f := math.Modf(t)
		dec_f *= 1e9
		res := time.Unix(int64(sec_f), int64(dec_f))
		return &res, true

	default:
		// Maybe it is an int
		sec, ok := utils.ToInt64(a)
		if ok {
			// Treat it as an epoch seconds
			res := time.Unix(sec, 0)
			return &res, true
		}

		return nil, false
	}
}

func (self *LtDispatcher) AddImpl(elements ...LtProtocol) {
	for _, impl := range elements {
		self.impl = append([]LtProtocol{impl}, self.impl...)
	}
}
