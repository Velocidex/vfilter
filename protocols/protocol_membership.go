package protocols

import (
	"reflect"
	"strings"

	"www.velocidex.com/golang/vfilter/types"
)

// Membership protocol (the "in" operator)
type MembershipProtocol interface {
	Applicable(a types.Any, b types.Any) bool
	Membership(scope types.Scope, a types.Any, b types.Any) bool
}

type MembershipDispatcher struct {
	impl []MembershipProtocol
}

func (self MembershipDispatcher) Copy() MembershipDispatcher {
	return MembershipDispatcher{
		append([]MembershipProtocol{}, self.impl...)}
}

func (self MembershipDispatcher) Membership(scope types.Scope, a types.Any, b types.Any) bool {
	switch t := b.(type) {
	case types.Null, *types.Null, nil:
		return false

	case string:
		// 'he' in 'hello'
		a_str, ok := a.(string)
		if ok {
			return strings.Contains(t, a_str)
		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Membership(scope, a, b)
		}
	}

	// Default behavior: Test lhs against each member in RHS -
	// slow but works.
	rt := reflect.TypeOf(b)
	if rt == nil {
		return false
	}

	kind := rt.Kind()
	value := reflect.ValueOf(b)
	if kind == reflect.Slice || kind == reflect.Array {
		for i := 0; i < value.Len(); i++ {
			item := value.Index(i).Interface()
			if scope.Eq(a, item) {
				return true
			}
		}
	} else {
		scope.Trace("Protocol Membership not found for %v (%T) and %v (%T)",
			a, a, b, b)
	}

	return false
}

func (self *MembershipDispatcher) AddImpl(elements ...MembershipProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}
