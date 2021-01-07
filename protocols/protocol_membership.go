package protocols

import (
	"reflect"
	"strings"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Membership protocol
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

type _SubstringMembership struct{}

func (self _SubstringMembership) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := utils.ToString(a)
	_, b_ok := utils.ToString(b)
	return a_ok && b_ok
}

func (self _SubstringMembership) Membership(scope types.Scope, a types.Any, b types.Any) bool {
	a_str, _ := utils.ToString(a)
	b_str, _ := utils.ToString(b)

	return strings.Contains(b_str, a_str)
}
