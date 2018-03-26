package vfilter

import (
	"reflect"
	"regexp"
	"strings"
)

type _BoolDispatcher struct {
	implementations []BoolProtocol
}

func (self _BoolDispatcher) Bool(scope *Scope, a Any) bool {
	for _, impl := range self.implementations {
		if impl.Applicable(a) {
			return impl.Bool(scope, a)
		}
	}

	return false
}

func (self *_BoolDispatcher) AddImpl(elements ...BoolProtocol) {
	for _, impl := range elements {
		self.implementations = append(self.implementations, impl)
	}
}

// This protocol implements the truth value.
type BoolProtocol interface {
	Applicable(a Any) bool
	Bool(scope *Scope, a Any) bool
}

// Bool Implementations
type _BoolImpl struct{}

func (self _BoolImpl) Bool(scope *Scope, a Any) bool {
	return a.(bool)
}

func (self _BoolImpl) Applicable(a Any) bool {
	_, ok := a.(bool)
	return ok
}

type _BoolInt struct{}

func (self _BoolInt) Bool(scope *Scope, a Any) bool {
	a_val, _ := to_float(a)
	if a_val != 0 {
		return true
	}

	return false
}

func (self _BoolInt) Applicable(a Any) bool {
	_, a_ok := to_float(a)
	return a_ok
}

// Eq protocol
type EqProtocol interface {
	Applicable(a Any, b Any) bool
	Eq(scope *Scope, a Any, b Any) bool
}

type _EqDispatcher struct {
	impl []EqProtocol
}

func (self _EqDispatcher) Eq(scope *Scope, a Any, b Any) bool {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return impl.Eq(scope, a, b)
		}
	}
	return false
}

func (self *_EqDispatcher) AddImpl(elements ...EqProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _StringEq struct{}

func (self _StringEq) Eq(scope *Scope, a Any, b Any) bool {
	return a == b
}

func (self _StringEq) Applicable(a Any, b Any) bool {
	_, a_ok := a.(string)
	_, b_ok := b.(string)
	return a_ok && b_ok
}

type _NumericEq struct{}

func (self _NumericEq) Eq(scope *Scope, a Any, b Any) bool {
	a_val, _ := to_float(a)
	b_val, _ := to_float(b)

	return a_val == b_val
}

func to_float(x Any) (float64, bool) {
	b_value, b_ok := x.(bool)
	if b_ok {
		if b_value {
			return 1, true
		} else {
			return 0, true
		}
	}

	f_value, f_ok := x.(float64)
	if f_ok {
		return f_value, true
	}

	int_value, int_ok := x.(int)
	if int_ok {
		return float64(int_value), true
	}

	int64_value, int64_ok := x.(int64)
	if int64_ok {
		return float64(int64_value), true
	}

	return 0, false
}

func (self _NumericEq) Applicable(a Any, b Any) bool {
	_, a_ok := to_float(a)
	_, b_ok := to_float(b)
	return a_ok && b_ok
}

type _ArrayEq struct{}

func (self _ArrayEq) Eq(scope *Scope, a Any, b Any) bool {
	value_a := reflect.ValueOf(a)
	value_b := reflect.ValueOf(b)

	if value_a.Len() != value_b.Len() {
		return false
	}

	for i := 0; i < value_a.Len(); i++ {
		if !scope.eq.Eq(
			scope,
			value_a.Index(i).Interface(),
			value_b.Index(i).Interface()) {
			return false
		}
	}

	return true
}

func is_array(a Any) bool {
	rt := reflect.TypeOf(a)
	return rt.Kind() == reflect.Slice || rt.Kind() == reflect.Array
}

func (self _ArrayEq) Applicable(a Any, b Any) bool {
	return is_array(a) && is_array(b)
}

// Implements Dict equality.
type _DictEq struct{}
func (self _DictEq) Eq(scope *Scope, a Any, b Any) bool {
	return reflect.DeepEqual(a, b)
}

func (self _DictEq) Applicable(a Any, b Any) bool {
	_, a_ok := a.(Dict)
	_, b_ok := b.(Dict)
	return a_ok && b_ok
}

// Less than protocol
type LtProtocol interface {
	Applicable(a Any, b Any) bool
	Lt(scope *Scope, a Any, b Any) bool
}

type _LtDispatcher struct {
	impl []LtProtocol
}

func (self _LtDispatcher) Lt(scope *Scope, a Any, b Any) bool {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return impl.Lt(scope, a, b)
		}
	}
	return false
}

func (self _LtDispatcher) Applicable(a Any, b Any) bool {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return true
		}
	}
	return false
}

func (self *_LtDispatcher) AddImpl(elements ...LtProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _NumericLt struct{}

func (self _NumericLt) Lt(scope *Scope, a Any, b Any) bool {
	a_val, _ := to_float(a)
	b_val, _ := to_float(b)

	return a_val < b_val
}
func (self _NumericLt) Applicable(a Any, b Any) bool {
	_, a_ok := to_float(a)
	_, b_ok := to_float(b)
	return a_ok && b_ok
}

// Add protocol
type AddProtocol interface {
	Applicable(a Any, b Any) bool
	Add(scope *Scope, a Any, b Any) Any
}

type _AddDispatcher struct {
	impl []AddProtocol
}

func (self _AddDispatcher) Add(scope *Scope, a Any, b Any) Any {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return impl.Add(scope, a, b)
		}
	}
	return false
}

func (self *_AddDispatcher) AddImpl(elements ...AddProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _AddStrings struct{}

func (self _AddStrings) Applicable(a Any, b Any) bool {
	_, a_ok := a.(string)
	_, b_ok := b.(string)
	return a_ok && b_ok
}

func (self _AddStrings) Add(scope *Scope, a Any, b Any) Any {
	return a.(string) + b.(string)
}

type _AddFloats struct{}

func (self _AddFloats) Applicable(a Any, b Any) bool {
	_, a_ok := to_float(a)
	_, b_ok := to_float(b)
	return a_ok && b_ok
}

func (self _AddFloats) Add(scope *Scope, a Any, b Any) Any {
	a_val, _ := to_float(a)
	b_val, _ := to_float(b)
	return a_val + b_val
}

// Sub protocol
type SubProtocol interface {
	Applicable(a Any, b Any) bool
	Sub(scope *Scope, a Any, b Any) Any
}

type _SubDispatcher struct {
	impl []SubProtocol
}

func (self _SubDispatcher) Sub(scope *Scope, a Any, b Any) Any {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return impl.Sub(scope, a, b)
		}
	}
	return false
}

func (self *_SubDispatcher) AddImpl(elements ...SubProtocol) {
	for _, impl := range elements {

		self.impl = append(self.impl, impl)
	}
}

type _SubFloats struct{}

func (self _SubFloats) Applicable(a Any, b Any) bool {
	_, a_ok := to_float(a)
	_, b_ok := to_float(b)
	return a_ok && b_ok
}

func (self _SubFloats) Sub(scope *Scope, a Any, b Any) Any {
	a_val, _ := to_float(a)
	b_val, _ := to_float(b)
	return a_val - b_val
}

// Multiply protocol
type MulProtocol interface {
	Applicable(a Any, b Any) bool
	Mul(scope *Scope, a Any, b Any) Any
}

type _MulDispatcher struct {
	impl []MulProtocol
}

func (self _MulDispatcher) Mul(scope *Scope, a Any, b Any) Any {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return impl.Mul(scope, a, b)
		}
	}
	return 0
}

func (self *_MulDispatcher) AddImpl(elements ...MulProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _NumericMul struct{}

func (self _NumericMul) Applicable(a Any, b Any) bool {
	_, a_ok := to_float(a)
	_, b_ok := to_float(b)
	return a_ok && b_ok
}

func (self _NumericMul) Mul(scope *Scope, a Any, b Any) Any {
	a_val, _ := to_float(a)
	b_val, _ := to_float(b)
	return a_val * b_val
}

// Divtiply protocol
type DivProtocol interface {
	Applicable(a Any, b Any) bool
	Div(scope *Scope, a Any, b Any) Any
}

type _DivDispatcher struct {
	impl []DivProtocol
}

func (self _DivDispatcher) Div(scope *Scope, a Any, b Any) Any {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return impl.Div(scope, a, b)
		}
	}
	return 0
}

func (self *_DivDispatcher) AddImpl(elements ...DivProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _NumericDiv struct{}

func (self _NumericDiv) Applicable(a Any, b Any) bool {
	_, a_ok := to_float(a)
	_, b_ok := to_float(b)
	return a_ok && b_ok
}

func (self _NumericDiv) Div(scope *Scope, a Any, b Any) Any {
	a_val, _ := to_float(a)
	b_val, _ := to_float(b)
	if b_val == 0 {
		return false
	}

	return a_val / b_val
}

// Membership protocol
type MembershipProtocol interface {
	Applicable(a Any, b Any) bool
	Membership(scope *Scope, a Any, b Any) bool
}

type _MembershipDispatcher struct {
	impl []MembershipProtocol
}

func (self _MembershipDispatcher) Membership(scope *Scope, a Any, b Any) bool {

	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			return impl.Membership(scope, a, b)
		}
	}

	// Default behavior: Test lhs against each member in RHS -
	// slow but works.
	rt := reflect.TypeOf(b)
	kind := rt.Kind()
	value := reflect.ValueOf(b)
	if kind == reflect.Slice || kind == reflect.Array {
		for i := 0; i < value.Len(); i++ {
			item := value.Index(i).Interface()
			if scope.eq.Eq(scope, a, item) {
				return true
			}
		}
	}

	return false
}

func (self *_MembershipDispatcher) AddImpl(elements ...MembershipProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _SubstringMembership struct{}

func (self _SubstringMembership) Applicable(a Any, b Any) bool {
	_, a_ok := a.(string)
	_, b_ok := b.(string)
	return a_ok && b_ok
}

func (self _SubstringMembership) Membership(scope *Scope, a Any, b Any) bool {
	return strings.Contains(b.(string), a.(string))
}

// Associative protocol.
type AssociativeProtocol interface {
	Applicable(a Any, b Any) bool
	Associative(scope *Scope, a Any, b Any) (Any, bool)
}

type _AssociativeDispatcher struct {
	impl []AssociativeProtocol
}

func (self *_AssociativeDispatcher) Associative(
	scope *Scope, a Any, b Any) (Any, bool) {
	for _, impl := range self.impl {
		if impl.Applicable(a, b) {
			res, pres := impl.Associative(scope, a, b)
			return res, pres
		}
	}


	res, pres := DefaultAssociative{}.Associative(scope, a, b)
	return res, pres
}

func (self *_AssociativeDispatcher) AddImpl(elements ...AssociativeProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

// Last resort associative - uses reflect package to resolve struct
// fields.
type DefaultAssociative struct{}
func (self DefaultAssociative) Applicable(a Any, b Any) bool {
	return false
}

func (self DefaultAssociative) Associative(scope *Scope, a Any, b Any) (Any, bool) {
	switch field_name := b.(type) {
	case string:
		{
			value := reflect.Indirect(reflect.ValueOf(a))
			if value.Kind() == reflect.Struct {
				field_value := value.FieldByName(field_name)
				if field_value.IsValid() && field_value.CanInterface() {
					return field_value.Interface(), true
				}
			} else if value.Kind() == reflect.Slice {
				var result []Any

				for i:=0; i < value.Len(); i++ {
					item := value.Index(i).Interface()
					item, pres := self.Associative(scope, item, b)
					if pres {
						result = append(result, item)
					}
				}

				return result, true
			}
		}

		value := reflect.ValueOf(a)
		method_value := value.MethodByName(field_name)
		if method_value.IsValid() {
			results := method_value.Call([]reflect.Value{})
			if method_value.CanInterface() {
				// In Go, a common pattern is to
				// return value, err. We try to guess
				// here by taking the first return
				// value as the value.
				return results[0].Interface(), true
			}
		}
	}
	return false, false
}


type _DictAssociative struct{}
func (self _DictAssociative) Applicable(a Any, b Any) bool {
	_, a_ok := a.(Dict)
	_, b_ok := b.(string)
	return a_ok && b_ok
}

// Associate object a with key b
func (self _DictAssociative) Associative(scope *Scope, a Any, b Any) (Any, bool) {
	key := b.(string)
	value := a.(Dict)
	res, pres := value[key]
	return res, pres
}

// Regex Match protocol
type RegexProtocol interface {
	Applicable(pattern Any, target Any) bool
	Match(scope *Scope, pattern Any, target Any) bool
}

type _RegexDispatcher struct {
	impl []RegexProtocol
}

func (self _RegexDispatcher) Match(scope *Scope, pattern Any, target Any) bool {
	for _, impl := range self.impl {
		if impl.Applicable(pattern, target) {
			return impl.Match(scope, pattern, target)
		}
	}

	return false
}

func (self *_RegexDispatcher) AddImpl(elements ...RegexProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _SubstringRegex struct{}

func (self _SubstringRegex) Applicable(pattern Any, target Any) bool {
	_, pattern_ok := pattern.(string)
	_, target_ok := target.(string)
	return pattern_ok && target_ok
}

func (self _SubstringRegex) Match(scope *Scope, pattern Any, target Any) bool {
	matched, err := regexp.MatchString(pattern.(string), target.(string))
	if err != nil {
		return false
	}
	return matched
}

type StringProtocol interface {
	ToString(scope *Scope) string
}
