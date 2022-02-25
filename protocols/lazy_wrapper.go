package protocols

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// This is a lazy wrapper around an object providing getters to
// property calls.
type LazyWrapper struct {
	Type         reflect.Type
	IndirectType reflect.Type
	Attributes   []string
	Methods      []string
}

func (self LazyWrapper) Applicable(a types.Any, b types.Any) bool {
	a = maybeReduce(a)
	b = maybeReduce(b)

	_, ok := b.(string)
	if !ok {
		return false
	}

	a_type := reflect.ValueOf(a).Type()
	a_indirect_type := reflect.Indirect(reflect.ValueOf(a)).Type()

	res := a_type == self.Type || a_type == self.IndirectType ||
		a_indirect_type == self.Type || a_indirect_type == self.IndirectType
	return res
}

func (self LazyWrapper) InAttributes(name string) bool {
	for _, x := range self.Attributes {
		if x == name {
			return true
		}
	}
	return false
}

func (self LazyWrapper) GetMembers(scope types.Scope, a types.Any) []string {
	return self.Attributes
}

func (self LazyWrapper) Associative(scope types.Scope, a types.Any, b types.Any) (res types.Any, pres bool) {
	a_value := reflect.Indirect(reflect.ValueOf(a))

	switch field_name := b.(type) {
	case string:
		if !self.InAttributes(field_name) {
			return &types.Null{}, false
		}

		field_value := a_value.FieldByName(field_name)
		if field_value.IsValid() && field_value.CanInterface() {
			if field_value.Kind() == reflect.Ptr && field_value.IsNil() {
				return &types.Null{}, true
			}
			return field_value.Interface(), true
		}

		method_value := reflect.ValueOf(a).MethodByName(field_name)
		if utils.IsCallable(method_value, field_name) {
			if method_value.Type().Kind() == reflect.Ptr {
				method_value = method_value.Elem()
			}

			cb := &LazyFunctionWrapper{cb: func() types.Any {
				results := method_value.Call([]reflect.Value{})

				// In Go, a common pattern is to
				// return (value, err). We try to
				// guess here by taking the first
				// return value as the value.
				if len(results) == 1 || len(results) == 2 {
					res := results[0]
					if res.CanInterface() {
						if res.Kind() == reflect.Ptr && res.IsNil() {
							return &types.Null{}
						}

						return res.Interface()
					}
				}
				return &types.Null{}
			}}
			return cb, true

		}
	}
	return &types.Null{}, false
}

func NewLazyStructWrapper(
	base types.Any, attributes ...string) AssociativeProtocol {
	return &LazyWrapper{
		Type:         reflect.ValueOf(base).Type(),
		IndirectType: reflect.Indirect(reflect.ValueOf(base)).Type(),
		Attributes:   attributes,
	}
}

type LazyFunctionWrapper struct {
	mu sync.Mutex
	cb func() types.Any

	cached types.Any
}

func (self *LazyFunctionWrapper) MarshalJSON() ([]byte, error) {
	return json.Marshal(self.Reduce(context.Background()))
}

func (self *LazyFunctionWrapper) Reduce(ctx context.Context) types.Any {
	self.mu.Lock()
	defer self.mu.Unlock()

	if self.cached == nil {
		self.cached = self.cb()
	}

	return self.cached
}

func (self *LazyFunctionWrapper) ReduceWithScope(
	ctx context.Context, scope types.Scope) types.Any {
	self.mu.Lock()
	defer self.mu.Unlock()

	if self.cached == nil {
		self.cached = self.cb()
	}

	return self.cached
}
