package vfilter

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/cevaris/ordered_map"
)

// A concerete implementation of a row - similar to Python's OrderedDict.
type Dict struct {
	sync.Mutex

	*ordered_map.OrderedMap
	default_value    Any
	case_insensitive bool
}

func NewDict() *Dict {
	return &Dict{OrderedMap: ordered_map.NewOrderedMap()}
}

func (self *Dict) IsCaseInsensitive() bool {
	self.Lock()
	defer self.Unlock()
	return self.case_insensitive
}

func (self *Dict) MergeFrom(other *Dict) {
	iter := other.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() {
		key := kv.Key.(string)
		self.Set(key, kv.Value)
	}
}

func (self *Dict) SetDefault(value Any) *Dict {
	self.Lock()
	defer self.Unlock()

	self.default_value = value
	return self
}

func (self *Dict) GetDefault() Any {
	self.Lock()
	defer self.Unlock()

	return self.default_value
}

func (self *Dict) SetCaseInsensitive() *Dict {
	self.Lock()
	defer self.Unlock()

	self.case_insensitive = true
	return self
}

func (self *Dict) Set(key string, value Any) *Dict {
	self.Lock()
	defer self.Unlock()

	self.OrderedMap.Set(key, value)
	return self
}

func (self *Dict) Get(key string) (Any, bool) {
	self.Lock()
	defer self.Unlock()

	return self.OrderedMap.Get(key)
}

func (self *Dict) ToDict() *map[string]Any {
	self.Lock()
	defer self.Unlock()

	result := make(map[string]Any)

	iter := self.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() {
		result[kv.Key.(string)] = kv.Value
	}

	return &result
}

func (self *Dict) String() string {
	self.Lock()
	defer self.Unlock()

	builder := make([]string, self.Len())

	var index int = 0
	iter := self.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() {
		val, _ := self.Get(kv.Key.(string))
		builder[index] = fmt.Sprintf("%v:%v, ", kv.Key, val)
		index++
	}
	return fmt.Sprintf("Dict%v", builder)
}

func (self *Dict) GoString() string {
	return self.String()
}

func (self *Dict) MarshalJSON() ([]byte, error) {
	res, err := json.Marshal(self.ToDict())
	return res, err
}

// Protocols:

// Implements Dict equality.
type _DictEq struct{}

func (self _DictEq) Eq(scope *Scope, a Any, b Any) bool {
	return reflect.DeepEqual(a, b)
}

func (self _DictEq) Applicable(a Any, b Any) bool {
	switch a.(type) {
	case Dict, *Dict:
		break
	default:
		return false
	}

	switch b.(type) {
	case Dict, *Dict:
		break
	default:
		return false
	}

	return true
}

type _DictAssociative struct{}

func (self _DictAssociative) Applicable(a Any, b Any) bool {
	switch a.(type) {
	case Dict, *Dict:
		break
	default:
		return false
	}

	switch b.(type) {
	case string:
		break
	default:
		return false
	}

	return true
}

// Associate object a with key b
func (self _DictAssociative) Associative(scope *Scope, a Any, b Any) (Any, bool) {
	key := b.(string)
	var value *Dict

	switch t := a.(type) {
	case Dict:
		value = &t

	case *Dict:
		value = t

	default:
		return nil, false
	}

	res, pres := value.Get(key)
	if !pres {
		if value.IsCaseInsensitive() {
			lower_case_key := strings.ToLower(key)
			for _, member := range scope.GetMembers(value) {
				if strings.ToLower(member) == lower_case_key {
					value, pres := scope.Associative(value, member)
					return value, pres
				}
			}

		}

		// Return the default value but still indicate the
		// value is not present.
		default_value := value.GetDefault()
		if default_value != nil {
			return default_value, false
		}
	}
	return res, pres
}

func (self _DictAssociative) GetMembers(scope *Scope, a Any) []string {
	var result []string

	var value *Dict
	switch t := a.(type) {
	case Dict:
		value = &t

	case *Dict:
		value = t

	default:
		return result
	}

	iter := value.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() {
		result = append(result, kv.Key.(string))
	}

	return result
}

type _BoolDict struct{}

func (self _BoolDict) Applicable(a Any) bool {
	switch a.(type) {
	case Dict, *Dict:
		return true
	}
	return false
}

func (self _BoolDict) Bool(scope *Scope, a Any) bool {
	switch t := a.(type) {
	case Dict:
		return t.Len() > 0

	case *Dict:
		return t.Len() > 0

	}
	return false
}
