package vfilter

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// A concerete implementation of a row - similar to Python's
// OrderedDict.  Main difference is that delete is not implemented -
// we just preserve the order of insertions.
type Dict struct {
	sync.Mutex

	store map[string]interface{}
	keys  []string

	default_value    Any
	case_insensitive bool
}

func NewDict() *Dict {
	return &Dict{
		store: make(map[string]interface{}),
	}
}

func (self *Dict) IsCaseInsensitive() bool {
	self.Lock()
	defer self.Unlock()

	return self.case_insensitive
}

func (self *Dict) MergeFrom(other *Dict) {
	for _, key := range other.keys {
		value, pres := other.Get(key)
		if pres {
			self.Set(key, value)
		}
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

func remove(s []string, r string) []string {
	for i, v := range s {
		if v == r {
			return append(s[:i], s[i+1:]...)
		}
	}
	return s
}

func (self *Dict) Set(key string, value Any) *Dict {
	self.Lock()
	defer self.Unlock()

	// O(n) but for our use case this is faster since Dicts are
	// typically small and we rarely overwrite a key.
	_, pres := self.store[key]
	if pres {
		self.keys = append(remove(self.keys, key), key)
	} else {
		self.keys = append(self.keys, key)
	}

	self.store[key] = value

	return self
}

func (self *Dict) Len() int {
	return len(self.store)
}

func (self *Dict) Get(key string) (Any, bool) {
	self.Lock()
	defer self.Unlock()

	val, ok := self.store[key]
	if !ok && self.default_value != nil {
		return self.GetDefault(), false
	}

	return val, ok
}

func (self *Dict) ToDict() *map[string]Any {
	self.Lock()
	defer self.Unlock()

	result := make(map[string]Any)

	for _, key := range self.keys {
		value, pres := self.store[key]
		if pres {
			result[key] = value
		}
	}

	return &result
}

func (self *Dict) String() string {
	self.Lock()
	defer self.Unlock()

	builder := make([]string, self.Len())

	var index int = 0
	for _, key := range self.keys {
		val, _ := self.store[key]
		builder[index] = fmt.Sprintf("%v:%v, ", key, val)
		index++
	}
	return fmt.Sprintf("Dict%v", builder)
}

func (self *Dict) GoString() string {
	return self.String()
}

func (self *Dict) MarshalJSON() ([]byte, error) {
	result := make(map[string]json.RawMessage)

	for _, key := range self.keys {
		val, pres := self.store[key]
		if !pres {
			continue
		}
		serialized, err := json.Marshal(val)
		if err != nil {
			serialized = []byte("null")
		}
		result[key] = json.RawMessage(serialized)
	}

	res, err := json.Marshal(result)
	return res, err
}

// Protocols:

// Implements Dict equality.
type _DictEq struct{}

func (self _DictEq) Eq(scope *Scope, a Any, b Any) bool {
	return reflect.DeepEqual(a, b)
}

func to_dict(a Any) (*Dict, bool) {
	switch t := a.(type) {
	case Dict:
		return &t, true
	case *Dict:
		return t, true
	default:
		return nil, false
	}
}

func (self _DictEq) Applicable(a Any, b Any) bool {
	_, a_ok := to_dict(a)
	_, b_ok := to_dict(b)

	return a_ok && b_ok
}

type _DictAssociative struct{}

func (self _DictAssociative) Applicable(a Any, b Any) bool {
	_, a_ok := to_dict(a)
	_, b_ok := to_string(b)

	return a_ok && b_ok
}

// Associate object a with key b
func (self _DictAssociative) Associative(scope *Scope, a Any, b Any) (Any, bool) {
	key, _ := to_string(b)
	value, _ := to_dict(a)

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
	value, ok := to_dict(a)
	if !ok {
		return nil
	}

	return value.keys
}

type _BoolDict struct{}

func (self _BoolDict) Applicable(a Any) bool {
	_, a_ok := to_dict(a)

	return a_ok
}

func (self _BoolDict) Bool(scope *Scope, a Any) bool {
	value, ok := to_dict(a)
	if !ok {
		return false
	}

	return value.Len() > 0
}
