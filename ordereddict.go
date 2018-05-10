package vfilter

import (
	"encoding/json"
	"fmt"
	"github.com/cevaris/ordered_map"
	"reflect"
)

// A concerete implementation of a row - similar to Python's OrderedDict.
type Dict struct {
	*ordered_map.OrderedMap
}

func NewDict() *Dict {
	return &Dict{ordered_map.NewOrderedMap()}
}

func (self *Dict) Set(key string, value Any) *Dict {
	self.OrderedMap.Set(key, value)
	return self
}

func (self *Dict) Get(key string) (Any, bool) {
	return self.OrderedMap.Get(key)
}

func (self *Dict) ToDict() *map[string]Any {
	result := make(map[string]Any)

	iter := self.IterFunc()
	for kv, ok := iter(); ok; kv, ok = iter() {
		result[kv.Key.(string)] = kv.Value
	}

	return &result
}

func (self *Dict) String() string {
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
