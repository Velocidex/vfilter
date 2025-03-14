package protocols

import (
	"context"
	"reflect"
	"strings"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Associative protocol.
type AssociativeProtocol interface {
	Applicable(a types.Any, b types.Any) bool

	// Returns a value obtained by dereferencing field b from
	// object a. If not present return pres == false and possibly
	// a default value in res. If no default is present res must
	// be nil.
	Associative(scope types.Scope, a types.Any, b types.Any) (res types.Any, pres bool)
	GetMembers(scope types.Scope, a types.Any) []string
}

type AssociativeDispatcher struct {
	impl []AssociativeProtocol
}

func (self AssociativeDispatcher) Copy() AssociativeDispatcher {
	return AssociativeDispatcher{
		append([]AssociativeProtocol{}, self.impl...)}
}

func (self *AssociativeDispatcher) Associative(
	scope types.Scope, a types.Any, b types.Any) (types.Any, bool) {
	ctx := context.Background()

	if types.IsNil(a) {
		return types.Null{}, true
	}

	b_str, ok := utils.ToString(b)
	if ok {
		switch t := a.(type) {
		case types.Scope:
			return t.Resolve(b_str)

		case types.StoredExpression:
			sub_scope := scope.Copy()
			defer sub_scope.Close()
			return scope.Associative(t.Reduce(ctx, sub_scope), b)

		case types.LazyRow:
			return t.Get(b_str)

			// Dereferencing a stroed query expands all
			// fields and extracts a single column
		case types.StoredQuery:
			result := []types.Row{}
			for row := range t.Eval(ctx, scope) {
				field, pres := scope.Associative(row, b_str)
				if pres {
					result = append(result, field)
				} else {
					result = append(result, &types.Null{})
				}
			}
			return result, true

		case *ordereddict.Dict:
			res, pres := t.Get(b_str)
			if !pres {
				default_value := t.GetDefault()
				if default_value != nil {
					return default_value, false
				}
				return nil, false
			}

			// Do not let naked nils to be retrieved from
			// a dict, instead return Null{}
			if res == nil || types.IsNil(res) {
				res = types.Null{}
			}

			return res, pres

		case types.Null, *types.Null, nil:
			return types.Null{}, true
		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, b) {
			scope.GetStats().IncProtocolSearch(i)
			res, pres := impl.Associative(scope, a, b)
			return res, pres
		}
	}
	res, pres := DefaultAssociative{}.Associative(scope, a, b)
	return res, pres
}

func (self *AssociativeDispatcher) GetMembers(
	scope types.Scope, a types.Any) []string {

	switch t := a.(type) {
	case types.Scope:
		return []string{}

	case types.LazyRow:
		return t.Columns()

	case *ordereddict.Dict:
		return t.Keys()

	case types.Null, *types.Null, nil:
		return []string{}

	case types.Memberer:
		return t.Members()
	}

	for i, impl := range self.impl {
		if impl.Applicable(a, "") {
			scope.GetStats().IncProtocolSearch(i)
			return impl.GetMembers(scope, a)
		}
	}
	return DefaultAssociative{}.GetMembers(scope, a)
}

// When adding external protocols they need to be considered before
// any built in protocols so they are able to override the built
// ins. Therefore add them to the front of the protocols array.
func (self *AssociativeDispatcher) AddImpl(elements ...AssociativeProtocol) {
	for _, impl := range elements {
		self.impl = append([]AssociativeProtocol{impl}, self.impl...)
	}
}

// Last resort associative - uses reflect package to resolve struct
// fields.
type DefaultAssociative struct{}

func (self DefaultAssociative) Applicable(a types.Any, b types.Any) bool {
	return false
}

func (self DefaultAssociative) Associative(scope types.Scope, a types.Any, b types.Any) (res types.Any, pres bool) {
	defer func() {
		// If an error occurs we return false - not found.
		recover()
	}()

	a = maybeReduce(a)

	// Handle an int index.
	idx, ok := utils.ToInt64(b)
	if ok {
		a_value := reflect.Indirect(reflect.ValueOf(a))

		// Handle string especially
		switch a_value.Type().Kind() {
		case reflect.String, reflect.Slice:
		default:
			return &types.Null{}, false
		}

		array_length := int64(a_value.Len())

		// Negative index refers to the end of the slice.
		if idx < 0 {
			idx = array_length + idx
		}

		// Index out of bounds - return NULL
		if idx < 0 || idx > array_length {
			return &types.Null{}, false
		}

		value := a_value.Index(int(idx))
		if value.Kind() == reflect.Ptr && value.IsNil() {
			return &types.Null{}, true
		}
		return value.Interface(), true
	}

	switch field_name := b.(type) {

	// Slice sub range can operate on strings or slices
	case []*int64:
		if len(field_name) != 2 {
			return &types.Null{}, true
		}

		a_value := reflect.Indirect(reflect.ValueOf(a))
		if a_value.Type().Kind() == reflect.String {
			value := a_value.String()
			array_length := int64(len(value))
			start_range, end_range := getRanges(field_name, array_length)
			if end_range <= start_range {
				return "", true
			}

			return value[int(start_range):int(end_range)], true
		}

		if a_value.Type().Kind() == reflect.Slice {
			array_length := int64(a_value.Len())
			start_range, end_range := getRanges(field_name, array_length)
			if end_range <= start_range {
				return []types.Any{}, true
			}

			result := make([]types.Any, 0, end_range-start_range)
			for i := start_range; i < end_range; i++ {
				value := a_value.Index(int(i))
				if value.Kind() == reflect.Ptr && value.IsNil() {
					result = append(result, &types.Null{})
				} else {
					result = append(result, value.Interface())
				}
			}
			return result, true
		}

	case string:
		a_value := reflect.Indirect(reflect.ValueOf(a))
		a_type := a_value.Type()

		// A struct with regular exportable field.
		if a_type.Kind() == reflect.Struct {
			field_value := a_value.FieldByNameFunc(FieldMatchName(a_type, field_name))
			if field_value.IsValid() && field_value.CanInterface() {
				if field_value.Kind() == reflect.Ptr && field_value.IsNil() {
					return &types.Null{}, true
				}
				return field_value.Interface(), true
			}
		}

		// A method we call. Usually this is a Getter.
		method_value := reflect.ValueOf(a).MethodByName(field_name)
		if utils.IsCallable(method_value, field_name) {
			if method_value.Type().Kind() == reflect.Ptr {
				method_value = method_value.Elem()
			}

			results := method_value.Call([]reflect.Value{})

			// In Go, a common pattern is to
			// return (value, err). We try to
			// guess here by taking the first
			// return value as the value.
			if len(results) == 1 || len(results) == 2 {
				res := results[0]
				if res.CanInterface() {
					if res.Kind() == reflect.Ptr && res.IsNil() {
						return &types.Null{}, true
					}

					return res.Interface(), true
				}
			}
			return &types.Null{}, true
		}

		// An array - we call Associative on each member.
		if a_type.Kind() == reflect.Slice {
			var result []types.Any

			for i := 0; i < a_value.Len(); i++ {
				element := a_value.Index(i).Interface()
				if item, pres := scope.Associative(element, b); pres {
					result = append(result, item)
				}
			}

			return result, true
		}
	}

	return &types.Null{}, false
}

func FieldMatchName(
	struct_type reflect.Type,
	field_name string) func(in string) bool {
	return func(in string) bool {
		if in == field_name {
			return true
		}

		// If the field has a json tag of this name we also consider
		// it a match.
		field, pres := struct_type.FieldByName(in)
		if pres {
			tag := field.Tag.Get("json")
			json_name, _, _ := strings.Cut(tag, ",")
			if json_name == field_name {
				return true
			}
		}
		return false
	}
}

// Get the members which are callable by VFilter.
func (self DefaultAssociative) GetMembers(scope types.Scope, a types.Any) []string {
	var result []string

	a_value := reflect.Indirect(reflect.ValueOf(a))
	if a_value.Kind() == reflect.Struct {
		for i := 0; i < a_value.NumField(); i++ {
			field_type := a_value.Type().Field(i)
			if utils.IsExported(field_type.Name) {
				result = append(result, field_type.Name)
			}
		}
	}

	a_value = reflect.ValueOf(a)

	// If a value is a slice, we get the members of the
	// first item. Hopefully they are the same as the
	// other items. A common use case is storing the
	// output of a query in the scope environment and then
	// selecting from it, in which case the value is a
	// list of Rows, each row has a Dict.
	if a_value.Type().Kind() == reflect.Slice {
		for i := 0; i < a_value.Len(); i++ {
			return scope.GetMembers(a_value.Index(i).Interface())
		}
	}

	for i := 0; i < a_value.NumMethod(); i++ {
		method_type := a_value.Type().Method(i)
		method_value := a_value.Method(i)
		if utils.IsCallable(method_value, method_type.Name) {
			result = append(result, method_type.Name)
		}
	}

	return result
}
