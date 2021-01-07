package types

import (
	"reflect"
	"regexp"
	"strings"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/utils"
)

var (
	field_regex = regexp.MustCompile("field=([a-zA-Z0-9_]+)")
)

func NewTypeMap() *TypeMap {
	return &TypeMap{
		desc: ordereddict.NewDict(),
	}
}

func canonicalTypeName(a_type reflect.Type) string {
	return strings.TrimLeft(a_type.String(), "*[]")
}

func (self *TypeMap) Get(scope Scope, name string) (*TypeDescription, bool) {
	res, pres := self.desc.Get(name)
	if res != nil {
		return res.(*TypeDescription), pres
	}
	return nil, false
}

// Introspect the type of the parameter. Add type descriptor to the
// type map and return the type name.
func (self *TypeMap) AddType(scope Scope, a Any) string {
	// Dont do anything if the caller does not care about a type
	// map.
	if self == nil || scope == nil {
		return ""
	}

	fields := scope.GetMembers(a)
	v := reflect.ValueOf(a)
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	a_type := v.Type()
	self.addType(scope, a_type, &fields)

	return canonicalTypeName(a_type)
}

func (self *TypeMap) addType(scope Scope, a_type reflect.Type, fields *[]string) {
	_, pres := self.desc.Get(canonicalTypeName(a_type))
	if pres {
		return
	}
	result := TypeDescription{
		Fields: ordereddict.NewDict(),
	}
	self.desc.Set(canonicalTypeName(a_type), &result)

	self.addFields(scope, a_type, &result, fields)
	self.addMethods(scope, a_type, &result, fields)
}

func (self *TypeMap) addFields(scope Scope, a_type reflect.Type, desc *TypeDescription,
	fields *[]string) {
	if a_type.Kind() != reflect.Struct {
		return
	}

	for i := 0; i < a_type.NumField(); i++ {
		field_value := a_type.Field(i)

		// Embedded structs just merge their fields with this
		// struct.
		if field_value.Anonymous {
			self.addFields(scope, field_value.Type, desc, fields)
			continue
		}

		// Skip un-exported names.
		if !utils.IsExported(field_value.Name) {
			continue
		}

		// Ignore missing fields.
		if len(*fields) > 0 && !utils.InString(fields, field_value.Name) {
			continue
		}

		return_type := field_value.Type
		return_type_descriptor := TypeReference{
			Target: canonicalTypeName(return_type),
			Tag:    field_value.Tag.Get("vfilter"),
		}

		switch return_type.Kind() {
		case reflect.Array, reflect.Slice:
			element := return_type.Elem()
			self.addType(scope, element, fields)
			return_type_descriptor.Target = canonicalTypeName(
				return_type.Elem())
			return_type_descriptor.Repeated = true

		case reflect.Map, reflect.Ptr:
			element := return_type.Elem()
			self.addType(scope, element, fields)
			return_type_descriptor.Target = canonicalTypeName(
				return_type.Elem())
		}

		name := field_value.Name
		m := field_regex.FindStringSubmatch(return_type_descriptor.Tag)
		if len(m) > 1 {
			name = m[1]
		}

		desc.Fields.Set(name, &return_type_descriptor)
	}
}

func (self *TypeMap) addMethods(scope Scope, a_type reflect.Type,
	desc *TypeDescription, fields *[]string) {
	// If a method has a pointer receiver than we will be able to
	// reflect on its literal type. We need to work on pointers.
	if a_type.Kind() != reflect.Ptr {
		a_type = reflect.PtrTo(a_type)
	}

	for i := 0; i < a_type.NumMethod(); i++ {
		method_value := a_type.Method(i)

		// Skip un-exported names.
		if !utils.IsExported(method_value.Name) {
			continue
		}

		// Ignore missing fields.
		if len(*fields) > 0 && !utils.InString(fields, method_value.Name) {
			continue
		}

		// VFilter only supports calling accessors with no args.
		if !method_value.Func.IsValid() ||
			method_value.Func.Type().NumIn() != 1 {
			continue
		}

		// VFilter only supports methods returning a single
		// value, or possible an error parameter.
		switch method_value.Func.Type().NumOut() {
		case 1, 2:
			return_type := method_value.Func.Type().Out(0)
			return_type_descriptor := TypeReference{
				Target: canonicalTypeName(return_type),
			}

			switch return_type.Kind() {
			case reflect.Array, reflect.Slice:
				element := return_type.Elem()
				self.addType(scope, element, fields)
				return_type_descriptor.Target = canonicalTypeName(
					return_type.Elem())
				return_type_descriptor.Repeated = true

			case reflect.Map, reflect.Ptr:
				element := return_type.Elem()
				self.addType(scope, element, fields)
				return_type_descriptor.Target = canonicalTypeName(
					return_type.Elem())
			}

			desc.Fields.Set(method_value.Name, &return_type_descriptor)
		}
	}
}
