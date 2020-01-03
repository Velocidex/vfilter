package vfilter

/* This file implements the explain algorithm.

We use reflection to explain all VQL extensions.
*/
import (
	"reflect"
	"regexp"
	"strings"

	"github.com/Velocidex/ordereddict"
)

var (
	field_regex = regexp.MustCompile("field=([a-zA-Z0-9_]+)")
)

// Populated with information about the scope.
type ScopeInformation struct {
	Plugins   []*PluginInfo
	Functions []*FunctionInfo
}

// Describes the specific plugin.
type PluginInfo struct {
	// The name of the plugin.
	Name string

	// A helpful description about the plugin.
	Doc string

	ArgType string

	// A hint about the type we return for each row. This is a
	// reference into the relevant type_map. It may be an empty
	// string if the plugin has no idea what type it will produce
	// for example if it relays output from other plugins.
	RowType string
}

// Describe functions.
type FunctionInfo struct {
	Name    string
	Doc     string
	ArgType string

	// This is true for functions which operate on aggregates
	// (i.e. group by). For any columns which contains such a
	// function, vfilter will first run the group by clause then
	// re-evaluate the function on the aggregate column.
	IsAggregate bool
}

// Describe a type. This is meant for human consumption so it does not
// need to be so accurate. Fields is a map between the Associative
// member and the type that is supposed to be returned. Note that
// Velocifilter automatically calls accessor methods so they look like
// standard exported fields.
type TypeDescription struct {
	Fields *ordereddict.Dict
}

// This describes what type is returned when we reference this field
// from the TypeDescription.
type TypeReference struct {
	Target   string
	Repeated bool
	Tag      string
}

// Map between type name and its description.
type TypeMap struct {
	desc *ordereddict.Dict
}

func NewTypeMap() *TypeMap {
	return &TypeMap{
		desc: ordereddict.NewDict(),
	}
}

func canonicalTypeName(a_type reflect.Type) string {
	return strings.TrimLeft(a_type.String(), "*[]")
}

func (self *TypeMap) Get(scope *Scope, name string) (*TypeDescription, bool) {
	res, pres := self.desc.Get(name)
	if res != nil {
		return res.(*TypeDescription), pres
	}
	return nil, false
}

// Introspect the type of the parameter. Add type descriptor to the
// type map and return the type name.
func (self *TypeMap) AddType(scope *Scope, a Any) string {
	if scope == nil {
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

func (self *TypeMap) addType(scope *Scope, a_type reflect.Type, fields *[]string) {
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

func (self *TypeMap) addFields(scope *Scope, a_type reflect.Type, desc *TypeDescription,
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
		if !is_exported(field_value.Name) {
			continue
		}

		// Ignore missing fields.
		if len(*fields) > 0 && !InString(fields, field_value.Name) {
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

func (self *TypeMap) addMethods(scope *Scope, a_type reflect.Type,
	desc *TypeDescription, fields *[]string) {
	// If a method has a pointer receiver than we will be able to
	// reflect on its literal type. We need to work on pointers.
	if a_type.Kind() != reflect.Ptr {
		a_type = reflect.PtrTo(a_type)
	}

	for i := 0; i < a_type.NumMethod(); i++ {
		method_value := a_type.Method(i)

		// Skip un-exported names.
		if !is_exported(method_value.Name) {
			continue
		}

		// Ignore missing fields.
		if len(*fields) > 0 && !InString(fields, method_value.Name) {
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
