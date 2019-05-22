// Utility functions for extracting and validating inputs to functions
// and plugins.
package vfilter

import (
	"fmt"
	"reflect"
	"strings"

	errors "github.com/pkg/errors"
)

// Structs may tag fields with this name to control parsing.
const tagName = "vfilter"

// Extract the content of args into the struct value. Value's members
// should be tagged with the "vfilter" tag.

// This function makes it very easy to extract args into VQL plugins
// or functions. Simply declare an args struct:

// type MyArgs struct {
//    Field string `vfilter:"required,field=field_name"`
// }

// And parse the struct using this function:
// myarg := &MyArgs{}
// err := vfilter.ExtractArgs(scope, args, myarg)

// We will raise an error if a required field is missing or has the
// wrong type of args.

// NOTE: In order for the field to be populated by this function, the
// field must be exported (i.e. name begins with cap) and it must have
// vfilter tags.
func ExtractArgs(scope *Scope, args *Dict, value interface{}) error {
	v := reflect.ValueOf(value)

	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	for i := 0; i < v.NumField(); i++ {
		// Get the field tag value
		field_types_value := v.Type().Field(i)
		tag := field_types_value.Tag.Get(tagName)

		// Skip if tag is not defined or ignored
		if tag == "" || tag == "-" {
			continue
		}

		directives := strings.Split(tag, ",")
		options := make(map[string]string)
		for _, directive := range directives {
			if strings.Contains(directive, "=") {
				components := strings.Split(directive, "=")
				if len(components) >= 2 {
					options[components[0]] = components[1]
				}
			}

		}
		field_name, pres := options["field"]
		if !pres {
			field_name = field_types_value.Name
		}

		if field_name == "" {
			panic("Fields can not be empty")
		}

		// Get the field. If it is not present but is
		// required, it is an error.
		arg, pres := args.Get(field_name)
		if !pres {
			if InString(&directives, "required") {
				return errors.New(fmt.Sprintf(
					"Field %s is required.", field_name))
			}
			// Field is optional and not provided.
			continue
		}

		// Now cast the arg into the correct type to go into
		// the value output struct.
		field_value := v.Field(field_types_value.Index[0])
		if !field_value.IsValid() || !field_value.CanSet() {
			return errors.New(fmt.Sprintf(
				"Field %s is unsettable.", field_name))
		}

		// The plugin may specify the arg as being a LazyExpr,
		// in which case it is completely up to it to evaluate
		// the expression (if at all).
		if field_types_value.Type.String() == "vfilter.LazyExpr" {
			field_value.Set(reflect.ValueOf(arg))
			continue
		}

		// From here below, arg has to be non-lazy so we can
		// deal with its materialized form.
		lazy_arg, ok := arg.(LazyExpr)
		if ok {
			arg = lazy_arg.Reduce()
		}

		// The target field is a StoredQuery - check that what
		// was provided is actually one of those.
		if field_types_value.Type.String() == "vfilter.StoredQuery" {
			stored_query, ok := arg.(StoredQuery)
			if !ok {
				return errors.New(fmt.Sprintf(
					"Field %s should be a query.",
					field_types_value.Name))
			}

			field_value.Set(reflect.ValueOf(stored_query))
			continue
		}

		// The target field is an Any type - just assign it directly.
		if field_types_value.Type.String() == "vfilter.Any" {
			// Evaluate the expression.
			field_value.Set(reflect.ValueOf(arg))
			continue
		}

		// Supported target field types:
		switch field_types_value.Type.Kind() {

		// It is a slice.
		case reflect.Slice:
			new_value, pres := _ExtractStringArray(scope, field_name, args)
			if pres {
				field_value.Set(reflect.ValueOf(new_value))
			}

		case reflect.String:
			switch t := arg.(type) {
			case string:
				field_value.Set(reflect.ValueOf(t))
			case Null, *Null, nil:
				continue
			default:
				field_value.Set(reflect.ValueOf(
					fmt.Sprintf("%s", arg)))
			}

		case reflect.Bool:
			a, ok := arg.(bool)
			if ok {
				field_value.Set(reflect.ValueOf(a))
			}

		case reflect.Float64:
			a, ok := to_float(arg)
			if ok {
				field_value.Set(reflect.ValueOf(a))
			}

		case reflect.Int64:
			a, ok := to_int64(arg)
			if ok {
				field_value.Set(reflect.ValueOf(a))
			}

		case reflect.Int:
			a, ok := to_int64(arg)
			if ok {
				field_value.Set(reflect.ValueOf(int(a)))
			}

		default:
			if InString(&directives, "required") {
				return errors.New(fmt.Sprintf(
					"Field %s is required.", field_name))
			}
		}
	}

	return nil
}

// Try to retrieve an arg name from the Dict of args. Coerce the arg
// into something resembling a list of strings.
func _ExtractStringArray(scope *Scope, name string, args *Dict) ([]string, bool) {
	var result []string
	arg, ok := (*args).Get(name)
	if !ok {
		return nil, false
	}

	// Handle potentially lazy args.
	lazy_arg, ok := arg.(LazyExpr)
	if ok {
		arg = lazy_arg.Reduce()
	}

	slice := reflect.ValueOf(arg)
	// A slice of strings.
	if slice.Type().Kind() == reflect.Slice {
		for i := 0; i < slice.Len(); i++ {
			value := slice.Index(i).Interface()
			item, ok := value.(string)
			if ok {
				result = append(result, item)
				continue
			}

			// If is a dict with only one member just use
			// that one.
			members := scope.GetMembers(value)
			if len(members) == 1 {
				member, ok := scope.Associative(value, members[0])
				if ok {
					item, ok := member.(string)
					if ok {
						result = append(result, item)
					}
				}
			}
		}
		return result, true
	}

	// A single string just expands into a list of length 1.
	item, ok := slice.Interface().(string)
	if ok {
		result = append(result, item)
		return result, true
	}

	return nil, false
}
