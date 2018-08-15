// Utility functions for extracting and validating inputs to functions
// and plugins.
package vfilter

import (
	"fmt"
	errors "github.com/pkg/errors"
	"reflect"
	"strings"
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
// field my be exported (i.e. name begins with cap) and it must have
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

		arg, pres := args.Get(field_name)
		if !pres && InString(&directives, "required") {
			return errors.New(fmt.Sprintf(
				"Field %s is required.", field_name))
		}

		field_value := v.Field(field_types_value.Index[0])
		if !field_value.IsValid() || !field_value.CanSet() {
			return errors.New(fmt.Sprintf(
				"Field %s is unsettable.", field_name))
		}

		// It is a slice.
		switch field_types_value.Type.Kind() {

		case reflect.Slice:
			new_value, pres := ExtractStringArray(scope, field_name, args)
			if !pres && InString(&directives, "required") {
				return errors.New(fmt.Sprintf(
					"Field %s is a required string array.",
					field_types_value.Name))
			}
			field_value.Set(reflect.ValueOf(new_value))
		case reflect.String:
			a, ok := arg.(string)
			if !ok {
				a = fmt.Sprintf("%v", arg)
			}
			field_value.Set(reflect.ValueOf(a))

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

func ExtractString(name string, args *Dict) (*string, bool) {
	if arg, pres := args.Get(name); pres {
		if arg_string, ok := arg.(string); ok {
			return &arg_string, true
		}
	}

	return nil, false
}

func ExtractFloat(output *float64, name string, args *Dict) bool {
	if arg, pres := args.Get(name); pres {
		if arg_float, ok := to_float(arg); ok {
			*output = arg_float
			return true
		}
	}

	return false
}

// Try to retrieve an arg name from the Dict of args. Coerce the arg
// into something resembling a list of strings.
func ExtractStringArray(scope *Scope, name string, args *Dict) ([]string, bool) {
	var result []string
	arg, ok := (*args).Get(name)
	if !ok {
		return nil, false
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

func ExtractStoredQuery(scope *Scope, name string, args *Dict) (
	StoredQuery, bool) {
	arg, ok := (*args).Get(name)
	if !ok {
		return nil, false
	}

	// Its already a stored query, just return it.
	stored_query_arg, ok := arg.(StoredQuery)
	if ok {
		return stored_query_arg, true
	}

	// Wrap in a stored query to return that.
	return &StoredQueryWrapper{arg}, true
}
