package vfilter

import (
	"reflect"
)

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
			}

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
