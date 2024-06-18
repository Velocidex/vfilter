// Utility functions for extracting and validating inputs to functions
// and plugins.
package arg_parser

import (
	"context"
	"fmt"
	"reflect"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

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

// Deprecate this in favor of ExtractArgsWithContext
func ExtractArgs(scope types.Scope, args *ordereddict.Dict, target interface{}) error {
	v := reflect.ValueOf(target)
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	parser, err := GetParser(v)
	if err != nil {
		scope.Explainer().ParseArgs(args, target, err)
		return err
	}

	err = parser.Parse(context.Background(), scope, args, v)
	scope.Explainer().ParseArgs(args, target, err)
	return err
}

func ExtractArgsWithContext(
	ctx context.Context, scope types.Scope, args *ordereddict.Dict, target interface{}) error {
	v := reflect.ValueOf(target)
	if v.Type().Kind() == reflect.Ptr {
		v = v.Elem()
	}

	parser, err := GetParser(v)
	if err != nil {
		scope.Explainer().ParseArgs(args, target, err)
		return err
	}

	err = parser.Parse(ctx, scope, args, v)
	scope.Explainer().ParseArgs(args, target, err)
	return err
}

// Try to retrieve an arg name from the Dict of args. Coerce the arg
// into something resembling a list of strings.
func _ExtractStringArray(
	ctx context.Context, scope types.Scope, arg types.Any) ([]string, bool) {
	var result []string

	// Handle potentially lazy args.
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	slice := reflect.ValueOf(arg)

	// A slice of strings.
	if slice.Type().Kind() == reflect.Slice {
		for i := 0; i < slice.Len(); i++ {
			value := slice.Index(i).Interface()
			item, ok := utils.ToString(value)
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
					item, ok := utils.ToString(member)
					if ok {
						result = append(result, item)
					}
				}
			}

			// Represent the value as a string.
			result = append(result, fmt.Sprintf("%v", value))
		}
		return result, true
	}

	// A single string just expands into a list of length 1.
	item, ok := utils.ToString(slice.Interface())
	if !ok {
		// If it has no StringProtocol fall back to golang
		// default.
		item = fmt.Sprintf("%v", slice.Interface())
	}
	result = append(result, item)
	return result, true
}

func _ExtractAnyArray(
	ctx context.Context, scope types.Scope, arg types.Any) ([]types.Any, bool) {
	var result []types.Any

	// Handle potentially lazy args.
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	slice := reflect.ValueOf(arg)

	// A slice of strings.
	if slice.Type().Kind() == reflect.Slice {
		for i := 0; i < slice.Len(); i++ {
			value := slice.Index(i).Interface()
			result = append(result, value)
		}
		return result, true
	}

	result = append(result, slice.Interface())
	return result, true
}

// Convert a type to a stored query
func ToStoredQuery(ctx context.Context, arg types.Any) types.StoredQuery {
	switch t := arg.(type) {
	case types.LazyExpr:
		return ToStoredQuery(ctx, t.Reduce(ctx))

	case types.StoredQuery:
		return t
	default:
		return &storedQueryWrapper{arg}
	}
}

type storedQueryWrapper struct {
	value types.Any
}

func (self *storedQueryWrapper) Eval(ctx context.Context, scope types.Scope) <-chan types.Row {
	output_chan := make(chan types.Row)
	go func() {
		defer close(output_chan)

		slice := reflect.ValueOf(self.value)
		if slice.Type().Kind() == reflect.Slice {
			for i := 0; i < slice.Len(); i++ {
				value := slice.Index(i).Interface()
				if !types.IsNil(value) {
					select {
					case <-ctx.Done():
						return
					case output_chan <- self.toRow(scope, value):
					}
				}
			}
		} else {
			row_value := self.toRow(scope, self.value)
			if !types.IsNil(row_value) {
				select {
				case <-ctx.Done():
					return
				case output_chan <- row_value:
				}
			}
		}

	}()
	return output_chan
}

func (self *storedQueryWrapper) toRow(scope types.Scope, value types.Any) types.Row {
	if types.IsNil(value) {
		return types.Null{}
	}

	members := scope.GetMembers(value)
	if len(members) > 0 {
		return value
	}

	return ordereddict.NewDict().Set("_value", value)
}

// Wrap an arg in a LazyExpr for plugins that want to receive a
// LazyExpr.
func ToLazyExpr(scope types.Scope, arg types.Any) types.LazyExpr {
	switch t := arg.(type) {
	case types.LazyExpr:
		return t

	case types.StoredQuery:
		return &StoredQueryWrapperLazyExpression{query: t}
	default:
		return &LazyExpressionWrapper{arg}
	}
}

// Wrap a Stored Query with a LazyExpr interface. Callers will receive
// the Stored Query when reducing us.
type StoredQueryWrapperLazyExpression struct {
	query types.StoredQuery
}

func (self *StoredQueryWrapperLazyExpression) Delegate() types.StoredQuery {
	return self.query
}

func (self *StoredQueryWrapperLazyExpression) ReduceWithScope(
	ctx context.Context, scope types.Scope) types.Any {
	return scope.Materialize(ctx, "", self.query)
}

func (self *StoredQueryWrapperLazyExpression) Reduce(ctx context.Context) types.Any {
	return self.query
}

type LazyExpressionWrapper struct {
	value types.Any
}

func (self *LazyExpressionWrapper) Delegate() types.Any {
	return self.value
}

func (self *LazyExpressionWrapper) ReduceWithScope(ctx context.Context, scope types.Scope) types.Any {
	return self.value
}

func (self *LazyExpressionWrapper) Reduce(ctx context.Context) types.Any {
	return self.value
}
