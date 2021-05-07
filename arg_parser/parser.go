package arg_parser

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/Velocidex/ordereddict"
	errors "github.com/pkg/errors"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

type tmpTypes struct {
	any    types.Any
	stored types.StoredQuery
	lazy   types.LazyExpr
}

var (
	// A bit of a hack to get the type of interface fields
	testType        = tmpTypes{}
	anyType         = reflect.ValueOf(testType).Type().Field(0).Type
	storedQueryType = reflect.ValueOf(testType).Type().Field(1).Type
	lazyExprType    = reflect.ValueOf(testType).Type().Field(2).Type
)

// Structs may tag fields with this name to control parsing.
const tagName = "vfilter"

type FieldParser struct {
	Field    string
	FieldIdx int
	Required bool
	Parser   func(ctx context.Context, scope types.Scope, value interface{}) (interface{}, error)
}

type Parser struct {
	Fields []*FieldParser
}

func (self *Parser) Parse(
	ctx context.Context, scope types.Scope, args *ordereddict.Dict, target reflect.Value) error {
	parsed := make([]string, 0, args.Len())

	for _, parser := range self.Fields {
		value, pres := args.Get(parser.Field)
		if !pres {
			if parser.Required {
				return fmt.Errorf("Field %s is required", parser.Field)
			}
			continue
		}

		// Keep track of the fields we parsed.
		parsed = append(parsed, parser.Field)

		// Convert the value using the parser
		new_value, err := parser.Parser(ctx, scope, value)
		if err != nil {
			return fmt.Errorf("Field %s %w", parser.Field, err)
		}

		// Now set the field on the struct.
		field_value := target.Field(parser.FieldIdx)
		field_value.Set(reflect.ValueOf(new_value))
	}

	// Something is wrong! We did not extract all the fields from
	// the args, there may be unexpected args.
	if len(parsed) != args.Len() {
		// Slow path should only be taken on error.
		for _, key := range args.Keys() {
			if !utils.InString(&parsed, key) {
				return fmt.Errorf("Unexpected arg %v", key)
			}
		}
	}

	return nil
}

func lazyExprParser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	return ToLazyExpr(scope, arg), nil
}

func storedQueryParser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	return ToStoredQuery(ctx, arg), nil
}

// Any fields can accept both storedQuery and LazyExpr but will
// materialize both.
func anyParser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.ReduceWithScope(ctx, scope)
	}

	return arg, nil
}

func sliceParser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	new_value, pres := _ExtractStringArray(ctx, scope, arg)
	if pres {
		return new_value, nil
	}
	return []interface{}{}, nil
}

func stringParser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	// If we expect a string and we get an array
	// of length 1 of strings, we just take the
	// first element. This allows us to simply
	// coerce a query into a variable without
	// using get.
	if utils.IsArray(arg) {
		new_value, pres := _ExtractStringArray(ctx, scope, arg)
		if pres && len(new_value) == 1 {
			return new_value[0], nil
		}
	}

	switch t := arg.(type) {
	case string:
		return t, nil

	case types.Null, *types.Null, nil:
		return "", nil
	default:
		return fmt.Sprintf("%s", arg), nil
	}
}

func boolParser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	return scope.Bool(arg), nil
}

func floatParser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	a, ok := utils.ToFloat(arg)
	if ok {
		return a, nil
	}
	return nil, errors.New(fmt.Sprintf("Should be a float not %t.", arg))
}

func int64Parser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	a, ok := utils.ToInt64(arg)
	if ok {
		return a, nil
	}
	return nil, errors.New("Should be an int.")
}

func uInt64Parser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	a, ok := utils.ToInt64(arg)
	if ok {
		return uint64(a), nil
	}
	return nil, errors.New("Should be an int.")
}

func intParser(ctx context.Context, scope types.Scope, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	a, ok := utils.ToInt64(arg)
	if ok {
		return int(a), nil
	}
	return nil, errors.New("should be an int.")
}

// Builds a cacheable parser that can parse into
func BuildParser(v reflect.Value) (*Parser, error) {
	t := v.Type()

	if t.Kind() != reflect.Struct {
		return nil, errors.New("Only structs can be set with ExtractArgs()")
	}

	result := &Parser{}

	for i := 0; i < v.NumField(); i++ {
		// Get the field tag value
		field_types_value := t.Field(i)

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
			} else {
				options[directive] = "Y"
			}
		}

		// Is the name specified in the tag?
		field_name, pres := options["field"]
		if !pres {
			field_name = field_types_value.Name
		}

		if field_name == "" {
			panic("Fields can not be empty")
		}

		_, required := options["required"]
		field_parser := &FieldParser{
			Field:    field_name,
			FieldIdx: i,
			Required: required,
		}
		result.Fields = append(result.Fields, field_parser)

		// Now figure out the required type that will go into
		// the value output struct field.
		field_value := v.Field(field_types_value.Index[0])
		if !field_value.IsValid() || !field_value.CanSet() {
			return nil, errors.New(fmt.Sprintf(
				"Field %s is unsettable.", field_name))
		}

		// The plugin may specify the arg as being a LazyExpr,
		// in which case it is completely up to it to evaluate
		// the expression (if at all).  Note: Reducing the
		// lazy expression may yield a StoredQuery - it is up
		// to the plugin to handle this case! Generally every
		// LazyExpr.Reduce() must be followed by a StoredQuery
		// check. The plugin may then choose to either iterate
		// over each StoredQuery row, or materialize the
		// StoredQuery into memory (not recommended).
		if field_types_value.Type == lazyExprType {
			// It is not a types.LazyExpr, we wrap it in one.
			field_parser.Parser = lazyExprParser
			continue
		}

		// The target field is a types.StoredQuery - check that what
		// was provided is actually one of those.
		if field_types_value.Type == storedQueryType {
			field_parser.Parser = storedQueryParser
			continue
		}

		// The target field is an types.Any type - just assign it directly.
		if field_types_value.Type == anyType {
			field_parser.Parser = anyParser
			continue
		}

		// Supported target field types:
		switch field_types_value.Type.Kind() {

		// It is a slice.
		case reflect.Slice:
			field_parser.Parser = sliceParser
			continue

		case reflect.String:
			field_parser.Parser = stringParser
			continue

		case reflect.Bool:
			field_parser.Parser = boolParser
			continue

		case reflect.Float64:
			field_parser.Parser = floatParser
			continue

		case reflect.Int64:
			field_parser.Parser = int64Parser
			continue

		case reflect.Uint64:
			field_parser.Parser = uInt64Parser
			continue

		case reflect.Int:
			field_parser.Parser = intParser
			continue

		default:
			return nil, fmt.Errorf("Unsupported type for field %v", field_name)
		}

	}

	return result, nil
}
