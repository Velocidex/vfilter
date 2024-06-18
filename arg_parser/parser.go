package arg_parser

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/Velocidex/ordereddict"
	errors "github.com/pkg/errors"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
	"www.velocidex.com/golang/vfilter/utils/dict"
)

type tmpTypes struct {
	any     types.Any
	stored  types.StoredQuery
	lazy    types.LazyExpr
	dict    *ordereddict.Dict
	lazyAny types.LazyAny
}

var (
	// A bit of a hack to get the type of interface fields
	testType        = tmpTypes{}
	anyType         = reflect.ValueOf(testType).Type().Field(0).Type
	storedQueryType = reflect.ValueOf(testType).Type().Field(1).Type
	lazyExprType    = reflect.ValueOf(testType).Type().Field(2).Type
	dictExprType    = reflect.ValueOf(testType).Type().Field(3).Type
	lazyAnyType     = reflect.ValueOf(testType).Type().Field(4).Type

	parser_mu      sync.Mutex
	typeDispatcher = initDefaultTypeDispatcher()
)

// Structs may tag fields with this name to control parsing.
const tagName = "vfilter"

type ParserDipatcher func(ctx context.Context,
	scope types.Scope, args *ordereddict.Dict,
	value interface{}) (interface{}, error)

type FieldParser struct {
	Field    string
	FieldIdx int
	Required bool
	Parser   ParserDipatcher
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
		new_value, err := parser.Parser(ctx, scope, args, value)
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

// The plugin may specify the arg as being a LazyExpr, in which case
// it is completely up to it to evaluate the expression (if at all).
// Note: Reducing the lazy expression may yield a StoredQuery - it is
// up to the plugin to handle this case! Generally every
// LazyExpr.Reduce() must be followed by a StoredQuery check. The
// plugin may then choose to either iterate over each StoredQuery row,
// or materialize the StoredQuery into memory (not recommended).
func lazyExprParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	return ToLazyExpr(scope, arg), nil
}

// The target field is a types.StoredQuery - check that what was
// provided is actually one of those.
func storedQueryParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	return ToStoredQuery(ctx, arg), nil
}

// Any fields can accept both storedQuery and LazyExpr but will
// materialize both.
func anyParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {

	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.ReduceWithScope(ctx, scope)
	}

	return arg, nil
}

func lazyAnyParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	return arg, nil
}
func sliceParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
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

func sliceAnyParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	new_value, pres := _ExtractAnyArray(ctx, scope, arg)
	if pres {
		return new_value, nil
	}
	return []interface{}{}, nil
}

func sliceDictParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	new_value, pres := _ExtractAnyArray(ctx, scope, arg)
	if !pres {
		return []interface{}{}, nil
	}

	result := []*ordereddict.Dict{}
	for i := 0; i < len(new_value); i++ {
		item := new_value[i]
		if !types.IsNil(item) {
			result = append(result, dict.RowToDict(ctx, scope, item))
		}
	}

	return result, nil
}

func stringParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
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

	case fmt.Stringer:
		return t.String(), nil

		// Convert integer things to what they normally would look
		// like as a string
	case uint64, int64, uint32, int32, uint16,
		int16, uint8, int8, float64, float32:
		return fmt.Sprintf("%v", arg), nil

	default:
		return fmt.Sprintf("%s", arg), nil
	}
}

func boolParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	return scope.Bool(arg), nil
}

func floatParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	a, ok := utils.ToFloat(arg)
	if ok {
		return a, nil
	}
	return nil, errors.New(fmt.Sprintf("Should be a float not %T.", arg))
}

func int64Parser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	a, ok := utils.ToInt64(arg)
	if ok {
		return a, nil
	}
	return nil, fmt.Errorf("Should be an int not %T.", arg)
}

// The target field is an ordered dict type - just assign it directly.
func dictParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
	lazy_arg, ok := arg.(types.LazyExpr)
	if ok {
		arg = lazy_arg.Reduce(ctx)
	}

	// Build the query args
	env := ordereddict.NewDict()
	if !types.IsNil(arg) {
		// Shortcut for actual dicts
		dict, ok := arg.(*ordereddict.Dict)
		if ok {
			return dict, nil
		}

		// Fallback for dict like things
		for _, member := range scope.GetMembers(arg) {
			v, _ := scope.Associative(arg, member)
			env.Set(member, v)
		}
	}

	return env, nil
}

func uInt64Parser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
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

func intParser(ctx context.Context, scope types.Scope,
	args *ordereddict.Dict, arg interface{}) (interface{}, error) {
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

		// Find a specialized parser for this type.
		parser, pres := typeDispatcher[field_types_value.Type]
		if pres {
			field_parser.Parser = parser
			continue
		}

		// Supported target field types:
		switch field_types_value.Type.Kind() {

		// It is a slice.
		case reflect.Slice:
			target_type := field_types_value.Type.Elem()
			// Currently only support slice of string and slice of any
			if target_type == anyType {
				field_parser.Parser = sliceAnyParser
			} else if target_type == dictExprType {
				field_parser.Parser = sliceDictParser
			} else if target_type.Kind() == reflect.String {
				field_parser.Parser = sliceParser
			} else {
				return nil, fmt.Errorf(
					"Unsupported slice type only []string and []types.Any are supported")
			}
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

		case reflect.Float32:
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

func initDefaultTypeDispatcher() map[reflect.Type]ParserDipatcher {
	result := make(map[reflect.Type]ParserDipatcher)
	result[anyType] = anyParser
	result[lazyAnyType] = lazyAnyParser
	result[storedQueryType] = storedQueryParser
	result[lazyExprType] = lazyExprParser
	result[dictExprType] = dictParser
	return result
}

func RegisterParser(exemplar types.Any, parser ParserDipatcher) {
	parser_mu.Lock()
	defer parser_mu.Unlock()

	type_obj := reflect.ValueOf(exemplar).Type()
	typeDispatcher[type_obj] = parser
}
