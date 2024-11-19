package functions

import (
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Velocidex/ordereddict"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils/dict"
)

// A helper function to build a dict within the query.
// e.g. dict(foo=5, bar=6)
type _DictFunc struct{}

func (self _DictFunc) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name: "dict",
		Doc:  "Construct a dict from arbitrary keyword args.",
	}
}

func (self _DictFunc) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) types.Any {
	return dict.RowToDict(ctx, scope, args)
}

type _TimestampArg struct {
	Epoch       int64 `vfilter:"optional,field=epoch"`
	WinFileTime int64 `vfilter:"optional,field=winfiletime"`
}
type _Timestamp struct{}

func (self _Timestamp) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:    "timestamp",
		Doc:     "Convert seconds from epoch into a string.",
		ArgType: type_map.AddType(scope, _TimestampArg{}),
	}
}

func (self _Timestamp) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) types.Any {
	arg := &_TimestampArg{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
	if err != nil {
		scope.Log("timestamp: %s", err.Error())
		return types.Null{}
	}

	if arg.Epoch > 0 {
		return time.Unix(arg.Epoch, 0)
	}

	if arg.WinFileTime > 0 {
		return time.Unix((arg.WinFileTime/10000000)-11644473600, 0)
	}

	return types.Null{}
}

type _SubSelectFunctionArgs struct {
	VQL types.StoredQuery `vfilter:"required,field=vql"`
}

type _SplitFunctionArgs struct {
	String     string `vfilter:"required,field=string,doc=The value to split"`
	Sep        string `vfilter:"optional,field=sep,doc=The separator that will be used to split"`
	Sep_string string `vfilter:"optional,field=sep_string,doc=The separator as string that will be used to split"`
}
type _SplitFunction struct{}

func (self _SplitFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:    "split",
		Doc:     "Splits a string into an array based on a regexp or string separator.",
		ArgType: type_map.AddType(scope, _SplitFunctionArgs{}),
	}
}

func (self _SplitFunction) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) types.Any {
	arg := &_SplitFunctionArgs{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
	if err != nil {
		scope.Log("split: %s", err.Error())
		return types.Null{}
	}

	if arg.Sep != "" {
		re, err := regexp.Compile(arg.Sep)
		if err != nil {
			scope.Log("split: %s", err.Error())
			return types.Null{}
		}
		return re.Split(arg.String, -1)
	} else if arg.Sep_string != "" {
		return strings.Split(arg.String, arg.Sep_string)
	} else {
		scope.Log("split: requires either sep or sep_string")
		return types.Null{}
	}
}

type _GetFunctionArgs struct {
	Item   types.Any `vfilter:"optional,field=item"`
	Member string    `vfilter:"required,field=member"`
}

type _GetFunction struct{}

func (self _GetFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:    "get",
		Doc:     "Gets the member field from item.",
		ArgType: type_map.AddType(scope, _GetFunctionArgs{}),
	}
}

func (self _GetFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_GetFunctionArgs{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
	if err != nil {
		scope.Log("get: %s", err.Error())
		return types.Null{}
	}

	result := arg.Item
	if result == nil {
		result = scope
	}
	var next_result types.Any

	var pres bool
	for _, member := range strings.Split(arg.Member, ".") {
		int_member, err := strconv.Atoi(member)
		if err == nil {
			// If it looks like an int it might be an
			// index reference.
			next_result, pres = scope.Associative(
				result, int_member)
		} else {
			next_result, pres = scope.Associative(
				result, member)
		}
		if !pres {
			return types.Null{}
		}

		result = next_result
	}

	return result
}

type _EncodeFunctionArgs struct {
	String types.Any `vfilter:"required,field=string"`
	Type   string    `vfilter:"required,field=type"`
}

type _EncodeFunction struct{}

func (self _EncodeFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:    "encode",
		Doc:     "Encodes a string as as different type. Currently supported types include 'hex', 'base64'.",
		ArgType: type_map.AddType(scope, _EncodeFunctionArgs{}),
	}
}

func (self _EncodeFunction) Call(
	ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &_EncodeFunctionArgs{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
	if err != nil {
		scope.Log("hex: %s", err.Error())
		return types.Null{}
	}

	var arg_string string
	switch t := arg.String.(type) {
	case string:
		arg_string = t
	case []byte:
		arg_string = string(t)

	case fmt.Stringer:
		arg_string = fmt.Sprintf("%s", t)

	default:
		arg_string = fmt.Sprintf("%v", t)
	}

	switch arg.Type {
	case "hex":
		return hex.EncodeToString([]byte(arg_string))

	case "string":
		return arg_string

	// Read a UTF16 encoded string and convert it to utf8
	case "utf16":
		codec := unicode.UTF16(unicode.LittleEndian, unicode.UseBOM)
		rd := strings.NewReader(arg_string)
		decoded, err := ioutil.ReadAll(
			transform.NewReader(
				rd, codec.NewDecoder()))
		if err != nil {
			scope.Log("encoder: %s", err.Error())
		}
		return string(decoded)
	default:
		scope.Log("hex: encoding %s not supported.", arg.Type)
	}
	return types.Null{}
}

type LenFunctionArgs struct {
	List types.Any `vfilter:"required,field=list,doc=A list of items"`
}
type LenFunction struct{}

func (self LenFunction) Call(ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) types.Any {
	arg := &LenFunctionArgs{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
	if err != nil {
		scope.Log("len: %s", err.Error())
		return &types.Null{}
	}

	slice := reflect.ValueOf(arg.List)
	// A slice of strings. Only the following are supported
	// https://golang.org/pkg/reflect/#Value.Len
	if slice.Type().Kind() == reflect.Slice ||
		slice.Type().Kind() == reflect.Map ||
		slice.Type().Kind() == reflect.Array ||
		slice.Type().Kind() == reflect.String {
		return slice.Len()
	}

	dict, ok := arg.List.(*ordereddict.Dict)
	if ok {
		return dict.Len()
	}

	return 0
}

func (self LenFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name:    "len",
		Doc:     "Returns the length of an object.",
		ArgType: type_map.AddType(scope, &LenFunctionArgs{}),
	}
}

func Materialize(ctx context.Context,
	scope types.Scope, stored_query types.StoredQuery) []types.Row {
	result := []types.Row{}

	// Materialize both queries to an array.
	new_scope := scope.Copy()
	defer new_scope.Close()

	for item := range stored_query.Eval(ctx, new_scope) {
		result = append(result, item)
	}

	return result
}
