package vfilter

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
)

type FunctionInterface interface {
	Call(ctx context.Context, scope *Scope, args *ordereddict.Dict) Any
	Info(scope *Scope, type_map *TypeMap) *FunctionInfo
}

// A helper function to build a dict within the query.
// e.g. dict(foo=5, bar=6)
type _DictFunc struct{}

func (self _DictFunc) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "dict",
		Doc:  "Construct a dict from arbitrary keyword args.",
	}
}

func (self _DictFunc) Call(ctx context.Context, scope *Scope, args *ordereddict.Dict) Any {
	result := ordereddict.NewDict()
	for _, k := range scope.GetMembers(args) {
		v, _ := args.Get(k)
		lazy_arg, ok := v.(LazyExpr)
		if ok {
			result.Set(k, lazy_arg.Reduce())
		} else {
			result.Set(k, v)
		}
	}
	return result
}

type _TimestampArg struct {
	Epoch       int64 `vfilter:"optional,field=epoch"`
	WinFileTime int64 `vfilter:"optional,field=winfiletime"`
}
type _Timestamp struct{}

func (self _Timestamp) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "timestamp",
		Doc:     "Convert seconds from epoch into a string.",
		ArgType: type_map.AddType(scope, _TimestampArg{}),
	}
}

func (self _Timestamp) Call(ctx context.Context, scope *Scope, args *ordereddict.Dict) Any {
	arg := &_TimestampArg{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("timestamp: %s", err.Error())
		return Null{}
	}

	if arg.Epoch > 0 {
		return time.Unix(arg.Epoch, 0)
	}

	if arg.WinFileTime > 0 {
		return time.Unix((arg.WinFileTime/10000000)-11644473600, 0)
	}

	return Null{}
}

type _SubSelectFunctionArgs struct {
	VQL StoredQuery `vfilter:"required,field=vql"`
}

type _SubSelectFunction struct{}

func (self _SubSelectFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "query",
		Doc:     "Launch a subquery and materialize it into a list of rows.",
		ArgType: type_map.AddType(scope, _SubSelectFunctionArgs{}),
	}
}

func (self _SubSelectFunction) Call(ctx context.Context, scope *Scope, args *ordereddict.Dict) Any {
	arg := _SubSelectFunctionArgs{}
	err := ExtractArgs(scope, args, &arg)
	if err != nil {
		scope.Log("query: %v.", err)
		return Null{}
	}

	return Materialize(ctx, scope, arg.VQL)
}

type _SplitFunctionArgs struct {
	String string `vfilter:"required,field=string,doc=The value to split"`
	Sep    string `vfilter:"required,field=sep,doc=The serparator that will be used to split"`
}
type _SplitFunction struct{}

func (self _SplitFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "split",
		Doc:     "Splits a string into an array based on a regexp separator.",
		ArgType: type_map.AddType(scope, _SplitFunctionArgs{}),
	}
}

func (self _SplitFunction) Call(ctx context.Context, scope *Scope, args *ordereddict.Dict) Any {
	arg := &_SplitFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("split: %s", err.Error())
		return Null{}
	}
	re, err := regexp.Compile(arg.Sep)
	if err != nil {
		scope.Log("split: %s", err.Error())
		return Null{}
	}

	return re.Split(arg.String, -1)
}

type _IfFunctionArgs struct {
	Condition Any      `vfilter:"required,field=condition"`
	Then      LazyExpr `vfilter:"required,field=then"`
	Else      LazyExpr `vfilter:"optional,field=else"`
}

type _IfFunction struct{}

func (self _IfFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "if",
		Doc:     "If condition is true, return the 'then' value otherwise the 'else' value.",
		ArgType: type_map.AddType(scope, _IfFunctionArgs{}),
	}
}

func (self _IfFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *ordereddict.Dict) Any {

	arg := &_IfFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("if: %s", err.Error())
		return Null{}
	}

	if scope.Bool(arg.Condition) {
		return arg.Then.Reduce()
	}

	return arg.Else.Reduce()
}

type _GetFunctionArgs struct {
	Item   Any    `vfilter:"required,field=item"`
	Member string `vfilter:"required,field=member"`
}

type _GetFunction struct{}

func (self _GetFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "get",
		Doc:     "Gets the member field from item.",
		ArgType: type_map.AddType(scope, _GetFunctionArgs{}),
	}
}

func (self _GetFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *ordereddict.Dict) Any {
	arg := &_GetFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("get: %s", err.Error())
		return Null{}
	}

	result := arg.Item
	var next_result Any

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
			return Null{}
		}

		result = next_result
	}

	return result
}

type _EncodeFunctionArgs struct {
	String Any    `vfilter:"required,field=string"`
	Type   string `vfilter:"required,field=type"`
}

type _EncodeFunction struct{}

func (self _EncodeFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "encode",
		Doc:     "Encodes a string as as different type. Currently supported types include 'hex', 'base64'.",
		ArgType: type_map.AddType(scope, _EncodeFunctionArgs{}),
	}
}

func (self _EncodeFunction) Call(
	ctx context.Context,
	scope *Scope,
	args *ordereddict.Dict) Any {
	arg := &_EncodeFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("hex: %s", err.Error())
		return Null{}
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
	return Null{}
}

type LenFunctionArgs struct {
	List Any `vfilter:"required,field=list,doc=A list of items"`
}
type LenFunction struct{}

func (self LenFunction) Call(ctx context.Context,
	scope *Scope,
	args *ordereddict.Dict) Any {
	arg := &LenFunctionArgs{}
	err := ExtractArgs(scope, args, arg)
	if err != nil {
		scope.Log("len: %s", err.Error())
		return &Null{}
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

func (self LenFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name:    "len",
		Doc:     "Returns the length of an object.",
		ArgType: type_map.AddType(scope, &LenFunctionArgs{}),
	}
}
