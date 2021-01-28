package arg_parser_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/sebdah/goldie/v2"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/scope"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

type vqlTest struct {
	name string
	vql  string
}

var multiVQLTest = []vqlTest{
	// Basic types are just passed as args.
	{"Parse basic types", `SELECT parse(r=1, int=1, string='hello') FROM scope()`},

	// Lazy expressions are expanded for basic types.
	{"Parse basic types", `
LET X = 5
SELECT parse(r=1, int=X) FROM scope()`},

	{"Parse basic types with param", `
LET Foo(X) = 1+X
SELECT parse(r=1, int=Foo(X=2)) FROM scope()`},

	// Error handling
	{"Passing Stored query to int field", `
LET Foo = SELECT 1 FROM scope()
SELECT parse(r=1, int=Foo) FROM scope()`},

	{"Passing string to int field", `
LET Foo = "Hello"
SELECT parse(r=1, int=Foo) FROM scope()`},

	// String Array
	{"String Array", `
SELECT parse(r=1, string_array=["X", "Y"]) FROM scope()`},

	// Passing a string into a plugin that expects a string array
	// creates an array on the fly.
	{"String Array with single field", `
SELECT parse(r=1, string_array="Hello") FROM scope()`},

	// String array stringifies if possible
	{"String Array getting int array stringifies it", `
SELECT parse(r=1, string_array=[1,]) FROM scope()`},
	{"String Array getting int stringifies it", `
SELECT parse(r=1, string_array=1) FROM scope()`},

	{"String Array with single field", `
LET Foo = "Hello"
SELECT parse(r=1, string_array=Foo) FROM scope()`},

	// Foo.X will expand into a list by virtue of the Associative
	// protocol.
	{"String Array with stored query expanding a row", `
LET Foo = SELECT "Hello" AS X FROM scope()
SELECT parse(r=1,string_array=Foo.X) FROM scope()`},

	// String array stringifies if possible
	{"String Array with stored query expanding a row of ints", `
LET Foo = SELECT 1 AS X FROM scope()
SELECT parse(r=1,string_array=Foo.X) FROM scope()`},

	// When accepting a lazy expression it is up to the plugin to
	// decide if to reduce it.
	{"Lazy expressions", `
LET lazy_expr = 1
SELECT parse(r=1,lazy=lazy_expr) FROM scope()`},

	{"Lazy expressions with parameters", `
LET lazy_expr(X) = X + 1
SELECT parse(r=1,lazy=lazy_expr(X=1)) FROM scope()`},

	// A plugin that accepts a LazyExpr may receive a StoredQuery
	// after reducing it. The StoredQuery will not be
	// automatically materialized - the plugin needs to expand it
	// by itself. Plugins that accept a LazyExpr must always check
	// to see if the expression is actually a StoredQuery and if
	// it should be expanded in memory.
	{"Lazy expressions of stored query", `
LET query = SELECT 1 FROM scope()
SELECT parse(r=1,lazy=query) FROM scope()`},

	{"Lazy expressions of stored query with parameters", `
LET X = 5    -- Verify this is masked
LET query(X) = SELECT X FROM scope()
SELECT parse(r=1,lazy=query(X=2)) FROM scope()`},

	{"Stored query", `
LET query = SELECT 1 FROM scope()
SELECT parse(r=1,query=query) FROM scope()`},

	{"Stored query with parameters", `
LET X = 5    -- Verify this is masked
LET query(X) = SELECT X FROM scope()
SELECT parse(r=1,query=query(X=2)) FROM scope()`},

	// A plugin that expects a stored query will received a
	// wrapper if the user passed a regular object.
	{"Stored query given a constant", `
SELECT parse(r=1,query="hello") FROM scope()`},

	{"Stored query given a dict", `
SELECT parse(r=1,query=dict(X="hello")) FROM scope()`},

	{"Stored query given an expression", `
LET X = 1
SELECT parse(r=1,query=X) FROM scope()`},

	// Plugins that accept Any have lazy expressions materialized
	// on function call.
	{"Any type", `
LET X = 1
SELECT parse(r=1,any=X) FROM scope()`},

	{"Any type", `
LET Foo(X) = X + 1
SELECT parse(r=1,any=Foo(X=1)) FROM scope()`},

	// Any fields receive stored queries unexpanded.
	{"Any type", `
LET query = SELECT 1 FROM scope()
SELECT parse(r=1,any=query) FROM scope()`},

	// Unexpected args
	{"Unexpected args", `
SELECT parse(r=1,int=1, foobar=2) FROM scope()`},

	{"Required args", `
SELECT parse() FROM scope()`},
}

type argFuncArgs struct {
	Any         types.Any         `vfilter:"optional,field=any"`
	LazyExpr    types.LazyExpr    `vfilter:"optional,field=lazy"`
	Int         int               `vfilter:"optional,field=int"`
	String      string            `vfilter:"optional,field=string"`
	StringArray []string          `vfilter:"optional,field=string_array"`
	StoredQuery types.StoredQuery `vfilter:"optional,field=query"`
	R           int               `vfilter:"required,field=r"`
}

type argFunc struct{}

func (self argFunc) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) types.Any {
	arg := argFuncArgs{}
	err := arg_parser.ExtractArgs(scope, args, &arg)
	if err != nil {
		result := ordereddict.NewDict().Set("ParseError", err.Error())
		return result
	}

	result := ordereddict.NewDict()
	if arg.Int != 0 {
		result.Set("int", arg.Int)
	}

	if arg.String != "" {
		result.Set("string", arg.String)
	}

	if arg.StringArray != nil {
		result.Set("string_array", arg.StringArray)
	}

	if !utils.IsNil(arg.Any) {
		result.Set("any", arg.Any)
		result.Set("any type", fmt.Sprintf("%T", arg.Any))

		stored_query, ok := arg.Any.(types.StoredQuery)
		if ok {
			result.Set("Any stored query",
				vfilter.Materialize(ctx, scope, stored_query))
		}
	}

	if arg.LazyExpr != nil {
		result.Set("Lazy type", fmt.Sprintf("%T", arg.LazyExpr))
		reduced := arg.LazyExpr.Reduce()
		result.Set("Lazy Reduced Type", fmt.Sprintf("%T", reduced))
		result.Set("Lazy Reduced", reduced)

		stored_query, ok := reduced.(types.StoredQuery)
		if ok {
			result.Set("Lazy Reduced stored query",
				vfilter.Materialize(ctx, scope, stored_query))
		}
	}

	if arg.StoredQuery != nil {
		result.Set("StoredQuery Materialized",
			vfilter.Materialize(ctx, scope, arg.StoredQuery))
	}

	return result
}

func (self argFunc) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
		Name: "parse",
	}
}

func makeTestScope() types.Scope {
	result := scope.NewScope().AppendFunctions(&argFunc{})
	result.SetLogger(log.New(os.Stdout, "Log: ", log.Ldate|log.Ltime|log.Lshortfile))
	return result
}

func TestArgParsing(t *testing.T) {
	// Store the result in ordered dict so we have a consistent golden file.
	result := ordereddict.NewDict()
	for i, testCase := range multiVQLTest {
		scope := makeTestScope()

		multi_vql, err := vfilter.MultiParse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		ctx := context.Background()
		for idx, vql := range multi_vql {
			var output []types.Row

			for row := range vql.Eval(ctx, scope) {
				output = append(output,
					vfilter.RowToDict(ctx, scope, row))
			}

			result.Set(fmt.Sprintf("%03d/%03d %s: %s", i, idx, testCase.name,
				vql.ToString(scope)), output)
		}
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "args", result)
}
