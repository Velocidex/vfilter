package vfilter

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/plugins"
	"www.velocidex.com/golang/vfilter/protocols"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
	"www.velocidex.com/golang/vfilter/utils/dict"
)

const (
	PARSE_ERROR = "PARSE ERROR"
)

type execTest struct {
	clause string
	result Any
}

var compareOptions = cmpopts.IgnoreUnexported(
	_Value{}, Plugin{}, _SymbolRef{}, _AliasedExpression{})

var execTestsSerialization = []execTest{
	{"1 or sleep(a=100)", true},

	// Arithmetic
	{"1", 1},
	{"0 or 3", true},
	{"1 and 3", true},
	{"1 = TRUE", true},
	{"0 = FALSE", true},

	// This should not parse properly. Previously this was parsed
	// like -2.
	{"'-' 2", PARSE_ERROR},

	{"1.5", 1.5},
	{"2 - 1", 1},
	{"1 + 2", 3},     // int
	{"1 + 2.0", 3},   // float
	{"1 + 2.5", 3.5}, // float
	{"2.5 + 1", 3.5}, // float
	{"1 + -2", -1},
	{"1 + (1 + 2) * 5", 16},
	{"1 + (2 + 2) / 2", 3},
	{"(1 + 2 + 3) + 1", 7},
	{"(1 + 2 - 3) + 1", 1},

	// Subtraction
	{"4 - 2", 2},
	{"4 - 2.0", 2.0},
	{"4 - 2.1", 1.9},
	{"4.0 - 2", 2},
	{"4.0 - 2.0", 2.0},

	// Division
	{"4 / 2", 2},
	{"4 / 2.0", 2.0},
	{"4.0 / 2", 2.0},
	{"4.0 / 2.0", 2.0},

	// Divide by zero
	{"4.0 / 0", &Null{}},

	// Fractions
	{"4 / 3", 4.0 / 3},

	// Precedence
	{"1 + 2 * 4", 9},
	{"1 and 2 * 4", true},
	{"1 and 2 * 0", false},

	// and is higher than OR
	{"false and 5 or 4", false},
	{"(false and 5) or 4", true},

	// Division by 0 silently trapped.
	{"10 / 0", Null{}},

	// Arithmetic on incompatible types silently trapped.
	{"1 + 'foo'", Null{}},
	{"'foo' - 'bar'", Null{}},

	// Logical operators
	{"1 and 2 and 3 and 4", true},
	{"1 and (2 = 1 + 1) and 3", true},
	{"1 and (2 = 1 + 2) and 3", false},
	{"1 and func_foo(return=FALSE) and 3", false},
	{"func_foo(return=FALSE) or func_foo(return=2) or func_foo(return=FALSE)", true},

	// String concat
	{"'foo' + 'bar'", "foobar"},
	{"'foo' + 'bar' = 'foobar'", true},
	{"5 * func_foo()", 5},

	// Equality
	{"const_foo = 1", true},
	{"const_foo != 2", true},
	{"func_foo() = 1", true},
	{"func_foo() = func_foo()", true},
	{"1 = const_foo", true},
	{"1 = TRUE", true},
	{"dict(X=1, Y=2) = dict(Y=2, X=1)", true},

	// Comparing int to float
	{"1 = 1.0", true},
	{"1.0 = 1", true},
	{"1.1 = 1", false},
	{"1 = 1.1", false},
	{"1 = 'foo'", false},

	// Floats do not compare with integers properly.
	{"281462092005375 = 65535 * 65535 * 65535", true},

	// Greater than
	{"const_foo > 1", false},
	{"const_foo < 2", true},
	{"func_foo() >= 1", true},
	{"func_foo() > 1", false},
	{"func_foo() < func_foo()", false},
	{"1 <= const_foo", true},
	{"1 >= TRUE", true},
	{"2 > 1", true},
	{"2 > 1.5", true},
	{"2 > 2.5", false},
	{"2 < 1", false},
	{"2 < 1.5", false},

	// Floats
	{"2.1 < three_int64", true},
	{"2.1 < 2.5", true},
	{"3.5 < three_int64", false},

	{"three_int64 < 3.6", true},
	{"three_int64 < 2.1", false},

	{"2.1 > three_int64", false},
	{"2.1 > 2.5", false},
	{"3.5 > three_int64", true},

	{"three_int64 > 3.6", false},
	{"three_int64 > 2.1", true},

	// Non matching types
	{"2 > 'hello'", false},
	{"2 < 'hello'", false},

	// Callables
	{"func_foo(return =1)", 1},
	{"func_foo(return =1) = 1", true},
	{"func_foo(return =1 + 2)", 3},
	{"func_foo(return = (1 + (2 + 3) * 3))", 16},

	// Previously this was misparsed as the - sign (e.g. -2).
	{"func_foo(return='-')", "-"},

	// Nested callables.
	{"func_foo(return = (1 + func_foo(return=2 + 3)))", 6},

	// Arrays
	{"(1, 2, 3, 4)", []int64{1, 2, 3, 4}},
	{"(1, 2.2, 3, 4)", []float64{1, 2.2, 3, 4}},
	{"2 in (1, 2, 3, 4)", true},
	{"(1, 2, 3) = (1, 2, 3)", true},
	{"(1, 2, 3) != (2, 3)", true},

	// Array additions means concatenate the array
	{"(1, 2) + (3, 4)", []int64{1, 2, 3, 4}},

	// Coercing single members into the array
	{"1 + (3, 4)", []int64{1, 3, 4}},
	{"(1, 2) + 3", []int64{1, 2, 3}},

	// Null
	{"1 + NULL", types.Null{}},
	{"NULL + 1", types.Null{}},
	{"1 - NULL", types.Null{}},
	{"NULL - 1", types.Null{}},
	{"1 * NULL", types.Null{}},
	{"NULL * 1", types.Null{}},
	{"1 / NULL", types.Null{}},
	{"NULL / 1", types.Null{}},
	{"1 =~ NULL", false},
	{"NULL =~ 1", false},
	{"1 in NULL", false},
	{"NULL in 1", false},

	// Dicts
	{"dict(foo=1) = dict(foo=1)", true},
	{"dict(foo=1)", ordereddict.NewDict().Set("foo", int64(1))},
	{"dict(foo=1.0)", ordereddict.NewDict().Set("foo", 1.0)},
	{"dict(foo=1, bar=2)", ordereddict.NewDict().
		Set("foo", int64(1)).
		Set("bar", int64(2))},
	{"dict(foo=1, bar=2, baz=3)", ordereddict.NewDict().
		Set("foo", int64(1)).
		Set("bar", int64(2)).
		Set("baz", int64(3))},

	{"dict(foo=[1, 2])", ordereddict.NewDict().
		Set("foo", []int64{1, 2})},

	{"dict(`key with spaces`='Value')",
		ordereddict.NewDict().Set("key with spaces", "Value")},

	// Using the trailing comma notation indicates an array.
	{"dict(foo=[1,])", ordereddict.NewDict().
		Set("foo", []int64{1})},

	// Tuple notation can also be used
	{"dict(foo=(1,))", ordereddict.NewDict().
		Set("foo", []int64{1})},

	{"dict(foo=len(list=[1,]))", ordereddict.NewDict().
		Set("foo", 1)},

	// Without it a single item array is silently converted to a
	// single value.
	{"dict(foo=[1])", ordereddict.NewDict().
		Set("foo", 1)},

	// Expression as parameter.
	{"dict(foo=1, bar=( 2 + 3 ))", ordereddict.NewDict().
		Set("foo", int64(1)).Set("bar", int64(5))},

	// Mixing floats and ints.
	{"dict(foo=1.0, bar=( 2.1 + 3 ))", ordereddict.NewDict().
		Set("foo", float64(1)).Set("bar", 5.1)},

	// List as parameter.
	{"dict(foo=1, bar= [2 , 3] )", ordereddict.NewDict().
		Set("foo", int64(1)).
		Set("bar", []Any{int64(2), int64(3)})},

	// Associative
	// Relies on pre-populating the scope with a Dict.
	{"foo.bar.baz, foo.bar2", []float64{5, 7}},
	{"dict(foo=dict(bar=5)).foo.bar", 5},
	{"1, dict(foo=5).foo", []float64{1, 5}},

	// Support array indexes.
	{"my_list_obj.my_list[2]", 3},
	{"my_list_obj.my_list[1]", 2},
	{"(my_list_obj.my_list[3]).Foo", "Bar"},
	{"dict(x=(my_list_obj.my_list[3]).Foo + 'a')",
		ordereddict.NewDict().Set("x", "Bara")},

	// Support index of strings
	{"'Hello'[1]", 101},
	{"'Hello'[-1]", 111},
	{"'Hello'[:3]", "Hel"},

	// Indexing past the end of the array should clamp to end.
	{"'Hello'[2:300]", "llo"},
	{"'Hello'[-2:300]", "lo"},
	{"'Hello'[-3:]", "llo"},

	// Rgexp operator
	{"'Hello' =~ '.'", true},

	// . matches anything including the empty string (it is optimized away).
	{"'' =~ '.'", true},
	{"'Hello' =~ 'he[lo]+'", true},
	// Null also matches "." because it is optimized away.
	{"NULL =~ '.'", true},
	{"NULL =~ '.*'", true},
	{"NULL =~ ''", true},

	// Arrays match any element
	{"('Hello', 'World') =~ 'he'", true},
	{"('Hello', 'World') =~ 'xx'", false},

	// For now dicts are not regexable
	{"dict(x='Hello', y='World') =~ 'he'", false},
}

// These tests are excluded from serialization tests.
var execTests = append(execTestsSerialization, []execTest{

	// We now support hex and octal integers directly.
	{"(0x10, 0x20, 070, 0xea, -4)", []int64{16, 32, 56, 234, -4}},

	// Spurious line breaks should be ignored.
	{"1 +\n2", 3},
	{"1 AND\n 2", true},
	{"NOT\nTRUE", false},
	{"2 IN\n(1,2)", true},
}...)

// Function that returns a value.
type TestFunction struct {
	return_value Any
}

func (self TestFunction) Copy() types.FunctionInterface {
	return &TestFunction{self.return_value}
}

func (self TestFunction) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) Any {
	if value, pres := args.Get("return"); pres {
		lazy_value, ok := value.(types.LazyExpr)
		if ok {
			return lazy_value.Reduce(ctx)
		}
		return value
	}
	return self.return_value
}

func (self TestFunction) Info(scope types.Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "func_foo",
	}
}

var CounterFunctionCount = 0

type CounterFunction struct{}

func (self CounterFunction) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) Any {
	CounterFunctionCount += 1
	return CounterFunctionCount
}

func (self CounterFunction) Info(scope types.Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "counter",
	}
}

type PanicFunction struct{}

type PanicFunctionArgs struct {
	Column Any `vfilter:"optional,field=column"`
	Value  Any `vfilter:"optional,field=value"`
}

// Panic if we get an arg of a=2
func (self PanicFunction) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) Any {
	arg := PanicFunctionArgs{}

	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, &arg)
	if err != nil {
		scope.Log("Panic: %v", err)
		return types.Null{}
	}

	if scope.Eq(arg.Value, arg.Column) {
		fmt.Printf("Panic because I got %v = %v! \n", arg.Column, arg.Value)
		panic(fmt.Sprintf("Panic because I got %v = %v!", arg.Column, arg.Value))
	}

	return arg.Value
}

func (self PanicFunction) Info(scope types.Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "panic",
	}
}

type SetEnvFunctionArgs struct {
	Column string `vfilter:"required,field=column"`
	Value  Any    `vfilter:"optional,field=value"`
}

type SetEnvFunction struct{}

func (self SetEnvFunction) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) Any {
	arg := SetEnvFunctionArgs{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, &arg)
	if err != nil {
		panic(err)
	}

	env_any, pres := scope.Resolve("RootEnv")
	if !pres {
		panic("Can not find env")
	}

	env, ok := env_any.(*ordereddict.Dict)
	if !ok {
		panic("Can not find env")
	}

	env.Set(arg.Column, arg.Value)
	return true
}
func (self SetEnvFunction) Info(scope types.Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "set_env",
	}
}

func makeScope() types.Scope {
	env := ordereddict.NewDict().
		Set("const_foo", 1).
		Set("three_int64", int64(3)).
		Set("my_list_obj", ordereddict.NewDict().
			Set("my_list", []interface{}{
				1, 2, 3,
				ordereddict.NewDict().Set("Foo", "Bar")})).
		Set("env_var", "EnvironmentData").
		Set("foo", ordereddict.NewDict().
			Set("bar", ordereddict.NewDict().Set("baz", 5)).
			Set("bar2", 7))

	result := NewScope().AppendVars(env).AppendFunctions(
		TestFunction{1},
		CounterFunction{}, SetEnvFunction{},
		PanicFunction{},
	).AppendPlugins(
		plugins.GenericListPlugin{
			PluginName: "range",
			Function: func(ctx context.Context, scope types.Scope, args *ordereddict.Dict) []Row {
				return []Row{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14}
			},
		},
	)

	env.Set("RootEnv", env)
	return result
}

func TestValue(t *testing.T) {
	scope := makeScope()
	ctx, cancel := context.WithCancel(context.Background())
	foo := "'foo'"
	value := _Value{
		// String now contains quotes to preserve quoting
		// style on serialization.
		String: &foo,
	}
	result := value.Reduce(ctx, scope)
	defer cancel()

	if !scope.Eq(result, "foo") {
		t.Fatalf("Expected %v, got %v", "foo", foo)
	}
}

func TestEvalWhereClause(t *testing.T) {
	scope := makeScope()
	for idx, test := range execTests {
		preamble := "select * from plugin() where \n"
		vql, err := Parse(preamble + test.clause)
		if err != nil {
			if test.result == PARSE_ERROR {
				continue
			}
			t.Fatalf("Failed to parse %v: %v", test.clause, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		value := vql.Query.Where.Reduce(ctx, scope)
		if !scope.Eq(value, test.result) {
			utils.Debug(test.clause)
			utils.Debug(test.result)
			utils.Debug(value)
			t.Fatalf("%v: %v: Expected %v, got %v", idx, test.clause, test.result, value)
		}
	}
}

// Check that ToString() methods work properly - convert an AST back
// to VQL. Since ToString() will produce normalized VQL, we ensure
// that re-parsing this will produce the same AST.
func TestSerializaition(t *testing.T) {
	scope := makeScope()
	for _, test := range execTestsSerialization {
		preamble := "select * from plugin() where "
		vql, err := Parse(preamble + test.clause)
		if err != nil {
			// If we expect a parse error then its ok.
			if test.result == PARSE_ERROR {
				continue
			}

			t.Fatalf("Failed to parse %v: %v", test.clause, err)
		}

		vql_string := FormatToString(scope, vql)
		parsed_vql, err := Parse(vql_string)
		if err != nil {
			utils.Debug(vql)
			t.Fatalf("Failed to parse stringified VQL %v: %v (%v)",
				vql_string, err, test.clause)
		}

		FormatToString(scope, parsed_vql)

		diff := cmp.Diff(parsed_vql, vql, compareOptions)
		if diff != "" {
			t.Fatalf("Parsed generated VQL not equivalent: %v vs %v: \n%v",
				preamble+test.clause, vql_string, diff)
		}
	}
}

type vqlTest struct {
	name string
	vql  string
}

var vqlTests = []vqlTest{
	{"query with dicts", "select * from test()"},
	{"query with ints", "select * from range(start=10, end=12)"},

	{"query with wild card followed by comma",
		"select *, 1 AS Extra from test()"},

	// The environment contains a 'foo' and the plugin emits 'foo'
	// which should shadow it.
	{"aliases with shadowed var", "select env_var as EnvVar, foo as FooColumn from test()"},
	{"aliases with non-shadowed var", "select foo as FooColumn from range(start=1, end=2)"},
	{"condition on aliases", "select foo as FooColumn from test() where FooColumn = 2"},
	{"condition on aliases with not", "select foo as FooColumn from test() where NOT FooColumn = 2"},
	{"condition on non aliases", "select foo as FooColumn from test() where foo = 4"},

	{"dict plugin", "select * from dict(env_var=15, foo=5, `field with space`='value')"},
	{"dict plugin with invalid column",
		"select no_such_column from dict(env_var=15, foo=5)"},
	{"dict plugin with invalid column in expression",
		"select no_such_column + 'foo' from dict(env_var=15, foo=5)"},
	{"mix from env and plugin", "select env_var + param as ConCat from dict(param='param')"},
	{"subselects", "select param from dict(param={select * from range(start=3, end=5)})"},
	{"empty subselects should produce null", "select {select * from range(start=3, end=5) WHERE 0} AS Value FROM scope()"},
	// Add two subselects - Adding sequences makes one longer sequence.
	{"subselects addition",
		`select q1.value + q2.value as Sum from
                         dict(q1={select * from range(start=3, end=5)},
                              q2={select * from range(start=10, end=14)})`},

	{"Functions in select expression",
		"select func_foo(return=q1 + 4) from dict(q1=3)"},

	// This query shows the power of VQL:
	// 1. First the test() plugin is called to return a set of rows.

	// 2. For each of these rows, the query() function is run with
	//    the subselect specified. Note how the subselect can use
	//    the values returned from the first query.
	{"Subselect in column.",
		`select bar, {select * from dict(column=bar)} as Query
                 from test()`},

	// The below query demonstrates that the query() function is
	// run on every row returned from the filter, and then the
	// output is filtered by the the where clause. Be aware that
	// this may be expensive if test() returns many rows.
	{"Subselect functions in filter.",
		`select bar, {select * from dict(column=bar)} as Query
                 from test() where 1 in Query.column`},

	{"Subselect in columns",
		`select bar, { select column from dict(column=bar) } AS subquery from test()
                        `},

	{"Foreach plugin", `
            select * from foreach(
                row={
                   select * from test()
                }, query={
                   select bar, foo, value from range(start=bar, end=foo)
                })`},

	{"Foreach plugin with array", `
            select * from foreach(
                row=[dict(bar=1, foo=2), dict(foo=1, bar=2)],
                query={
                   select bar, foo from scope()
                })`},

	{"Foreach plugin with single object", `
            select * from foreach(
                row=dict(bar=1, foo=2),
                query={
                   select bar, foo from scope()
                })`},

	{"Foreach fully materializes row before passing to query ", `
           SELECT Evaluated FROM foreach(row={
                SELECT value,
                       set_env(column="Evaluated", value=TRUE)
                FROM range(start=1, end=10)
           },
              query={
                SELECT value from scope()
              }) LIMIT 1
        `},

	{"Foreach with non row elements",
		"SELECT * FROM foreach(row=1, query='hello')"},

	{"Foreach with non row elements",
		"SELECT * FROM foreach(row=1, query=[1,2,3,4])"},

	{"Foreach with non row elements",
		"SELECT * FROM foreach(row=[1,2,3], query={SELECT _value FROM scope()})"},

	{"Foreach with no query - single object",
		"SELECT * FROM foreach(row=dict(X=1))"},

	{"Foreach with no query - array of objects",
		"SELECT * FROM foreach(row=[dict(X=1), dict(X=2)])"},

	{"Foreach with no query - select with column",
		"SELECT * FROM foreach(row={ SELECT dict(X=1) AS X FROM scope()}, column='X')"},

	// Foreach ignores NULL rows.
	{"Foreach with no query - with null",
		"SELECT * FROM foreach(row=NULL)"},

	{"Foreach with no query - with null in array",
		"SELECT * FROM foreach(row=[NULL, NULL, dict(X=1)])"},

	{"Query plugin with dots", "Select * from Artifact.Linux.Sys()"},
	{"Order by", "select * from test() order by foo"},

	{"Order by desc", "select * from test() order by foo DESC"},
	{"Limit", "select * from test() limit 1"},
	{"Limit and order", "select * from test() order by foo desc limit 1"},
	{"Comments Simple", `// This is a single line comment
select * from test() limit 1`},
	{"Comments SQL Style", `-- This is a single line comment in sql style
select * from test() limit 1`},
	{"Comments Multiline", `/* This is a multiline comment
this is the rest of the comment */
select * from test() limit 1`},
	{"Not combined with AND",
		"select * from test() WHERE 1 and not foo = 2"},
	{"Not combined with AND 2",
		"select * from test() WHERE 0 and not foo = 2"},
	{"Not combined with OR",
		"select * from test() WHERE 1 or not foo = 20"},
	{"Not combined with OR 2",
		"select * from test() WHERE 0 or not foo = 20"},

	{"Group by 1",
		"select foo, bar from groupbytest() GROUP BY bar"},
	{"Group by *",
		"select * from groupbytest() GROUP BY bar"},
	{"Group by count",
		"select foo, bar, count(items=bar) from groupbytest() GROUP BY bar"},
	// Should be exactly the same as above
	{"Group by count with *",
		"select *, count(items=bar) from groupbytest() GROUP BY bar"},
	{"Group by count with where",
		"select foo, bar, count(items=bar) from groupbytest() WHERE foo < 4 GROUP BY bar"},
	{"Group by min",
		"select foo, bar, min(item=foo) from groupbytest() GROUP BY bar"},
	{"Group by max",
		"select foo, bar, max(item=foo) from groupbytest() GROUP BY bar"},

	{"Group by enumrate of string",
		"select baz, bar, enumerate(items=baz) from groupbytest() GROUP BY bar"},

	{"Groupby evaluates each row twice",
		`SELECT * FROM chain(
a={ SELECT count() FROM scope()},
b={
     SELECT count(), count(items=bar), bar FROM groupbytest() GROUP BY bar
})`},

	{"Lazy row evaluation (Shoud panic if foo=2",
		"select foo, panic(column=foo, value=2) from test() where foo = 4"},
	{"Quotes strings",
		"select 'foo\\'s quote' from scope()"},
	{"Hex quotes",
		`SELECT format(format='%x', args="\x01\x02\xf0\xf1") FROM scope()`},
	{"Test get()",
		"select get(item=[dict(foo=3), 2, 3, 4], member='0.foo') AS Foo from scope()"},
	{"Array concatenation",
		"SELECT (1,2) + (3,4) FROM scope()"},
	{"Array concatenation to any",
		"SELECT (1,2) + 4 FROM scope()"},
	{"Array concatenation with if",
		"SELECT (1,2) + if(condition=1, then=(3,4)) AS Field FROM scope()"},
	{"Array empty with if",
		"SELECT if(condition=1, then=[]) AS Field FROM scope()"},
	{"Array concatenation with Null",
		"SELECT (1,2) + if(condition=0, then=(3,4)) AS Field FROM scope()"},
	{"Spurious line feeds and tabs",
		"SELECT  \n1\n+\n2\tAS\nFooBar\t\n FROM\n scope(\n)\nWHERE\n FooBar >\n1\nAND\nTRUE\n"},
	{"If function and comparison expression",
		"SELECT if(condition=1 + 1 = 2, then=2, else=3), if(condition=1 + 2 = 2, then=2, else=3) FROM scope()"},
	{"If function and subselects",
		"SELECT if(condition=1, then={ SELECT * FROM test() }) FROM scope()"},
	{"If function should be lazy",
		"SELECT if(condition=FALSE, then=panic(column=3, value=3)) from scope()"},
	{"If function should be lazy",
		"SELECT if(condition=TRUE, else=panic(column=7, value=7)) from scope()"},

	{"If function should be lazy with sub query",
		"SELECT if(condition=TRUE, then={ SELECT * FROM test() LIMIT 1}) from scope()"},
	{"If function should be lazy with sub query",
		"SELECT if(condition=FALSE, then={ SELECT panic(column=8, value=8) FROM test()}) from scope()"},
	{"If function should be lazy",
		"SELECT if(condition=TRUE, else={ SELECT panic(column=9, value=9) FROM test()}) from scope()"},

	{"If function should be lazy WRT stored query 1/2",
		"LET bomb = SELECT panic(column=1, value=1) FROM scope()"},

	{"If function should be lazy WRT stored query 2/2",
		"SELECT if(condition=FALSE, then=bomb) FROM scope()"},

	{"If plugin and arrays",
		"SELECT * FROM if(condition=1, then=[dict(Foo=1), dict(Foo=2)])"},
	{"If plugin and dict",
		"SELECT * FROM if(condition=1, then=dict(Foo=2))"},

	{"Columns with space in them",
		"SELECT foo as `column with space` FROM dict(foo='hello world')"},

	{"Alternatives with the OR shortcut operator",
		"SELECT get(member='Foo') || get(member='Bar') || 'Hello' FROM scope()"},

	{"Alternatives with the OR shortcut operator false",
		"SELECT NULL || '', NULL || FALSE, NULL || 'X', 'A' || 'B', 'A' || FALSE, 'A' || '' || 'B' FROM scope()"},

	{"Alternatives with AND shortcut operator",
		"SELECT NULL && '', TRUE && 'XX', 'A' && 'B', 'A' && FALSE, ((FALSE && 1) || 2), TRUE && 1 || 2 FROM scope()"},

	{"Whitespace in the query",
		"SELECT * FROM\ntest()"},
}

var multiVQLTest = []vqlTest{
	{"Query with LET", "LET X = SELECT * FROM test()  SELECT * FROM X"},
	{"MultiSelect", "SELECT 'Bar' AS Foo FROM scope() SELECT 'Foo' AS Foo FROM scope()"},
	{"LET with index", "LET X = SELECT * FROM test() SELECT X[0], X[1].bar FROM scope()"},

	{"LET with extra columns", "LET X = SELECT * FROM test() SELECT *, 1 FROM X"},
	{"LET with extra columns before *", "LET X = SELECT * FROM test() SELECT 1, *, 2 FROM X"},
	{"LET with extra columns before * and override", "LET X = SELECT * FROM test() SELECT 1000 + foo as foo, *, 2 FROM X"},
	{"LET materialized with extra columns", "LET X <= SELECT * FROM test() SELECT *, 1 FROM X"},
	{"Column name with space", "LET X <= SELECT 2 AS `Hello World` FROM scope() " +
		"SELECT `Hello World`, `Hello World` + 4 AS Foo, X.`Hello World` FROM X"},

	{"Group by with columns with spaces",
		"LET X = SELECT foo, bar AS `Foo Bar` FROM groupbytest() SELECT * FROM X GROUP BY `Foo Bar`"},
	{"Order by with columns with spaces",
		"LET X = SELECT foo AS `Foo Bar` FROM groupbytest() SELECT * FROM X ORDER BY `Foo Bar` DESC"},
	{"LET with expression",
		"LET X = 'Hello world' SELECT X FROM scope()"},
	{"LET with expression lazy",
		"LET X = panic() SELECT 1 + 1 FROM scope()"},
	{"LET materialize with expression",
		"LET X <= 'Hello world' SELECT X FROM scope()"},
	{"Serialization (Unexpected arg aborts parsing)",
		"SELECT panic(value=1, column=1, colume='X'), func_foo() FROM scope()"},
	{"LET with expression lazy - string concat",
		"LET X = 'hello' SELECT X + 'world', 'world' + X, 'hello world' =~ X FROM scope()"},

	// count() increments every time it is called proving X is lazy
	// and will be re-evaluated each time. NOTE: Referencing variables
	// without calling them **does not** create an isolated scope. In
	// this way LET X is different than LET X(Y)
	{"Lazy expression in arrays",
		"LET X = count() SELECT (1, X), dict(foo=X, bar=[1,X]) FROM scope()"},

	{"Calling stored queries as plugins",
		"LET X = SELECT Foo FROM scope() SELECT * FROM X(Foo=1)"},

	{"Defining functions with args",
		"LET X(Foo, Bar) = Foo + Bar SELECT X(Foo=5, Bar=2) FROM scope()"},

	{"Defining stored queries with args",
		"LET X(Foo, Bar) = SELECT Foo + Bar FROM scope() SELECT * FROM X(Foo=5, Bar=2)"},

	{"Defining functions masking variable name",
		"LET X(foo) = foo + 2 SELECT X(foo=foo), foo FROM test()"},

	{"Defining stored queries masking variable name",
		"LET X(foo) = SELECT *, foo FROM range(start=foo, end=foo + 2) LET foo=2 SELECT * FROM X(foo=foo)"},
	{"Calling stored query in function context",
		// Calling a parameterized stored query in function
		// context materialized it in place.
		"LET X(foo) = SELECT *, foo FROM range(start=foo, end=foo + 2) SELECT X(foo=5).value, X(foo=10) FROM scope()"},
	{"Calling stored query with args",
		// Referring to a parameterized stored query in an arg
		// without calling it passes the stored query itself
		// as an arg.
		"LET X(foo) = SELECT *, foo FROM range(start=foo, end=foo + 2) LET foo = 8 SELECT * FROM foreach(row=X, query={ SELECT *, value FROM X(foo=value) })"},

	{"Lazy expression evaluates in caller's scope",
		"LET X(foo) = 1 + foo SELECT X(foo= foo + 1 ), foo FROM test()"},

	// Calling a symbol will reset aggregator context, but simply
	// referencing it will not. Therefore Y1 = 6, Y2 = 7 but Y3 = 6 again.
	{"Calling lazy expressions as functions allows access to global scope", `
LET Xk = 5
LET Y = Xk + count()
SELECT Y AS Y1, Y AS Y2, Y() AS Y3 FROM scope()
`},
	{"Overflow condition - should not get stuck",
		"LET X = 1 + X SELECT X(X=1), X FROM test()"},

	{"Overflow condition - https://github.com/Velocidex/velociraptor/issues/2845",
		"LET X = X.ID SELECT * FROM X"},

	{"Overflow condition - should not get stuck",
		"LET X = 1 + X  LET Y = 1 + Y SELECT X, Y FROM scope()"},

	{"Overflow condition materialized - should not get stuck",
		"LET X <= 1 + X  LET Y = 1 + Y SELECT X, Y FROM scope()"},

	{"Overflow with plugins",
		"LET foo_plugin(X) = SELECT * FROM chain(a={SELECT * FROM foo_plugin(X=1)}) SELECT * FROM foo_plugin(X=1)"},

	{"Escaped identifiers for arg parameters",
		"SELECT dict(`arg-with-special chars`=TRUE) FROM scope()"},

	// The following two queries should be the same.
	{"Group by hidden column",
		"select bar, baz from groupbytest() GROUP BY bar select baz from groupbytest() GROUP BY bar "},

	// A group by can refer to an expression, in which case the
	// expression is calculated for each row.
	{"Group by expression",
		"select *, bar + bar from groupbytest() GROUP BY bar + bar"},

	{"Variable can not mask a function.",
		"LET dict(x) = 1 SELECT 1 AS dict, dict(foo=1) FROM scope() WHERE dict"},

	{"Foreach evals query in row scope (both queries should be same)", `
LET row_query = SELECT 1 AS ColumnName123 FROM scope()
LET foreach_query = SELECT ColumnName123 FROM scope()
SELECT * FROM foreach(row=row_query, query=foreach_query)
SELECT * FROM foreach(row=row_query, query={SELECT ColumnName123 FROM scope()})
`},

	{"Aggregate functions with multiple evaluations", `
SELECT count() AS Count FROM foreach(row=[0, 1, 2])
WHERE Count <= 2  AND Count AND Count AND Count
   AND count() and count()  -- Each count() instance is unique and has unique state
`},

	{"Aggregate functions: min max", `
SELECT min(item=_value) AS Min,
       max(item=_value) AS Max,
       count() AS Count
FROM foreach(row=[0, 1, 2])
GROUP BY 1
`},

	{"Aggregate functions: min max on strings", `
SELECT min(item=_value) AS Min,
       max(item=_value) AS Max,
       count() AS Count
FROM foreach(row=["AAA", "BBBB", "CCC"])
GROUP BY 1
`},

	{"Aggregate functions keep state per unique instance", `
SELECT count() AS A, count() AS B FROM foreach(row=[0, 1, 2])
`},

	{"Aggregate functions within a VQL function have their own state", `
LET Adder(X) = SELECT *, count() AS Count FROM range(start=10, end=10 + X, step=1)

SELECT Adder(X=4), Adder(X=2) FROM scope()
`},

	{"Aggregate functions within a VQL function have their own state", `
LET Adder(X) = SELECT *, count() AS Count FROM range(start=10, end=10 + X, step=1)

SELECT * FROM foreach(row={ SELECT value FROM range(start=0, end=2, step=1)},
query={
   SELECT * FROM Adder(X=value)
})
`},

	// A foreach query is not an isolated scope which mean it can
	// refer to values outside its definition.
	{"Aggregate functions: Sum and Count together", `
LET MyValue <= "Hello"

SELECT * FROM foreach(row=[2, 3, 4],
  query={
    SELECT count() AS Count,
       sum(item=_value) AS Sum,
       MyValue
    FROM scope()
})`},

	// When the subquery is defined as a function it is evaluated in a
	// new scope with a new context - so count() and sum() start fresh
	// each time. We can still refer to global items inside the
	// function definition.
	{"Aggregate functions: Sum and Count in stored query definition", `
LET MyValue <= "Hello"
LET CountMe(Value) = SELECT count() AS Count,
    Value,
    sum(item=Value) AS Sum,
    MyValue
    FROM scope()

LET _value = 10

SELECT * FROM foreach(row=[2, 3, 4],
  query={
    SELECT * FROM CountMe(Value=_value)
})`},

	// Calling a stored query as a parameter will evaluate it before
	// passing to the foreach plugin. It will have access to any scope
	// variables available in the foreach but **not** those provided
	// by the row variables.

	// In the below you can think of CountMe(Value=_value) to be
	// expanded first with _value = 10 into an array of rows. That
	// array is then passed as the query parameter to foreach.
	{"Aggregate functions: Sum and Count in stored query definition", `
LET MyValue <= "Hello"
LET CountMe(Value) = SELECT count() AS Count,
    Value,
    sum(item=Value) AS Sum,
    MyValue
    FROM scope()

LET _value = 10

-- CountMe is evaluated at point of definition to return a stored query.
SELECT * FROM foreach(row=[2, 3, 4],
  query=CountMe(Value=_value))`},

	{"Aggregate functions: Sum all rows", `
SELECT sum(item=_value) AS Total,
       sum(item=_value * 2) AS TotalDouble
FROM foreach(row=[2, 3, 4])
GROUP BY 1
`},

	// Test if function
	{"If function with stored query", `
-- Prove that stored query was evaluated
LET Foo = SELECT 2 FROM scope() WHERE set_env(column="Eval", value=TRUE)

-- Materialize an expression
LET result <= if(condition=TRUE, then=Foo) -- should materialize
SELECT RootEnv.Eval AS Pass FROM scope()  -- should be set
`},

	{"If function with subqueries", `
LET abc(a) = if(
  condition=a,
  then={SELECT a AS Pass FROM scope()},
  else={SELECT false AS Pass from scope()})

SELECT abc(a=TRUE) AS Pass FROM scope()
`},

	{"If function with subqueries should return a lazy query", `
LET _ <= SELECT * FROM reset_objectwithmethods()

LET MyCounter(Length) =
   SELECT * FROM foreach(row={
    SELECT value
    FROM range(start=0, end=Length, step=1)
   }, query={
      SELECT Value2 FROM objectwithmethods()
      WHERE Value2
   })

-- The if plugin calls the if function directly here.
-- In previous versions this would cause it to materialize
-- the stored query. In current version the if() function
-- returns the stored query directly so it is not materialized.
SELECT * FROM if(condition=TRUE,
then=if(condition=TRUE,
  then=MyCounter(Length=1000)
))
LIMIT 3

SELECT * FROM if(condition=TRUE,
then=if(condition=TRUE,
  then={
   SELECT VarIsObjectWithMethods.Counter < 20,
       Value2 =~ "called" FROM MyCounter(Length=100) }
))
LIMIT 3


// Just prove we did not materialize the MyCounter() query
SELECT Counter < 20 FROM objectwithmethods()
LIMIT 1
`},

	{"If function with functions", `
LET abc(a) = if(
  condition=a,
  then=set_env(column="EvalFunc", value=TRUE))

LET _ <= SELECT abc(a=TRUE) FROM scope()
SELECT RootEnv.EvalFunc AS Pass FROM scope()
`},

	{"If function with conditions as subqueries", `
LET abc(a) = if(
  condition={SELECT * FROM scope()},  -- returns TRUE
  then={SELECT a AS Pass FROM scope()},
  else={SELECT false AS Pass from scope()})

SELECT abc(a=TRUE) AS Pass FROM scope()
`},

	{"If function with conditions as stored query", `
LET stored_query = SELECT * FROM scope()

LET abc(a) = if(
  condition=stored_query,
  then={SELECT a AS Pass FROM scope()},
  else={SELECT false AS Pass from scope()})

SELECT abc(a=TRUE) AS Pass FROM scope()
`},

	{"If function with conditions as vql functions", `
LET adder(a) = a =~ "Foo"

LET abc(a) = if(
  condition=adder(a="Foobar"),
  then={SELECT a AS Pass FROM scope()},
  else={SELECT false AS Pass from scope()})

SELECT abc(a=TRUE) AS Pass FROM scope()
`},

	// Multiline string constants
	{"Multiline string constants", `LET X = '''This
is
a
multiline with 'quotes' and "double quotes" and \ backslashes
''' + "A string"

SELECT X FROM scope()
`},

	{"Early breakout of foreach with infinite row query", `
SELECT * FROM foreach(row={
  SELECT count() AS Count FROM range(start=1, end=20)
  WHERE panic(column=Count, value=5)    -- Should trigger panic if we reach 5
},
query={
  SELECT Count FROM scope()
})
LIMIT 1`},

	{"Early breakout of foreach with stored query", `
LET X =   SELECT count() AS Count FROM range(start=1, end=20)
  WHERE panic(column=Count, value=6)    -- Should trigger panic if we reach 6

SELECT * FROM foreach(row=X,
query={
  SELECT Count FROM scope()
})
LIMIT 1`},

	{"Early breakout of foreach with stored query with parameters", `
LET X(Y) =   SELECT Y, count() AS Count FROM range(start=1, end=20)
  WHERE panic(column=Count, value=7)    -- Should trigger panic if we reach 7

SELECT * FROM foreach(row=X(Y=23),
query={
  SELECT Y, Count FROM scope()
})
LIMIT 1`},

	{"Expand stored query with parameters on associative", `
LET X(Y) = SELECT Y + 5 + value AS Foo FROM range(start=1, end=2)

SELECT X(Y=2).Foo FROM scope()`},

	// ORDER BY
	{"Order by", `
SELECT * FROM foreach(row=(1,8,3,2),
   query={SELECT _value AS X FROM scope()}) ORDER BY X`},
	{"Group by also orders", `
SELECT * FROM foreach(row=(1,1,1,1,8,3,3,3,2),
   query={SELECT _value AS X FROM scope()}) GROUP BY X`},
	{"Group by with explicit order by", `
SELECT * FROM foreach(row=(1,1,1,1,8,3,3,3,2),
   query={
       SELECT _value AS X, 10 - _value AS Y FROM scope()
   }) GROUP BY X ORDER BY Y`},

	{"Test array index", `
LET BIN <= SELECT * FROM test()
SELECT BIN, BIN[0] FROM scope()
`},

	{"Test array index with expression", `
LET Index(X) = X - 1
LET BIN <= SELECT * FROM test()
SELECT BIN, BIN[ Index(X=2) ] FROM scope()
SELECT BIN, BIN[ Index(X=0) ] FROM scope()
`},

	{"Create Let expression", `
let result = select  * from test()
// Create Let materialized expression
let result <= select  * from test()

//Refer to Let expression
select * from result

//Refer to non existent Let expression returns no rows
select * from no_such_result
// Refer to non existent Let expression by column returns no rows
select foobar from no_such_result`},

	{"Override function with a variable",
		"LET format = 5 SELECT format, format(format='%v', args=1) AS A FROM scope()"},

	{"Stored Expressions as plugins",
		"LET Foo = (dict(X=1),dict(X=2),dict(X=3))   SELECT * FROM Foo"},

	{"Materialized Expressions as plugins",
		"LET Foo <= (dict(X=1),dict(X=2),dict(X=3))   SELECT * FROM Foo"},

	{"Stored Expressions as plugins with args",
		"LET Foo(X) = (dict(X=1+X),dict(X=2+X),dict(X=3+X))   SELECT * FROM Foo(X=1)"},

	// This behaves identically to Python
	// >>> X = (0, 1, 2, 3, 4, 5, 6, 7)
	// >>> X[2 : ], X[2 : 4], X[ : 2], X[-1], X[-2], X[-2 : ], X[2 : -1]
	// ((2, 3, 4, 5, 6, 7), (2, 3), (0, 1), 7, 6, (6, 7), (2, 3, 4, 5, 6))
	{"Slice Range", `
LET X <= (0,1,2,3,4,5,6,7)
SELECT X[2:], X[2:4], X[:2], X[-1], X[-2], X[-2:], X[2:-1] FROM scope()
`},
	{"Slice Strings", `
LET X = "Hello World"
SELECT X[1:5], X[-5:], X[:5], X[5:2], X[5:5] FROM scope()
`},

	{"Slice Strings Binary", `
LET X = "\x00\xff\xfe\xfc\xd0\x01"
SELECT X[1], X[2], format(format="%02x", args=X[2:5]), X[5:2], X[2:2]
FROM scope()
`},

	// Value2 is a method accesses as a field
	{"Access object methods as properties.", `
LET _ <= SELECT * FROM reset_objectwithmethods()

-- Should increment Value2 count 1-2
SELECT * FROM objectwithmethods()

-- Should also increment Value2 count 3-4
SELECT Value1, Value2 + "X" FROM objectwithmethods()

-- Value2 is lazy so should **not** increment Value2 count
SELECT Value1 FROM objectwithmethods()

-- Value2 is evaluated lazily - no rows are emitted so Value2 is not called.
SELECT Value2 + "X" FROM objectwithmethods()
WHERE False

-- Value2 is **not** evaluated in an if() clause when not needed
SELECT if(condition=1, then=2, else=Value2)
FROM objectwithmethods()

-- Should resume value2 from previous query 5-6.
-- Reusing Value2 in the WHERE clause should not re-evalute it as it is cached.
SELECT Value2 FROM objectwithmethods()
WHERE Value2 =~ "method"

`},
	{"Access object methods as properties", `
LET _ <= SELECT * FROM reset_objectwithmethods()

-- Access another field should not increment
SELECT VarIsObjectWithMethods.Value1 FROM scope()

-- Access method - should increment to 1
SELECT VarIsObjectWithMethods.Value2 FROM scope()

-- Access another field should not increment
SELECT VarIsObjectWithMethods.Value1 FROM scope()

-- Value2 is **not** evaluated in an if() clause when not needed
SELECT if(condition=1, then=2, else=VarIsObjectWithMethods.Value2)
FROM scope()

-- Access method - should increment to 2
SELECT VarIsObjectWithMethods.Value2 FROM scope()

-- Value2 is evaluated in an if() clause when needed
-- Each associative dereference re-evaluates the property - so these are 3, 4, 5
SELECT if(condition=FALSE, then=2, else=VarIsObjectWithMethods.Value2) + "X",
       VarIsObjectWithMethods.Value2 =~ "I am a method",
       VarIsObjectWithMethods.Value2
FROM scope()

`},
	{"VQL Functions can access global scope", `
LET Foo = "Hello"
LET MyFunc(X) = SELECT X, Foo FROM scope()

SELECT * FROM MyFunc(X=1)`},
	// Fix issue https://github.com/Velocidex/velociraptor/issues/1756
	{"Function returning array", `
SELECT func_foo(return=ArrayValue) FROM scope()
`},

	{"If function with stored query", `
LET FooBar = SELECT "A" FROM scope()
LET B = SELECT if(condition=TRUE, then=FooBar) AS Item
FROM scope()
SELECT B, FooBar FROM scope()
`},
	{"Explain query", `
EXPLAIN SELECT "A" FROM scope()
`},
	{"Flatten query", `
SELECT * FROM flatten(query={
   SELECT 1 AS A, (1,2) AS B FROM scope()
})`},
	{"Flatten query cartesian with 2 lists", `
SELECT * FROM flatten(query={
   SELECT (3, 4) AS A, (1,2) AS B FROM scope()
})`},
	{"Flatten query empty list", `
LET FOO <= SELECT * FROM scope() WHERE FALSE
SELECT * FROM flatten(query={
   SELECT 1 AS A, FOO, (1, 2) AS B FROM scope()
})`},
	{"Flatten dict query", `
SELECT * FROM flatten(query={
   SELECT 1 AS A, dict(E=1,F=2) AS B FROM scope()
})`},
	{"Flatten subquery", `
SELECT * FROM flatten(query={
   SELECT *, { SELECT * FROM range(start=1, end=3) } AS Count
   FROM foreach(row=[dict(A=1)])
})`},
	{"Flatten stored query", `
LET SQ = SELECT * FROM range(start=1, end=3)
SELECT * FROM flatten(query={
   SELECT *, SQ
   FROM foreach(row=[dict(A=1)])
})`},

	// Each count() AST node will use a different aggregator context
	// so will count separately.
	{"Foreach query with multiple count()", `
SELECT * FROM foreach(row={
   SELECT count() AS RowCount
   FROM range(start=1, end=3)
}, query={
   SELECT RowCount, count() AS QueryCount, count() AS SecondQueryCount
   FROM range(start=1, step=1, end=3)
})
`},

	// Calling a VQL stored query will reset the aggregator context.
	{"Calling stored query with aggregators", `
LET Counter(Start) = SELECT count() AS Count, Start
  FROM range(start=1, step=1, end=3)

SELECT * FROM foreach(row={
   SELECT count() AS RowCount
   FROM range(start=1, end=3)
}, query={
   SELECT * FROM Counter(Start=RowCount)
})
`},

	// Each time that Counter() is called should reset.
	// Each time that CountFunc() is called should reset.
	{"Aggregate function in a parameter resets stat", `
LET Counter(Start) = SELECT count() AS Count, Start
  FROM range(start=1, step=1, end=3)

LET CountFunc(Start) = dict(A=count(), B=Start)

SELECT set_env(column="Eval", value=Counter(Start="First Call")),
       set_env(column="Eval2", value=Counter(Start="Second Call")),
       set_env(column="Eval3", value=CountFunc(Start="First Func Call")),
       set_env(column="Eval4", value=CountFunc(Start="Second Func Call"))
FROM scope()

SELECT RootEnv.Eval AS FirstCall, RootEnv.Eval2 AS SecondCall,
       RootEnv.Eval3 AS FirstFuncCall, RootEnv.Eval4 AS SecondFuncCall
FROM scope()
`}, {"Test Scope Clearing", `
LET Data <= (dict(A=1), dict(B=2))
LET s = scope()

SELECT s.A, A, s.B, B FROM Data
`},
	// Comparing time to strings in unsupported in the base vfilter
	// project, it is added with specialized handlers with
	// Velociraptor.
	{"Test timestamp comparisons", `
SELECT timestamp(epoch=1723428985) < 1118628985,
       1118628985 < timestamp(epoch=1723428985),
       timestamp(epoch=1723428985) < timestamp(epoch=1118628985),
       timestamp(epoch=1118628985) < timestamp(epoch=1723428985),
       timestamp(epoch=1723428985) > 1118628985,
       1118628985 > timestamp(epoch=1723428985),
       timestamp(epoch=1723428985) > timestamp(epoch=1118628985),
       timestamp(epoch=1118628985) > timestamp(epoch=1723428985),
       timestamp(epoch=1723428985) < 1118628985.0,
       1118628985.0 < timestamp(epoch=1723428985),
       timestamp(epoch=1723428985) > 1118628985.0,
       1118628985.0 > timestamp(epoch=1723428985),
       timestamp(epoch=1723428985) < "2024-08-12T02:15:25.176Z",
       "2024-08-12T02:15:25.176Z" < timestamp(epoch=1723428985),
       timestamp(epoch=1723428985) > "2024-08-12T02:15:25.176Z",
       "2024-08-12T02:15:25.176Z" > timestamp(epoch=1723428985)
FROM scope()
`},

	{"Test struct associative", `
SELECT StructValue.SrcIP, StructValue.src_ip, StructValue.SrcIp
FROM scope()`},
}

type _RangeArgs struct {
	Start float64 `vfilter:"required,field=start"`
	End   float64 `vfilter:"required,field=end"`
}

func makeTestScope() types.Scope {
	result := makeScope().
		AppendVars(ordereddict.NewDict().
			Set("ArrayValue", [3]int{1, 2, 3}).
			Set("StructValue", structWithJson{
				SrcIP: "127.0.0.1",
			}).
			Set("VarIsObjectWithMethods", ObjectWithMethods{Value1: 1})).
		AddProtocolImpl(protocols.NewLazyStructWrapper(
			ObjectWithMethods{}, "Value1", "Value2", "Value3", "Counter")).
		AppendPlugins(
			plugins.GenericListPlugin{
				PluginName: "test",
				Function: func(ctx context.Context, scope types.Scope, args *ordereddict.Dict) []Row {
					var result []Row
					for i := 0; i < 3; i++ {
						result = append(result, ordereddict.NewDict().
							Set("foo", i*2).
							Set("bar", i))
					}
					return result
				},
			}, plugins.GenericListPlugin{
				PluginName: "range",
				Function: func(ctx context.Context, scope types.Scope, args *ordereddict.Dict) []Row {
					arg := &_RangeArgs{}
					arg_parser.ExtractArgsWithContext(ctx, scope, args, arg)
					var result []Row
					for i := arg.Start; i <= arg.End; i++ {
						result = append(result, ordereddict.NewDict().Set("value", i))
					}
					return result
				},
			}, plugins.GenericListPlugin{
				PluginName: "dict",
				Doc:        "Just echo back the args as a dict.",
				Function: func(ctx context.Context, scope types.Scope, args *ordereddict.Dict) []Row {
					result := ordereddict.NewDict()
					for _, k := range scope.GetMembers(args) {
						v, _ := args.Get(k)
						lazy_arg, ok := v.(types.LazyExpr)
						if ok {
							result.Set(k, lazy_arg.Reduce(ctx))
						} else {
							result.Set(k, v)
						}
					}

					return []Row{result}
				},
			}, plugins.GenericListPlugin{
				PluginName: "groupbytest",
				Function: func(ctx context.Context, scope types.Scope, args *ordereddict.Dict) []Row {
					return []Row{
						ordereddict.NewDict().Set("foo", 1).Set("bar", 5).
							Set("baz", "a"),
						ordereddict.NewDict().Set("foo", 2).Set("bar", 5).
							Set("baz", "b"),
						ordereddict.NewDict().Set("foo", 3).Set("bar", 2).
							Set("baz", "c"),
						ordereddict.NewDict().Set("foo", 4).Set("bar", 2).
							Set("baz", "d"),
					}
				},
			}, plugins.GenericListPlugin{
				PluginName: "reset_objectwithmethods",
				Function: func(ctx context.Context, scope types.Scope, args *ordereddict.Dict) []Row {
					ObjectWithMethodsCallCounter_mu.Lock()
					defer ObjectWithMethodsCallCounter_mu.Unlock()

					ObjectWithMethodsCallCounter = 0
					return []Row{}
				},
			}, plugins.GenericListPlugin{
				PluginName: "objectwithmethods",
				Function: func(ctx context.Context, scope types.Scope, args *ordereddict.Dict) []Row {
					return []Row{
						&ObjectWithMethods{Value1: 1},
						&ObjectWithMethods{Value1: 2},
					}
				}})
	result.SetLogger(log.New(os.Stdout, "Log: ", log.Ldate|log.Ltime|log.Lshortfile))
	return result
}

var (
	ObjectWithMethodsCallCounter_mu sync.Mutex
	ObjectWithMethodsCallCounter    int
)

type ObjectWithMethods struct {
	Value1       int
	IgnoredValue int
}

func (self ObjectWithMethods) Counter() int {
	ObjectWithMethodsCallCounter_mu.Lock()
	defer ObjectWithMethodsCallCounter_mu.Unlock()

	return ObjectWithMethodsCallCounter
}

func (self ObjectWithMethods) Value2() string {
	ObjectWithMethodsCallCounter_mu.Lock()
	defer ObjectWithMethodsCallCounter_mu.Unlock()

	ObjectWithMethodsCallCounter++
	return fmt.Sprintf("I am a method, called %v", ObjectWithMethodsCallCounter)
}

func (self ObjectWithMethods) InvisibleMethod() string {
	return "Invisible"
}

// This checks that lazy queries are not evaluated unnecessarily. We
// use the counter() function and watch its side effects.
func TestMaterializedStoredQuery(t *testing.T) {
	scope := makeTestScope()

	run_query := func(query string) {
		vql, err := Parse(query)
		assert.NoError(t, err)

		ctx := context.Background()
		_, err = OutputJSON(vql, ctx, scope, marshal_indent)
		assert.NoError(t, err)
	}

	CounterFunctionCount = 0
	assert.Equal(t, CounterFunctionCount, 0)

	// Running a query directly will evaluate.
	run_query("SELECT counter() FROM scope()")
	assert.Equal(t, CounterFunctionCount, 1)

	// Just storing the query does not evaluate.
	run_query("LET stored = SELECT counter() from scope()")
	assert.Equal(t, CounterFunctionCount, 1)

	// Using the stored query will cause it to evaluate.
	run_query("SELECT * FROM stored")
	assert.Equal(t, CounterFunctionCount, 2)

	// Materializing the query will evaluate it and store it in a
	// variable.
	run_query("LET materialized <= SELECT counter() from scope()")
	assert.Equal(t, CounterFunctionCount, 3)

	// Expanding it wont evaluate since it is already
	// materialized.
	run_query("SELECT * FROM materialized")
	assert.Equal(t, CounterFunctionCount, 3)
}

func TestVQLQueries(t *testing.T) {
	// Store the result in ordered dict so we have a consistent golden file.
	result := ordereddict.NewDict()
	for i, testCase := range vqlTests {
		if false && i != 45 {
			continue
		}

		scope := makeTestScope()

		vql, err := Parse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		ctx := context.Background()
		var output []Row
		for row := range vql.Eval(ctx, scope) {
			output = append(output, dict.RowToDict(ctx, scope, row))
		}

		result.Set(fmt.Sprintf("%03d %s: %s", i, testCase.name,
			FormatToString(scope, vql)), output)
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "vql_queries", result)
}

func TestMultiVQLQueries(t *testing.T) {
	// Store the result in ordered dict so we have a consistent golden file.
	result := ordereddict.NewDict()
	for i, testCase := range multiVQLTest {
		if false && i != 85 {
			continue
		}
		scope := makeTestScope()
		multi_vql, err := MultiParse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		ctx := context.Background()
		for idx, vql := range multi_vql {
			var output []Row

			for row := range vql.Eval(ctx, scope) {
				output = append(output, dict.RowToDict(ctx, scope, row))
			}

			result.Set(fmt.Sprintf("%03d/%03d %s: %s", i, idx, testCase.name,
				FormatToString(scope, vql)), output)
		}
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "multi_vql_queries", result)
}

// Check that ToString() methods work properly - convert an AST back
// to VQL. Since ToString() will produce normalized VQL, we ensure
// that re-parsing this will produce the same AST.
func TestVQLSerializaition(t *testing.T) {
	scope := makeScope()
	for _, test := range vqlTests {
		vql, err := Parse(test.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", test.vql, err)
		}

		vql_string := FormatToString(scope, vql)

		parsed_vql, err := Parse(vql_string)
		if err != nil {
			t.Fatalf("Failed to parse stringified VQL %v: %v (%v)",
				vql_string, err, test.vql)
		}
		FormatToString(scope, parsed_vql)

		diff := cmp.Diff(parsed_vql, vql, compareOptions)
		if diff != "" {
			t.Fatalf("Parsed generated VQL not equivalent: %v vs %v: \n%v",
				test.vql, vql_string, diff)
		}
	}
}

type structWithJson struct {
	SrcIP string `json:"src_ip,omitempty"`
}
