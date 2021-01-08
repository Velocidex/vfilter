package vfilter

import (
	"context"
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/go-test/deep"
	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"www.velocidex.com/golang/vfilter/plugins"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

const (
	PARSE_ERROR = "PARSE ERROR"
)

type execTest struct {
	clause string
	result Any
}

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

	// Rgexp operator
	{"'Hello' =~ '.'", true},

	// . matches anything including the empty string (it is optimized away).
	{"'' =~ '.'", true},
	{"'Hello' =~ 'he[lo]+'", true},

	// Non strings do not match
	{"NULL =~ '.'", false},
	{"1 =~ '.'", false},

	// Arrays match any element
	{"('Hello', 'World') =~ 'he'", true},
	{"('Hello', 'World') =~ 'xx'", false},

	// For now dicts are not regexable
	{"dict(x='Hello', y='World') =~ 'he'", false},
}

// These tests are excluded from serialization tests.
var execTests = append(execTestsSerialization, []execTest{

	// We now support hex and octal integers directly.
	{"(0x10, 0x20, 070, -4)", []int64{16, 32, 56, -4}},

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

func (self TestFunction) Call(ctx context.Context, scope types.Scope, args *ordereddict.Dict) Any {
	if value, pres := args.Get("return"); pres {
		lazy_value := value.(types.LazyExpr)
		return lazy_value.Reduce()
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

	err := ExtractArgs(scope, args, &arg)
	if err != nil {
		panic(err)
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
	err := ExtractArgs(scope, args, &arg)
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
	return env
}
func (self SetEnvFunction) Info(scope types.Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "set_env",
	}
}

func makeScope() types.Scope {
	env := ordereddict.NewDict().
		Set("const_foo", 1).
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
			Function: func(scope types.Scope, args *ordereddict.Dict) []Row {
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
	for _, test := range execTests {
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
			t.Fatalf("%v: Expected %v, got %v", test.clause, test.result, value)
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

		vql_string := vql.ToString(scope)
		parsed_vql, err := Parse(vql_string)
		if err != nil {
			utils.Debug(vql)
			t.Fatalf("Failed to parse stringified VQL %v: %v (%v)",
				vql_string, err, test.clause)
		}

		if !reflect.DeepEqual(parsed_vql, vql) {
			utils.Debug(vql)
			t.Fatalf("Parsed generated VQL not equivalent: %v vs %v.",
				preamble+test.clause, vql_string)
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

	{"dict plugin", "select * from dict(env_var=15, foo=5)"},
	{"dict plugin with invalide column",
		"select no_such_column from dict(env_var=15, foo=5)"},
	{"dict plugin with invalide column in expression",
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

	{"Create Let expression", "let result = select  * from test()"},
	{"Create Let materialized expression", "let result <= select  * from test()"},
	{"Refer to Let expression", "select * from result"},
	{"Refer to non existent Let expression returns no rows", "select * from no_such_result"},
	{"Refer to non existent Let expression by column returns no rows",
		"select foobar from no_such_result"},

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
	{"Group by count with where",
		"select foo, bar, count(items=bar) from groupbytest() WHERE foo < 4 GROUP BY bar"},
	{"Group by min",
		"select foo, bar, min(items=foo) from groupbytest() GROUP BY bar"},
	{"Group by max",
		"select foo, bar, max(items=foo) from groupbytest() GROUP BY bar"},
	{"Group by max of string",
		"select baz, bar, max(items=baz) from groupbytest() GROUP BY bar"},
	{"Group by min of string",
		"select baz, bar, min(items=baz) from groupbytest() GROUP BY bar"},

	{"Group by enumrate of string",
		"select baz, bar, enumerate(items=baz) from groupbytest() GROUP BY bar"},

	{"Lazy row evaluation (Shoud panic if foo=2",
		"select foo, panic(column=foo, value=2) from test() where foo = 4"},
	{"Quotes strings",
		"select 'foo\\'s quote' from scope()"},
	{"Test get()",
		"select get(item=[dict(foo=3), 2, 3, 4], member='0.foo') AS Foo from scope()"},
	{"Test array index",
		"LET BIN <= SELECT * FROM test()"},
	{"Test array index 2",
		"SELECT BIN, BIN[0] FROM scope()"},
	{"Array concatenation",
		"SELECT (1,2) + (3,4) FROM scope()"},
	{"Array concatenation to any",
		"SELECT (1,2) + 4 FROM scope()"},
	{"Array concatenation with if",
		"SELECT (1,2) + if(condition=1, then=(3,4)) AS Field FROM scope()"},
	{"Array concatenation with Null",
		"SELECT (1,2) + if(condition=0, then=(3,4)) AS Field FROM scope()"},
	{"Spurious line feeds and tabs",
		"SELECT  \n1\n+\n2\tAS\nFooBar\t\n FROM\n scope(\n)\nWHERE\n FooBar >\n1\nAND\nTRUE\n"},
	{"If function and comparison expression",
		"SELECT if(condition=1 + 1 = 2, then=2, else=3), if(condition=1 + 2 = 2, then=2, else=3) FROM scope()"},
	{"If function and subselects",
		"SELECT if(condition=1, then={ SELECT * FROM test() }) FROM scope()"},
	{"If function should be lazy",
		"SELECT if(condition=FALSE, then=panic(column=1, value=1)) from scope()"},
	{"If function should be lazy",
		"SELECT if(condition=TRUE, else=panic(column=1, value=1)) from scope()"},

	{"If function should be lazy with sub query",
		"SELECT if(condition=TRUE, then={ SELECT * FROM test() LIMIT 1}) from scope()"},
	{"If function should be lazy with sub query",
		"SELECT if(condition=FALSE, then={ SELECT panic(column=1, value=1) FROM test()}) from scope()"},
	{"If function should be lazy",
		"SELECT if(condition=TRUE, else={ SELECT panic(column=1, value=1) FROM test()}) from scope()"},

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
}

var multiVQLTest = []vqlTest{
	{"Query with LET", "LET X = SELECT * FROM test()  SELECT * FROM X"},
	{"MultiSelect", "SELECT 'Bar' AS Foo FROM scope() SELECT 'Foo' AS Foo FROM scope()"},
	{"LET with index", "LET X = SELECT * FROM test() SELECT X[0], X[1].bar FROM scope()"},

	{"LET with extra columns", "LET X = SELECT * FROM test() SELECT *, 1 FROM X"},
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
	{"Serialization",
		"SELECT panic(value=1, colume='X'), func_foo() FROM scope()"},
	{"LET with expression lazy - string concat",
		"LET X = 'hello' SELECT X + 'world', 'world' + X, 'hello world' =~ X FROM scope()"},

	// count() increments every time it is called proving X is
	// lazy and will be re-evaluated each time.
	{"Lazy expression in arrays",
		"LET X = count() SELECT (1, X), dict(foo=X, bar=[1,X]) FROM scope()"},

	{"Calling stored queries as plugins",
		"LET X = SELECT Foo FROM scope() SELECT * FROM X(Foo=1)"},

	// First two calls to Y are not function calls so they
	// evaluate on the current scope. Third call makes a new scope
	// which resets count().
	{"Calling lazy expressions as functions creates a new scope",
		"LET Y = count() SELECT Y AS Y1, Y AS Y2, Y() AS Y3 FROM scope()"},

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

	{"Calling lazy expressions as functions allows access to global scope",
		"LET Xk = 5 LET Y = Xk + count() SELECT Y AS Y1, Y AS Y2, Y() AS Y3 FROM scope()"},

	{"Overflow condition - should not get stuck",
		"LET X = 1 + X SELECT X(X=1), X FROM test()"},

	{"Overflow condition - should not get stuck",
		"LET X = 1 + X  LET Y = 1 + Y SELECT X, Y FROM scope()"},

	{"Overflow condition materialized - should not get stuck",
		"LET X <= 1 + X  LET Y = 1 + Y SELECT X, Y FROM scope()"},

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

	{"Aggregate functions keep state per unique instance", `
SELECT * FROM foreach(row=[0, 1, 2],
  query={
    SELECT count() AS A, count() AS B FROM scope()
})`},
}

type _RangeArgs struct {
	Start float64 `vfilter:"required,field=start"`
	End   float64 `vfilter:"required,field=end"`
}

func makeTestScope() types.Scope {
	result := makeScope().AppendPlugins(
		plugins.GenericListPlugin{
			PluginName: "test",
			Function: func(scope types.Scope, args *ordereddict.Dict) []Row {
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
			Function: func(scope types.Scope, args *ordereddict.Dict) []Row {
				arg := &_RangeArgs{}
				ExtractArgs(scope, args, arg)
				var result []Row
				for i := arg.Start; i <= arg.End; i++ {
					result = append(result, ordereddict.NewDict().Set("value", i))
				}
				return result
			},
		}, plugins.GenericListPlugin{
			PluginName: "dict",
			Doc:        "Just echo back the args as a dict.",
			Function: func(scope types.Scope, args *ordereddict.Dict) []Row {
				result := ordereddict.NewDict()
				for _, k := range scope.GetMembers(args) {
					v, _ := args.Get(k)
					lazy_arg, ok := v.(types.LazyExpr)
					if ok {
						result.Set(k, lazy_arg.Reduce())
					} else {
						result.Set(k, v)
					}
				}

				return []Row{result}
			},
		}, plugins.GenericListPlugin{
			PluginName: "groupbytest",
			Function: func(scope types.Scope, args *ordereddict.Dict) []Row {
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
		})
	result.SetLogger(log.New(os.Stdout, "Log: ", log.Ldate|log.Ltime|log.Lshortfile))
	return result
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
	scope := makeTestScope()

	// Store the result in ordered dict so we have a consistent golden file.
	result := ordereddict.NewDict()
	for i, testCase := range vqlTests {
		vql, err := Parse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		ctx := context.Background()
		var output []Row
		for row := range vql.Eval(ctx, scope) {
			output = append(output, RowToDict(ctx, scope, row))
		}

		result.Set(fmt.Sprintf("%03d %s: %s", i, testCase.name,
			vql.ToString(scope)), output)
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
		scope := makeTestScope()

		multi_vql, err := MultiParse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		ctx := context.Background()
		for idx, vql := range multi_vql {
			var output []Row

			for row := range vql.Eval(ctx, scope) {
				output = append(output, RowToDict(ctx, scope, row))
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

		vql_string := vql.ToString(scope)

		parsed_vql, err := Parse(vql_string)
		if err != nil {
			t.Fatalf("Failed to parse stringified VQL %v: %v (%v)",
				vql_string, err, test.vql)
		}

		diffs := deep.Equal(parsed_vql, vql)
		if diffs != nil {
			t.Error(diffs)
			t.Fatalf("Parsed generated VQL not equivalent: %v vs %v.",
				test.vql, vql_string)
		}
	}
}
