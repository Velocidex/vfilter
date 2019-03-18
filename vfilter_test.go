package vfilter

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/sebdah/goldie"
)

type execTest struct {
	clause string
	result Any
}

var execTests = []execTest{
	{"1 or sleep(a=100)", true},

	// Arithmetic
	{"1", 1},
	{"0 or 3", true},
	{"1 and 3", true},
	{"1 = TRUE", true},
	{"0 = FALSE", true},

	{"1.5", 1.5},
	{"2 - 1", 1},
	{"1 + 2", 3},
	{"1 + 2.0", 3},
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
	{"10 / 0", false},

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

	// Greater than
	{"const_foo > 1", false},
	{"const_foo < 2", true},
	{"func_foo() >= 1", true},
	{"func_foo() > 1", false},
	{"func_foo() < func_foo()", false},
	{"1 <= const_foo", true},
	{"1 >= TRUE", true},

	// Callables
	{"func_foo(return =1)", 1},
	{"func_foo(return =1) = 1", true},
	{"func_foo(return =1 + 2)", 3},
	{"func_foo(return = (1 + (2 + 3) * 3))", 16},

	// Nested callables.
	{"func_foo(return = (1 + func_foo(return=2 + 3)))", 6},

	// Arrays
	{"(1, 2, 3, 4)", []float64{1, 2, 3, 4}},
	{"2 in (1, 2, 3, 4)", true},
	{"(1, 2, 3) = (1, 2, 3)", true},
	{"(1, 2, 3) != (2, 3)", true},

	// Dicts
	{"dict(foo=1) = dict(foo=1)", true},
	{"dict(foo=1)", NewDict().Set("foo", 1.0)},
	{"dict(foo=1, bar=2)", NewDict().Set("foo", 1.0).Set("bar", 2.0)},
	{"dict(foo=1, bar=2, baz=3)", NewDict().
		Set("foo", 1.0).
		Set("bar", 2.0).
		Set("baz", 3.0)},

	// Expression as parameter.
	{"dict(foo=1, bar=( 2 + 3 ))", NewDict().
		Set("foo", 1.0).Set("bar", 5.0)},

	// List as parameter.
	{"dict(foo=1, bar= [2 , 3] )", NewDict().
		Set("foo", 1.0).
		Set("bar", []Any{2.0, 3.0})},

	// Associative
	// Relies on pre-populating the scope with a Dict.
	{"foo.bar.baz, foo.bar2", []float64{5, 7}},
	{"dict(foo=dict(bar=5)).foo.bar", 5},
	{"1, dict(foo=5).foo", []float64{1, 5}},
}

// Function that returns a value.
type TestFunction struct {
	return_value Any
}

func (self TestFunction) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	if value, pres := args.Get("return"); pres {
		return value
	}
	return self.return_value
}

func (self TestFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "func_foo",
	}
}

type PanicFunction struct{}

// Panic if we get an arg of a=2
func (self PanicFunction) Call(ctx context.Context, scope *Scope, args *Dict) Any {
	column, pres := args.Get("column")
	if !pres {
		return Null{}
	}
	value, pres := args.Get("value")
	if !pres {
		return Null{}
	}
	if scope.Eq(value, column) {
		panic(fmt.Sprintf("Panic because I got %v!", value))
	}

	return value
}

func (self PanicFunction) Info(scope *Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "panic",
	}
}

func makeScope() *Scope {
	return NewScope().AppendVars(NewDict().
		Set("const_foo", 1).
		Set("env_var", "EnvironmentData").
		Set("foo", NewDict().
			Set("bar", NewDict().Set("baz", 5)).
			Set("bar2", 7)),
	).AppendFunctions(
		TestFunction{1},
		PanicFunction{},
	).AppendPlugins(
		GenericListPlugin{
			PluginName: "range",
			Function: func(scope *Scope, args *Dict) []Row {
				return []Row{1, 2, 3, 4}
			},
			RowType: 1,
		},
	)
}

func TestValue(t *testing.T) {
	scope := makeScope()
	ctx, cancel := context.WithCancel(context.Background())
	foo := "foo"
	value := _Value{
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
			t.Fatalf("Failed to parse %v: %v", test.clause, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		value := vql.Query.Where.Reduce(ctx, scope)
		if !scope.Eq(value, test.result) {
			Debug(test.clause)
			t.Fatalf("%v: Expected %v, got %v", test.clause, test.result, value)
		}
	}
}

// Check that ToString() methods work properly - convert an AST back
// to VQL. Since ToString() will produce normalized VQL, we ensure
// that re-parsing this will produce the same AST.
func TestSerializaition(t *testing.T) {
	scope := makeScope()
	for _, test := range execTests {
		preamble := "select * from plugin() where "
		vql, err := Parse(preamble + test.clause)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", test.clause, err)
		}

		vql_string := vql.ToString(scope)

		parsed_vql, err := Parse(vql_string)
		if err != nil {
			t.Fatalf("Failed to parse stringified VQL %v: %v (%v)",
				vql_string, err, test.clause)
		}

		if !reflect.DeepEqual(parsed_vql, vql) {
			Debug(vql)
			t.Fatalf("Parsed generated VQL not equivalent: %v vs %v.",
				preamble+test.clause, vql_string)
		}
	}
}

// Implement some test plugins for testing.
type _RepeaterPlugin struct{}

func (self _RepeaterPlugin) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)
		if value, pres := scope.Associative(args, "return"); pres {
			output_chan <- value
		}
	}()

	return output_chan
}

func (self _RepeaterPlugin) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "repeater",
	}
}

func TestSubselectDefinition(t *testing.T) {
	// Compile a query which uses 2 args.
	vql, err := Parse("select value+addand1 as total from repeater(return=dict(value=addand2))")
	if err != nil {
		t.Fatalf(err.Error())
	}

	scope := makeScope().AppendPlugins(
		_RepeaterPlugin{},
		SubSelectFunction{
			PluginName: "test1",
			SubSelect:  vql,
			RowType:    1,
		},
	)

	// Call the pre-baked query with args.
	vql, err = Parse("select * from test1(addand1=7, addand2=10)")
	if err != nil {
		t.Fatalf(err.Error())
	}

	ctx := context.Background()
	output_chan := vql.Eval(ctx, scope)
	var result []Row
	for row := range output_chan {
		result = append(result, row)
	}

	if !reflect.DeepEqual(result, []Row{NewDict().
		Set("total", 17.0),
	}) {
		Debug(result)
		t.Fatalf("failed.")
	}
}

type vqlTest struct {
	name string
	vql  string
}

var vqlTests = []vqlTest{
	{"query with dicts", "select * from test()"},
	{"query with ints", "select * from range(start=10, end=12)"},

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
	{"Subselect functions.",
		`select bar,
                        query(vql={select * from dict(column=bar)}) as Query
                 from test()`},

	// The below query demonstrates that the query() function is
	// run on every row returned from the filter, and then the
	// output is filtered by the the where clause. Be aware that
	// this may be expensive if test() returns many rows.
	{"Subselect functions in filter.",
		`select bar,
                        query(vql={select * from dict(column=bar)}) as Query
                 from test() where 1 in Query.column`},

	{"Subselect in columns",
		`select bar, { select column from dict(column=bar) } AS subquery from test()
                        `},

	{"Create Let expression", "let result = select  * from test()"},
	{"Create Let materialized expression", "let result <= select  * from test()"},
	{"Refer to Let expression", "select * from result"},
	{"Refer to non existent Let expression", "select * from no_such_result"},
	{"Refer to non existent Let expression by column",
		"select foobar from no_such_result"},

	{"Foreach plugin", `
            select * from foreach(
                row={
                   select * from test()
                }, query={
                   select bar, foo, value from range(start=bar, end=foo)
                })`},

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
}

func makeTestScope() *Scope {
	return makeScope().AppendPlugins(
		GenericListPlugin{
			PluginName: "test",
			Function: func(scope *Scope, args *Dict) []Row {
				var result []Row
				for i := 0; i < 3; i++ {
					result = append(result, NewDict().
						Set("foo", i*2).
						Set("bar", i))
				}
				return result
			},
		}, GenericListPlugin{
			PluginName: "range",
			Function: func(scope *Scope, args *Dict) []Row {
				start := 0.0
				end := 3.0
				ExtractFloat(&start, "start", args)
				ExtractFloat(&end, "end", args)

				var result []Row
				for i := start; i <= end; i++ {
					result = append(result, NewDict().Set("value", i))
				}
				return result
			},
		}, GenericListPlugin{
			PluginName: "dict",
			Doc:        "Just echo back the args as a dict.",
			Function: func(scope *Scope, args *Dict) []Row {
				return []Row{args}
			},
		}, GenericListPlugin{
			PluginName: "groupbytest",
			Function: func(scope *Scope, args *Dict) []Row {
				return []Row{
					NewDict().Set("foo", 1).Set("bar", 5).
						Set("baz", "a"),
					NewDict().Set("foo", 2).Set("bar", 5).
						Set("baz", "b"),
					NewDict().Set("foo", 3).Set("bar", 2).
						Set("baz", "c"),
					NewDict().Set("foo", 4).Set("bar", 2).
						Set("baz", "d"),
				}
			},
		})
}

func TestVQLQueries(t *testing.T) {
	scope := makeTestScope()

	// Store the result in ordered dict so we have a consistent golden file.
	result := NewDict()
	for i, testCase := range vqlTests {
		vql, err := Parse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		ctx := context.Background()
		output_json, err := OutputJSON(vql, ctx, scope)
		if err != nil {
			t.Fatalf("Failed to eval %v: %v", testCase.vql, err)
		}

		var output Any
		json.Unmarshal(output_json, &output)

		result.Set(fmt.Sprintf("%03d %s: %s", i, testCase.name,
			vql.ToString(scope)), output)
	}

	result_json, _ := json.MarshalIndent(result, "", " ")
	goldie.Assert(t, "vql_queries", result_json)
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

		if !reflect.DeepEqual(parsed_vql, vql) {
			Debug(vql)
			t.Fatalf("Parsed generated VQL not equivalent: %v vs %v.",
				test.vql, vql_string)
		}
	}
}

var columnTests = []vqlTest{
	{"Columns from env", "select Field from TestDict"},
	{"Columns from env wildcard", "select * from TestDict"},
}

func TestColumns(t *testing.T) {
	env := NewDict().Set("TestDict", []Row{
		NewDict().Set("Field", 2),
	})
	scope := makeTestScope().AppendVars(env)

	result := NewDict()
	for i, testCase := range columnTests {
		vql, err := Parse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		result.Set(fmt.Sprintf("%03d %s: %s", i, testCase.name,
			vql.ToString(scope)), vql.Columns(scope))
	}

	result_json, _ := json.MarshalIndent(result, "", " ")
	goldie.Assert(t, "columns", result_json)
}
