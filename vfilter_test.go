package vfilter

import (
	"context"
	"reflect"
	"testing"
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
	{"1 + 'foo'", false},
	{"'foo' - 'bar'", false},

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

	// Sub select as parameter.
	{"dict(foo=1, bar={select * from range()} )", NewDict().
		Set("foo", 1.0).
		Set("bar", []Any{1, 2, 3, 4}),
	},

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

func (self TestFunction) Call(ctx context.Context, scope *Scope, row Row) Any {
	if value, pres := scope.Associative(row, "return"); pres {
		return value
	}
	return self.return_value
}

func (self TestFunction) Name() string {
	return "func_foo"
}

func makeScope() *Scope {
	return NewScope().AppendVars(NewDict().
		Set("const_foo", 1).
		Set("foo", NewDict().
			Set("bar", NewDict().Set("baz", 5)).
			Set("bar2", 7)),
	).AppendFunctions(
		TestFunction{1},
	).AppendPlugins(
		GenericListPlugin{
			PluginName:  "range",
			Description: "Return a range of numbers.",
			Function: func(args *Dict) []Row {
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
	result := <-value.Reduce(ctx, scope)
	defer cancel()
	if !scope.Eq(result, "foo") {
		t.Fatalf("Expected %v, got %v", "foo", foo)
	}
}

func TestEvalWhereClause(t *testing.T) {
	scope := makeScope()
	for _, test := range execTests {
		preamble := "select * from plugin() where \n"
		sql, err := Parse(preamble + test.clause)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", test.clause, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		output := sql.query.Where.Reduce(ctx, scope)
		value, ok := <-output
		if !ok {
			t.Fatalf("No output from channel")
			return
		}
		if !scope.Eq(value, test.result) {
			Debug(sql)
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
		sql, err := Parse(preamble + test.clause)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", test.clause, err)
		}

		vql_string := sql.ToString(scope)

		parsed_sql, err := Parse(vql_string)
		if err != nil {
			t.Fatalf("Failed to parse stringified VQL %v: %v (%v)",
				vql_string, err, test.clause)
		}

		if !reflect.DeepEqual(parsed_sql, sql) {
			Debug(sql)
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

func (self _RepeaterPlugin) Name() string {
	return "repeater"
}

func (self _RepeaterPlugin) Info(type_map *TypeMap) *PluginInfo {
	return &PluginInfo{}
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
