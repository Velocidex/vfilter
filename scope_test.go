package vfilter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Velocidex/ordereddict"
	"github.com/sebdah/goldie/v2"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

var (
	mu      sync.Mutex
	markers = []string{}
)

type DestructorFunctionArgs struct {
	Name string `vfilter:"optional,field=name"`
}

type DestructorFunction struct {
	count int
}

func (self *DestructorFunction) Call(
	ctx context.Context, scope types.Scope, args *ordereddict.Dict) Any {
	arg := DestructorFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, &arg)
	if err != nil {
		panic(err)
	}

	self.count++
	markers = append(markers, fmt.Sprintf("Func Open %s %x", arg.Name, self.count))
	scope.AddDestructor(func() {
		logMarkers("Func Close %s %x", arg.Name, self.count)
	})

	return self.count
}

func (self DestructorFunction) Info(scope types.Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "destructor",
	}
}

type DestructorPluginArgs struct {
	Name string `vfilter:"optional,field=name"`
	Rows int64  `vfilter:"optional,field=rows"`
}

type DestructorPlugin struct {
	count int
}

func (self *DestructorPlugin) Call(
	ctx context.Context, scope types.Scope,

	args *ordereddict.Dict) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		arg := DestructorPluginArgs{}
		err := arg_parser.ExtractArgs(scope, args, &arg)
		if err != nil {
			panic(err)
		}

		if arg.Rows == 0 {
			arg.Rows = 1
		}

		self.count++
		logMarkers("Plugin Open %s %x", arg.Name, self.count)

		scope.AddDestructor(func() {
			logMarkers("Plugin Close %s %x", arg.Name, self.count)
		})

		for i := int64(0); i < arg.Rows; i++ {
			output_chan <- ordereddict.NewDict().Set("Count", i)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	return output_chan
}

func (self DestructorPlugin) Info(scope types.Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "destructor",
	}
}

var scopeTests = []vqlTest{
	{"Destructor as function",
		"SELECT destructor() AS X FROM scope()"},
	{"Destructor as plugin",
		"SELECT * FROM destructor()"},

	// Func destructor is called once per row, plugin destructor
	// only at start and end.
	{"Both", "SELECT destructor() AS X FROM destructor(rows=2)"},

	// Rows evaluate first then query
	{"Nested foreach - destructors in row clause", `
SELECT * FROM foreach(
 row={SELECT destructor(name='row_func') FROM scope()},
 query={
      SELECT * FROM destructor(name='inner_query')
})`},

	// Rows plugins evaluate first then query
	{"Nested foreach - destructor in query clause", `
SELECT * FROM foreach(
 row={SELECT * FROM destructor(name='rows_query', rows=2)},
 query={
      SELECT destructor(name='iterator_func') FROM scope()
})`},

	// Columns who do not get evaluated do not call destructors.
	{"Lazy function", `
SELECT destructor(name='lazy_func') AS X FROM scope()
WHERE FALSE
`},
	// Materialized boundaries are rows - each row emitted in a
	// stored query will be materialized fully - and therefore
	// call destructors.
	{"Lazy stored function", `
LET lazy(x) = destructor(name='lazy_func')

SELECT lazy(x=1) FROM scope()
WHERE FALSE
`},

	{"Lazy stored function evaluated", `
LET lazy(x) = destructor(name='lazy_func')

SELECT lazy(x=1) AS X FROM scope()
WHERE X AND FALSE
`},

	{"Lazy stored query", `
LET lazy(x) = SELECT * FROM destructor(name='stored_query', rows=2)

SELECT X FROM lazy(x=1)
WHERE FALSE
`},

	{"Indirect functions", `
SELECT dict(x=destructor(name='inner')) AS Foo FROM scope()
`},

	// All should open and all should close at the end of the scope.
	{"Multiple functions", `
SELECT destructor(name='one'), destructor(name='two'), destructor(name='three') FROM scope()
`},
}

// Test the correct destructor call order
func TestDestructors(t *testing.T) {
	result := ordereddict.NewDict()
	for i, testCase := range scopeTests {
		mu.Lock()
		markers = []string{}
		mu.Unlock()

		scope := NewScope().
			AppendFunctions(&DestructorFunction{}).
			AppendPlugins(&DestructorPlugin{})

		multi_vql, err := MultiParse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		query := ""
		for _, vql := range multi_vql {
			ctx := context.Background()
			var output []Row
			for row := range vql.Eval(ctx, scope) {
				output = append(output, RowToDict(ctx, scope, row))
			}
			query += vql.ToString(scope)
		}
		// Close the scope to force destructors to be called.
		scope.Close()

		result.Set(fmt.Sprintf(
			"%03d %s: %s - markers", i, testCase.name, query),
			markers)
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "TestDestructors", result)
}