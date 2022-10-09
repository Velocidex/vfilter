package scope_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Velocidex/ordereddict"
	"github.com/sebdah/goldie/v2"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/functions"
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
	functions.Aggregator
}

func (self DestructorFunction) Call(
	ctx context.Context, scope types.Scope, args *ordereddict.Dict) types.Any {
	arg := DestructorFunctionArgs{}
	err := arg_parser.ExtractArgs(scope, args, &arg)
	if err != nil {
		panic(err)
	}

	count := 0
	count_any, ok := self.GetContext(scope)
	if ok {
		count = count_any.(int)
	}

	count++

	mu.Lock()
	markers = append(markers, fmt.Sprintf("Func Open %s %x", arg.Name, count))
	mu.Unlock()

	scope.AddDestructor(func() {
		logMarkers("Func Close %s %x", arg.Name, count)
	})

	self.SetContext(scope, count)

	return count
}

func (self DestructorFunction) Info(scope types.Scope, type_map *types.TypeMap) *types.FunctionInfo {
	return &types.FunctionInfo{
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

	args *ordereddict.Dict) <-chan types.Row {
	output_chan := make(chan types.Row)

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
			select {
			case <-ctx.Done():
				return
			case output_chan <- ordereddict.NewDict().Set("Count", i):
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	return output_chan
}

func (self DestructorPlugin) Info(scope types.Scope, type_map *types.TypeMap) *types.PluginInfo {
	return &types.PluginInfo{
		Name: "destructor",
	}
}

type vqlTest struct {
	name string
	vql  string
}

var scopeTests = []vqlTest{
	{"Destructor as function",
		"SELECT destructor() AS X FROM scope()"},
	{"Destructor as plugin",
		"SELECT * FROM destructor()"},

	// Func destructor is called once per row, plugin destructor
	// only at start and end.
	{"Both", "SELECT destructor() AS X FROM destructor(rows=2)"},

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

		scope := vfilter.NewScope().
			AppendFunctions(DestructorFunction{}).
			AppendPlugins(&DestructorPlugin{})

		multi_vql, err := vfilter.MultiParse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		query := ""
		for _, vql := range multi_vql {
			ctx := context.Background()
			var output []types.Row
			for row := range vql.Eval(ctx, scope) {
				output = append(output, vfilter.RowToDict(ctx, scope, row))
			}
			query += vql.ToString(scope)
		}
		// Close the scope to force destructors to be called.
		scope.Close()

		mu.Lock()
		result.Set(fmt.Sprintf(
			"%03d %s: %s - markers", i, testCase.name, query),
			markers)
		mu.Unlock()
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "TestDestructors", result)
}

func logMarkers(format string, args ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	markers = append(markers, fmt.Sprintf(format, args...))
}
