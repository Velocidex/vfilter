package vfilter

import (
	"context"
	"fmt"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/sebdah/goldie/v2"
)

type lazyTypeTest struct {
	Const string
}

func (self lazyTypeTest) Foo() string {
	logMarkers("Foo ran")
	return "Hello"
}

func (self lazyTypeTest) Bar() string {
	logMarkers("Bar ran")
	return "Goodbye"
}

func (self lazyTypeTest) Close() {
	logMarkers("Close ran")
}

// Advertise only some fields and methods
func (self lazyTypeTest) Members() []string {
	return []string{"Foo", "Const"}
}

type LazyPluginArgs struct {
	Rows int64 `vfilter:"optional,field=rows"`
}

type LazyPlugin struct {
	count int
}

func (self *LazyPlugin) Call(
	ctx context.Context, scope *Scope,

	args *ordereddict.Dict) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		arg := LazyPluginArgs{}
		err := ExtractArgs(scope, args, &arg)
		if err != nil {
			panic(err)
		}

		if arg.Rows == 0 {
			arg.Rows = 1
		}

		for i := int64(0); i < arg.Rows; i++ {
			output_chan <- lazyTypeTest{}
		}
	}()

	return output_chan
}

func (self LazyPlugin) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "lazy",
	}
}

type LazyDictPlugin struct {
	count int
}

func (self *LazyDictPlugin) Call(
	ctx context.Context, scope *Scope,

	args *ordereddict.Dict) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		arg := LazyPluginArgs{}
		err := ExtractArgs(scope, args, &arg)
		if err != nil {
			panic(err)
		}

		if arg.Rows == 0 {
			arg.Rows = 1
		}

		for i := int64(0); i < arg.Rows; i++ {
			output_chan <- ordereddict.NewDict().
				Set("Const", 10).
				Set("Foo", func() Any {
					logMarkers("Foo ran")
					return "Hello"
				}).
				Set("Bar", func() Any {
					logMarkers("Bar ran")
					return "Goodbye"
				})
		}
	}()

	return output_chan
}

func (self LazyDictPlugin) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "lazy_dict",
	}
}

var lazyTests = []vqlTest{
	{"Lazy function hide method",
		"SELECT * FROM lazy()"},
	{"Lazy function extra method",
		"SELECT *, Bar FROM lazy()"},

	{"Lazy dict plugin",
		"SELECT Bar FROM lazy_dict()"},
}

// Test the correct destructor call order
func TestLazy(t *testing.T) {
	result := ordereddict.NewDict()
	for i, testCase := range lazyTests {
		markers = []string{}

		scope := NewScope().AppendPlugins(&LazyPlugin{}, &LazyDictPlugin{})

		multi_vql, err := MultiParse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		query := ""
		var output []Row

		for _, vql := range multi_vql {
			ctx := context.Background()
			for row := range vql.Eval(ctx, scope) {
				output = append(output, RowToDict(ctx, scope, row))
			}
			query += vql.ToString(scope)
		}
		// Close the scope to force destructors to be called.
		scope.Close()

		result.Set(fmt.Sprintf(
			"%03d %s: %s", i, testCase.name, query),
			output)
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
	g.AssertJson(t, "TestLazy", result)
}

func logMarkers(format string, args ...interface{}) {
	mu.Lock()
	defer mu.Unlock()

	markers = append(markers, fmt.Sprintf(format, args...))
}
