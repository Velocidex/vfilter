package vfilter

import (
	"context"
	"testing"
)

type execPluginTest struct {
	query  string
	result []*Dict
}

var execPluginTests = []execPluginTest{
	execPluginTest{
		query: "select * from test_plugin() where foo.bar < 2",
		result: []*Dict{
			NewDict().
				Set("foo", NewDict().Set("bar", 1)).
				Set("foo_2", 2).
				Set("foo_3", 3),
		},
	},
	execPluginTest{
		query: ("select foo.bar as column1, foo.bar from " +
			"test_plugin() where foo.bar = 2"),
		result: []*Dict{
			NewDict().
				Set("column1", 2).
				Set("foo.bar", 2),
		},
	},
}

// Implement some test plugins for testing.
type TestGeneratorPlugin struct{}

func (self TestGeneratorPlugin) Call(
	ctx context.Context,
	scope *Scope,
	args *Dict) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		for i := 1; i < 10; i++ {
			row := NewDict().
				Set("foo", NewDict().Set("bar", i)).
				Set("foo_2", i*2).
				Set("foo_3", i*3)
			output_chan <- row
		}

	}()

	return output_chan
}

func (self TestGeneratorPlugin) Name() string {
	return "test_plugin"
}

func (self TestGeneratorPlugin) Info(type_map *TypeMap) *PluginInfo {
	return &PluginInfo{}
}

func TestPlugins(t *testing.T) {
	scope := NewScope().AppendPlugins(TestGeneratorPlugin{})
	for _, test := range execPluginTests {
		sql, err := Parse(test.query)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", test.query, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		input := sql.Eval(ctx, scope)
		var result []Row
		for {
			row, ok := <-input
			if !ok {
				break
			}
			result = append(result, row)
		}

		if !scope.Eq(result, test.result) {
			Debug(scope)
			Debug(result)
			Debug(test.result)
			t.Fatalf("Query %v Failed.", test.query)
		}
	}
}
