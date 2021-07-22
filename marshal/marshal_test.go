package marshal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/alecthomas/assert"
	"github.com/sebdah/goldie/v2"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/marshal"
	"www.velocidex.com/golang/vfilter/scope"
	"www.velocidex.com/golang/vfilter/types"
)

var marshalTestCases = []struct {
	name     string
	pre_vql  string
	post_vql string
}{
	{`Simple materialized`,
		`LET X <= 1`, `SELECT X FROM scope()`},

	{`Materialized query`,
		`LET X <= SELECT _value, _value + 1 AS A FROM range(start=0, end=5, step=1)`,
		`SELECT * FROM X WHERE _value = 4`},

	{`Stored Query`,
		`LET X = SELECT _value FROM range(start=0, end=5, step=1)`,
		`SELECT * FROM X WHERE _value = 2`},

	{`Lazy Expression`,
		`LET X = 1 + 2`,
		`SELECT X FROM scope()`},

	{`VQL Functions`,
		`LET X(Y) = 1 + Y`,
		`SELECT X(Y=1) FROM scope()`},

	{`Stored Query with parameters`,
		`LET X(Y) = SELECT Y FROM scope()`,
		`SELECT * FROM X(Y=1)`},

	{`OrderedDict materialized`,
		`LET X <= dict(A=1)`, `SELECT X FROM scope()`},
}

func TestMarshal(t *testing.T) {
	// Build an unmarshaller
	unmarshaller := marshal.NewUnmarshaller()
	unmarshaller.Handlers["Scope"] = scope.ScopeUnmarshaller{}
	unmarshaller.Handlers["Replay"] = vfilter.ReplayUnmarshaller{}
	unmarshaller.Handlers["OrderedDict"] = vfilter.OrdereddictUnmarshaller{}

	results := ordereddict.NewDict()

	for idx, testCase := range marshalTestCases {
		if false && idx != 5 {
			continue
		}

		scope := makeTestScope()
		multi_vql, err := vfilter.MultiParse(testCase.pre_vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.pre_vql, err)
		}

		// Ignore the rows returns in the pre_vql - it is just
		// used to set up the scope.
		ctx := context.Background()
		for _, vql := range multi_vql {
			for _ = range vql.Eval(ctx, scope) {
			}
		}

		// Serialize the scope.
		intermediate, err := marshal.Marshal(scope, scope)
		assert.NoError(t, err)

		serialized, err := json.MarshalIndent(intermediate, "  ", "  ")
		assert.NoError(t, err)

		results.Set(fmt.Sprintf("%v: Marshal %v", idx, testCase.name),
			intermediate)

		unmarshal_item := &types.MarshalItem{}
		err = json.Unmarshal(serialized, &unmarshal_item)
		assert.NoError(t, err)

		new_scope := makeTestScope()
		_, err = unmarshaller.Unmarshal(unmarshaller,
			new_scope, unmarshal_item)
		assert.NoError(t, err)

		multi_vql, err = vfilter.MultiParse(testCase.post_vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.post_vql, err)
		}

		rows := make([]vfilter.Row, 0)
		for _, vql := range multi_vql {
			for row := range vql.Eval(ctx, scope) {
				rows = append(rows, row)
			}
		}

		results.Set(fmt.Sprintf("%v: Rows %v", idx, testCase.name), rows)
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "Serialization", results)
}

func makeTestScope() types.Scope {
	env := ordereddict.NewDict().
		Set("const_foo", 1)

	result := vfilter.NewScope().AppendVars(env)
	result.SetLogger(log.New(os.Stdout, "Log: ", log.Ldate|log.Ltime|log.Lshortfile))
	return result
}
