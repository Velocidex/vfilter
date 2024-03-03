package explain

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/sebdah/goldie/v2"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils/dict"
)

type CapturingLogger struct {
	rows []string
}

func (self *CapturingLogger) Write(b []byte) (int, error) {
	self.rows = append(self.rows, string(b))
	return len(b), nil
}

var explainTests = []struct{ name, vql string }{
	// Queries without the EXPLAIN keyword will not be explained
	{"No Explain", "SELECT 'No Explain' FROM range(end=1)"},

	{"Simple Explain", "EXPLAIN SELECT 'A' FROM range(end=1)"},
	{"Query with WHERE", "EXPLAIN SELECT * FROM range(end=10) WHERE _value = 2"},
	{"Error Arg Parsing", "EXPLAIN SELECT 'A' FROM range(end=1, foo=2)"},
}

func makeTestScope(logger *CapturingLogger) types.Scope {
	result := vfilter.NewScope()
	result.SetExplainer(NewLoggingExplainer(result))
	result.SetLogger(
		log.New(logger, "", 0))
	return result
}

func TestExplain(t *testing.T) {
	result := ordereddict.NewDict()
	for i, testCase := range explainTests {
		logger := &CapturingLogger{}
		scope := makeTestScope(logger)
		multi_vql, err := vfilter.MultiParse(testCase.vql)
		if err != nil {
			t.Fatalf("Failed to parse %v: %v", testCase.vql, err)
		}

		ctx := context.Background()
		for idx, vql := range multi_vql {
			var output []vfilter.Row
			for row := range vql.Eval(ctx, scope) {
				output = append(output, dict.RowToDict(ctx, scope, row))
			}
			for _, row := range logger.rows {
				output = append(output, row)
			}

			result.Set(fmt.Sprintf("%03d/%03d %s: %s", i, idx, testCase.name,
				vfilter.FormatToString(scope, vql)), output)
			logger.rows = nil
		}
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "TestExplain", result)
}
