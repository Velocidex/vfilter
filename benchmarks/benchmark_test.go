package benchmarks

import (
	"context"
	"fmt"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/alecthomas/assert"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/scope"
	"www.velocidex.com/golang/vfilter/types"
)

func makeScope() vfilter.Scope {
	env := ordereddict.NewDict()
	result := scope.NewScope().AppendVars(env)
	env.Set("RootEnv", env)
	//result.SetLogger(log.New(os.Stdout, "Log: ", log.Ldate|log.Ltime|log.Lshortfile))
	return result
}

func runBenchmark(b *testing.B, query string) {
	// Store the result in ordered dict so we have a consistent golden file.
	result := ordereddict.NewDict()
	scope := makeScope()

	multi_vql, err := vfilter.MultiParse(query)
	assert.NoError(b, err, "Failed to parse %v: %v", query, err)

	ctx := context.Background()
	for idx, vql := range multi_vql {
		var output []types.Row

		for row := range vql.Eval(ctx, scope) {
			output = append(output, vfilter.RowToDict(ctx, scope, row))
		}

		result.Set(fmt.Sprintf("%03d %s: %s", idx, query,
			vql.ToString(scope)), output)
	}
}

func BenchmarkRange10k(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runBenchmark(b, `
SELECT format(format='value %v', args=_value) AS Value
FROM range(start=0, step=1, end=10000)
WHERE Value =~ '.' AND 1 = 1 AND _value > 10`,
		)
	}
}

func BenchmarkForeach10k(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runBenchmark(b, `
SELECT * FROM foreach(row={
    SELECT * FROM range(start=0, step=1, end=10000)
}, query={
    SELECT format(format='value %v', args=_value) FROM scope()
})`)
	}
}

func BenchmarkForeachWithWorkers10k(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runBenchmark(b, `
SELECT * FROM foreach(row={
    SELECT * FROM range(start=0, step=1, end=10000)
}, workers=10, query={
    SELECT format(format='value %v', args=_value) FROM scope()
})`)
	}
}
