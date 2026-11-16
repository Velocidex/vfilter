package vfilter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Velocidex/ordereddict"
	"github.com/stretchr/testify/assert"
	"www.velocidex.com/golang/vfilter/types"
)

type copyRaceState struct {
	copying int64
	copies  int64
}

// copyIdFunction returns the id of the copy that handled the
// call. Copy() holds the first caller until a second one is also
// copying (or a timeout passes), which forces two goroutines to
// race on caching the copy in the same AST node.
type copyIdFunction struct {
	id    int64
	state *copyRaceState
}

func (self copyIdFunction) Copy() types.FunctionInterface {
	atomic.AddInt64(&self.state.copying, 1)
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt64(&self.state.copying) < 2 &&
		time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	return copyIdFunction{
		id:    atomic.AddInt64(&self.state.copies, 1),
		state: self.state,
	}
}

func (self copyIdFunction) Call(
	ctx context.Context, scope types.Scope, args *ordereddict.Dict) Any {
	return self.id
}

func (self copyIdFunction) Info(
	scope types.Scope, type_map *TypeMap) *FunctionInfo {
	return &FunctionInfo{
		Name: "copy_id",
	}
}

// Aggregates keep their state in the function copy cached on the
// AST node. foreach workers evaluate the same AST, so every worker
// must end up calling the same copy. Two workers could each make a
// copy, and the second one stored replaced the first after other
// rows had already used it, so count() came out short.
func TestFunctionCopySharedByConcurrentCallers(t *testing.T) {
	state := &copyRaceState{}
	scope := makeScope().AppendFunctions(copyIdFunction{state: state})
	defer scope.Close()

	vql, err := Parse(`
SELECT * FROM foreach(
  row={ SELECT * FROM range() },
  query={ SELECT copy_id() AS Id FROM scope() },
  workers=2)`)
	assert.NoError(t, err)

	ids := make(map[int64]int)
	for row := range vql.Eval(context.Background(), scope) {
		id, _ := scope.Associative(row, "Id")
		ids[id.(int64)]++
	}

	// All 14 rows must come from one copy of the function.
	assert.Equal(t, 1, len(ids), "rows by function copy: %v", ids)
}
