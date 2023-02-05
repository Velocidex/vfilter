package materializer

import (
	"context"
	"encoding/json"

	"www.velocidex.com/golang/vfilter/types"
)

// An in memory materializer - this is equivalent to the old behavior
// of expanding all rows in memory.
type InMemoryMatrializer struct {
	rows []types.Row
}

func NewInMemoryMatrializer(rows []types.Row) *InMemoryMatrializer {
	return &InMemoryMatrializer{rows}
}

// Support StoredQuery protocol.
func (self InMemoryMatrializer) Eval(
	ctx context.Context, scope types.Scope) <-chan types.Row {

	output_chan := make(chan types.Row)
	go func() {
		defer close(output_chan)

		for _, row := range self.rows {
			select {
			case <-ctx.Done():
				return
			case output_chan <- row:
			}
		}
	}()

	return output_chan
}

// Support indexing (Associative protocol)
func (self InMemoryMatrializer) Applicable(a types.Any, b types.Any) bool {
	_, ok := a.(*InMemoryMatrializer)
	if !ok {
		return false
	}

	return true
}

// Just deletegate to our contained rows array.
func (self InMemoryMatrializer) GetMembers(scope types.Scope, a types.Any) []string {
	a_materializer, ok := a.(*InMemoryMatrializer)
	if !ok {
		return nil
	}

	return scope.GetMembers(a_materializer.rows)
}

func (self InMemoryMatrializer) Associative(scope types.Scope, a types.Any, b types.Any) (res types.Any, pres bool) {
	a_materializer, ok := a.(*InMemoryMatrializer)
	if !ok {
		return nil, false
	}

	return scope.Associative(a_materializer.rows, b)
}

// Support JSON Marshal protocol
func (self *InMemoryMatrializer) MarshalJSON() ([]byte, error) {
	return json.Marshal(self.rows)
}

// An object implementing the ScopeMaterializer interface. This is the
// default meterializer used in VQL to expand a LET query into the
// scope. It returns wrapper that contains the list of rows in
// memory. Library users may register a more sophisticated
// meterializer that backs data in more scalable storage to avoid
// memory costs.
type DefaultMaterializer struct{}

func (self DefaultMaterializer) Materialize(
	ctx context.Context, scope types.Scope,
	operator string, query types.StoredQuery) types.StoredQuery {
	rows := types.Materialize(ctx, scope, query)
	return &InMemoryMatrializer{rows}
}
