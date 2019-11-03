package vfilter

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Velocidex/ordereddict"
)

// A response from VQL queries.
type VFilterJsonResult struct {
	Part      int
	TotalRows int
	Columns   []string
	Payload   []byte
}

// Returns a channel over which multi part results are sent.
func GetResponseChannel(
	vql *VQL,
	ctx context.Context,
	scope *Scope,
	maxrows int,
	// Max time to wait before returning some results.
	max_wait int) <-chan *VFilterJsonResult {
	result_chan := make(chan *VFilterJsonResult)

	go func() {
		defer close(result_chan)

		part := 0
		row_chan := vql.Eval(ctx, scope)
		columns := vql.Columns(scope)
		rows := []Row{}

		ship_payload := func() {
			s, err := json.MarshalIndent(rows, "", " ")
			if err != nil {
				scope.Log("Unable to serialize: %v",
					err.Error())
				return
			}

			result := &VFilterJsonResult{
				Part:      part,
				TotalRows: len(rows),
				Columns:   *columns,
				Payload:   s,
			}

			// We dont know the columns but we have at
			// least one row. Set the columns from this
			// row.
			if len(result.Columns) == 0 && len(rows) > 0 {
				result.Columns = scope.GetMembers(rows[0])
			}

			result_chan <- result

			rows = []Row{}
			part += 1
		}
		// Send the last payload outstanding.
		defer ship_payload()
		deadline := time.After(time.Duration(max_wait) * time.Second)

		for {

			select {
			case <-ctx.Done():
				return

			// If the query takes too long, send what we
			// have.
			case <-deadline:
				if len(rows) > 0 {
					ship_payload()
				}
				// Update the deadline to re-fire next.
				deadline = time.After(time.Duration(max_wait) * time.Second)

			case row, ok := <-row_chan:
				if !ok {
					return
				}

				// Send the payload if it is too full.
				if len(rows) > maxrows {
					ship_payload()
					deadline = time.After(time.Duration(max_wait) *
						time.Second)
				}

				if len(*columns) == 0 {
					rows = append(rows, row)

				} else {
					new_row := ordereddict.NewDict()
					for _, key := range *columns {
						value, pres := scope.Associative(row, key)
						if pres && !IsNil(value) {
							var cell Any
							switch t := value.(type) {
							case Null:
								cell = nil

							case fmt.Stringer:
								cell = value

							case []byte:
								cell = string(t)

							case StoredQuery:
								cell = Materialize(
									ctx, scope, t)

							default:
								// Pass directly to
								// Json Marshal
								cell = value
							}
							new_row.Set(key, cell)
						}
					}
					rows = append(rows, new_row)
				}

				// Throttle if needed.
				ChargeOp(scope)
			}
		}
	}()

	return result_chan
}

// A convenience function to generate JSON output from a VQL query.
func OutputJSON(vql *VQL, ctx context.Context, scope *Scope) ([]byte, error) {
	output_chan := vql.Eval(ctx, scope)
	columns := vql.Columns(scope)
	result := []Row{}

	// If the caller provided a throttler in the scope we
	// use it. We charge 1 op per row.
	any_throttle, _ := scope.Resolve("$throttle")
	throttle, _ := any_throttle.(<-chan time.Time)

	for row := range output_chan {
		if len(*columns) == 0 {
			members := scope.GetMembers(row)
			columns = &members
		}
		if len(*columns) == 0 {
			result = append(result, row)

		} else {
			new_row := ordereddict.NewDict()
			for _, key := range *columns {
				value, pres := scope.Associative(row, key)
				if pres && !IsNil(value) {
					var cell Any
					switch t := value.(type) {
					case Null:
						cell = nil

					case fmt.Stringer:
						cell = value

					case []byte:
						cell = string(t)

					case StoredQuery:
						cell = Materialize(ctx, scope, t)

					default:
						// Pass directly to
						// Json Marshal
						cell = value
					}
					new_row.Set(key, cell)
				}
			}
			result = append(result, new_row)
		}
		if throttle != nil {
			<-throttle
		}
	}

	s, err := json.MarshalIndent(result, "", " ")
	return s, err
}
