package vfilter

import (
	"context"
	"fmt"
	"time"

	"www.velocidex.com/golang/vfilter/types"
)

// A response from VQL queries.
type VFilterJsonResult struct {
	Part      int
	TotalRows int
	Columns   []string
	Payload   []byte
}

type RowEncoder func(rows []Row) ([]byte, error)

// Returns a channel over which multi part results are sent.
func GetResponseChannel(
	vql *VQL,
	ctx context.Context,
	scope types.Scope,
	encoder RowEncoder,
	maxrows int,
	// Max time to wait before returning some results.
	max_wait int) <-chan *VFilterJsonResult {
	result_chan := make(chan *VFilterJsonResult)

	go func() {
		defer close(result_chan)

		part := 0
		row_chan := vql.Eval(ctx, scope)
		rows := []Row{}

		ship_payload := func() {
			s, err := encoder(rows)
			if err != nil {
				scope.Log("Unable to serialize: %v", err)
				return
			}

			result := &VFilterJsonResult{
				Part:      part,
				TotalRows: len(rows),
				Payload:   s,
			}

			// We dont know the columns but we have at
			// least one row. Set the columns from this
			// row.
			if len(rows) > 0 {
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
				if len(rows) >= maxrows {
					ship_payload()
					deadline = time.After(time.Duration(max_wait) *
						time.Second)
				}

				value := RowToDict(ctx, scope, row)
				rows = append(rows, value)

				// Throttle if needed.
				ChargeOp(scope)
			}
		}
	}()

	return result_chan
}

// A convenience function to generate JSON output from a VQL query.
func OutputJSON(
	vql *VQL,
	ctx context.Context,
	scope types.Scope,
	encoder RowEncoder) ([]byte, error) {
	output_chan := vql.Eval(ctx, scope)
	result := []Row{}

	for row := range output_chan {
		value := RowToDict(ctx, scope, row)
		result = append(result, value)

		// Throttle if needed.
		ChargeOp(scope)
	}

	s, err := encoder(result)
	return s, err
}

type Empty struct{}

func ToString(a types.Any, scope types.Scope) string {
	stinger, ok := a.(StringProtocol)
	if ok {
		return stinger.ToString(scope)
	}

	return fmt.Sprintf("%v", a)
}
