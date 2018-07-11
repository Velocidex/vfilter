package vfilter

import (
	"context"
	"encoding/json"
	"fmt"
)

// A response from VQL queries.
type VFilterJsonResult struct {
	Part    int
	Columns []string
	Payload []byte
}

// Returns a channel over which multi part results are sent.
func GetResponseChannel(
	vql *VQL, ctx context.Context, scope *Scope,
	maxrows int) <-chan *VFilterJsonResult {
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
			result_chan <- &VFilterJsonResult{
				Part:    part,
				Columns: *columns,
				Payload: s,
			}

			rows = []Row{}
			part += 1
		}
		defer ship_payload()
		for row := range row_chan {
			// Send the payload.
			if len(rows) > maxrows {
				ship_payload()
			}

			if len(*columns) == 0 {
				members := scope.GetMembers(row)
				columns = &members
			}
			if len(*columns) == 0 {
				rows = append(rows, row)

			} else {
				new_row := NewDict()
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
		}
	}()

	return result_chan
}

// A convenience function to generate JSON output from a VQL query.
func OutputJSON(vql *VQL, ctx context.Context, scope *Scope) ([]byte, error) {
	output_chan := vql.Eval(ctx, scope)
	columns := vql.Columns(scope)
	result := []Row{}
	for row := range output_chan {
		if len(*columns) == 0 {
			members := scope.GetMembers(row)
			columns = &members
		}
		if len(*columns) == 0 {
			result = append(result, row)

		} else {
			new_row := NewDict()
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
	}

	s, err := json.MarshalIndent(result, "", " ")
	return s, err
}
