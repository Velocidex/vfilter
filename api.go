package vfilter

import (
	"context"
	"encoding/json"
	"fmt"
)

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
					case fmt.Stringer:
						cell = t.String()

					case []byte:
						cell = string(t)
					default:
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
