package vfilter

import (
	"context"
	"fmt"
	"encoding/json"
)


// A convenience function to generate JSON output from a VQL query.
func OutputJSON(vql *VQL, ctx context.Context, scope *Scope) ([]byte, error) {
	output_chan := vql.Eval(ctx, scope)
	result := []Row{}
	columns := vql.Columns(scope)
	for row := range output_chan {
		new_row := Dict{}
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
				new_row[key] = cell
			}
		}
		result = append(result, new_row)
	}

	s, err := json.MarshalIndent(result, "", " ")
	return s, err
}
