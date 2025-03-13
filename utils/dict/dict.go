package dict

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

// RowToDict reduces the row into a simple Dict. This materializes any
// lazy queries that are stored in the row into a stable materialized
// dict.
func RowToDict(
	ctx context.Context,
	scope types.Scope, row types.Row) *ordereddict.Dict {

	// Even if it is already a dict we still need to iterate its
	// values to make sure they are fully materialized.
	result := ordereddict.NewDict()
	for _, column := range scope.GetMembers(row) {
		value, pres := scope.Associative(row, column)
		if pres {
			result.Set(column, normalize_value(ctx, scope, value, 0))
		}
	}

	return result
}

// Recursively convert types in the rows to standard types to allow
// for json encoding.
func normalize_value(ctx context.Context,
	scope types.Scope, value types.Any, depth int) types.Any {
	if depth > 10 {
		return types.Null{}
	}

	if value == nil {
		value = types.Null{}
	}

	switch t := value.(type) {

	// All valid JSON types.
	case string, types.Null, *types.Null, bool, float64, int, uint,
		int8, int16, int32, int64,
		uint8, uint16, uint32, uint64,
		time.Time, *time.Time,
		*ordereddict.Dict:
		return value

	case fmt.Stringer:
		return value

	case []byte:
		return t

		// Reduce any LazyExpr to materialized types
	case types.LazyExpr:
		return normalize_value(ctx, scope, t.Reduce(ctx), depth+1)

		// Materialize stored queries into an array.
	case types.StoredQuery:
		result := types.Materialize(ctx, scope, t)
		return result

		// A dict may expose a callable as a member - we just
		// call it lazily if it is here.
	case func() types.Any:
		return normalize_value(ctx, scope, t(), depth+1)

	case types.Materializer:
		return t.Materialize(ctx, scope)

	case types.Memberer:
		result := ordereddict.NewDict()
		for _, member := range t.Members() {
			value, pres := scope.Associative(t, member)
			if !pres {
				value = types.Null{}
			}
			result.Set(member,
				normalize_value(ctx, scope, value, depth+1))
		}
		return result

	default:
		a_value := reflect.ValueOf(value)
		a_value = reflect.Indirect(a_value)
		a_type := reflect.TypeOf(value)
		if a_type == nil {
			return types.Null{}
		}

		if a_type.Kind() == reflect.Slice || a_type.Kind() == reflect.Array {
			if types.IsNil(a_value) {
				return types.Null{}
			}

			length := a_value.Len()
			result := make([]types.Any, 0, length)
			for i := 0; i < length; i++ {
				result = append(result, normalize_value(
					ctx, scope, a_value.Index(i).Interface(), depth+1))
			}
			return result

			// Map keys are random so they lead to unstable output
		} else if a_type.Kind() == reflect.Map {
			if types.IsNil(a_value) {
				return types.Null{}
			}

			result := ordereddict.NewDict()
			for _, key := range a_value.MapKeys() {
				str_key, ok := key.Interface().(string)
				if ok {
					result.Set(str_key, normalize_value(
						ctx, scope, a_value.MapIndex(key).Interface(),
						depth+1))
				}
			}

			// Sort the output to make keys stable
			stable_dict := ordereddict.NewDict()
			keys := result.Keys()
			sort.Strings(keys)
			for _, k := range keys {
				v, _ := result.Get(k)
				stable_dict.Set(k, v)
			}

			return stable_dict
		}

		return value
	}
}
