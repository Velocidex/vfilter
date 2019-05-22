package vfilter

import (
	"context"
	"reflect"
)

type _FlattenPluginImplArgs struct {
	Query StoredQuery `vfilter:"required,field=query"`
}

type _FlattenPluginImpl struct{}

func (self _FlattenPluginImpl) Call(ctx context.Context,
	scope *Scope,
	args *Dict) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		arg := _FlattenPluginImplArgs{}
		err := ExtractArgs(scope, args, &arg)
		if err != nil {
			scope.Log("flatten: %v", err)
			return
		}

		row_chan := arg.Query.Eval(ctx, scope)
		for {
			row_item, ok := <-row_chan
			if !ok {
				break
			}
			members := scope.GetMembers(row_item)
			for _, item := range flatten(scope, row_item, members, 0) {
				output_chan <- item
			}
		}
	}()

	return output_chan
}

func makeDict(scope *Scope, item Any) *Dict {
	result_dict, ok := item.(*Dict)
	if ok {
		return result_dict
	}

	result := NewDict()
	for _, member := range scope.GetMembers(item) {
		value, pres := scope.Associative(item, member)
		if pres {
			result.Set(member, value)
		}
	}
	return result
}

func flatten(scope *Scope, item Row, members []string, idx int) []*Dict {
	result := []*Dict{}
	if idx >= len(members) {
		return result
	}

	tail := flatten(scope, item, members, idx+1)
	column := members[idx]
	cell, pres := scope.Associative(item, column)
	if !pres {
		return tail
	}

	slice := reflect.Indirect(reflect.ValueOf(cell))
	if slice.Type().Kind() == reflect.Slice {
		switch slice.Type().Elem().Kind() {
		case reflect.String, reflect.Struct, reflect.Interface,
			reflect.Map, reflect.Array:
			for i := 0; i < slice.Len(); i++ {
				original_value := slice.Index(i).Interface()
				if len(tail) == 0 {
					result = append(
						result,
						NewDict().Set(column, original_value))
				} else {
					for _, subrow := range tail {
						new_row := NewDict()
						new_row.Set(column, original_value)

						new_row.MergeFrom(subrow)

						result = append(result, new_row)
					}
				}
			}

			return result
		}
	}

	// Not an array - just set this column and pass all the
	// expansions up the call chain.
	if len(tail) == 0 {
		result = append(result, NewDict().Set(column, cell))
	} else {
		for _, subrow := range tail {
			subrow.Set(column, cell)
			result = append(result, subrow)
		}
	}
	return result
}

func (self _FlattenPluginImpl) Name() string {
	return "foreach"
}

func (self _FlattenPluginImpl) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "flatten",
		Doc: "Flatten the columns in query. If any column repeats " +
			"then we repeat the entire row once for each item.",

		ArgType: type_map.AddType(scope, &_FlattenPluginImpl{}),

		// Our type is not known - it depends on the
		// delegate's type.
		RowType: "",
	}
}
