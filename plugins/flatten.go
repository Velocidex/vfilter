package plugins

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

type _FlattenPluginImplArgs struct {
	Query types.StoredQuery `vfilter:"required,field=query"`
}

type _FlattenPluginImpl struct{}

func (self _FlattenPluginImpl) Call(ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) <-chan types.Row {
	output_chan := make(chan types.Row)

	go func() {
		defer close(output_chan)

		arg := _FlattenPluginImplArgs{}
		err := arg_parser.ExtractArgsWithContext(ctx, scope, args, &arg)
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

			row_dict := makeDict(scope, row_item)
			members := row_dict.Keys()

			flattened := flatten(ctx, scope, row_dict, len(members)-1)
			for _, item := range flattened {
				select {
				case <-ctx.Done():
					return
				case output_chan <- item:
				}
			}
		}
	}()

	return output_chan
}

func makeDict(scope types.Scope, item types.Any) *ordereddict.Dict {
	result_dict, ok := item.(*ordereddict.Dict)
	if ok {
		return result_dict
	}

	result := ordereddict.NewDict()
	for _, member := range scope.GetMembers(item) {
		value, pres := scope.Associative(item, member)
		if pres {
			result.Set(member, value)
		}
	}
	return result
}

// Expands the idx'th key into a list of rows
func flatten(ctx context.Context,
	scope types.Scope, item *ordereddict.Dict, idx int) []*ordereddict.Dict {
	if idx < 0 {
		return []*ordereddict.Dict{item}
	}

	result := []*ordereddict.Dict{}
	members := item.Keys()
	column := members[idx]
	cell, _ := item.Get(column)

	// Now iterate over all items in the cell.
	count := 0
	for member := range scope.Iterate(ctx, cell) {
		count++
		member_dict, ok := member.(*ordereddict.Dict)
		if ok {
			real_member, ok := member_dict.Get("_value")
			if ok {
				member = real_member
			}
		}

		// Prepare a copy of the row
		new_row := ordereddict.NewDict()
		new_row.MergeFrom(item)
		new_row.Update(column, member)

		// By induction
		result = append(result, flatten(ctx, scope, new_row, idx-1)...)
	}

	// Iterating over the member produced no results, just forward the
	// member directly.
	if count == 0 {
		result = append(result, flatten(ctx, scope, item, idx-1)...)
	}

	return result
}

func (self _FlattenPluginImpl) Name() string {
	return "foreach"
}

func (self _FlattenPluginImpl) Info(scope types.Scope, type_map *types.TypeMap) *types.PluginInfo {
	return &types.PluginInfo{
		Name: "flatten",
		Doc: "Flatten the columns in query. If any column repeats " +
			"then we repeat the entire row once for each item.",

		ArgType: type_map.AddType(scope, &_FlattenPluginImplArgs{}),
	}
}
