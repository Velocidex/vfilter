package vfilter

import (
	"context"
)

type _ForeachPluginImpl struct{}

func (self _ForeachPluginImpl) Call(ctx context.Context,
	scope *Scope,
	args *Dict) <-chan Row {
	output_chan := make(chan Row)

	row_stored_query, ok := ExtractStoredQuery(scope, "row", args)
	if !ok {
		scope.Log("Expecting 'row' parameter to be a " +
			"stored query (Use LET).")
		close(output_chan)
		return output_chan
	}

	stored_query, ok := ExtractStoredQuery(scope, "query", args)
	if !ok {
		scope.Log("Expecting 'query' parameter to be a stored query (" +
			"Try using LET)")
		close(output_chan)
		return output_chan
	}

	go func() {
		defer close(output_chan)

		row_chan := row_stored_query.Eval(ctx, scope)
		for {
			row_item, ok := <-row_chan
			if !ok {
				break
			}
			// Evaluate the query on a new sub scope. The
			// query can refer to rows returned by the
			// "row" query.
			child_scope := scope.Copy()
			child_scope.AppendVars(row_item)
			query_chan := stored_query.Eval(ctx, child_scope)
			for {
				query_chan_item, ok := <-query_chan
				if !ok {
					break
				}
				output_chan <- query_chan_item
			}
		}
	}()

	return output_chan
}

func (self _ForeachPluginImpl) Name() string {
	return "foreach"
}

func (self _ForeachPluginImpl) Info(type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "foreach",
		Doc:  "Executes 'query' once for each row in the 'row' query.",

		// Our type is not known - it depends on the
		// delegate's type.
		RowType: "",
	}
}
