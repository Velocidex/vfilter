package vfilter

import (
	"context"
)

type _ForeachPluginImplArgs struct {
	Row   StoredQuery `vfilter:"required,field=row"`
	Query StoredQuery `vfilter:"required,field=query"`
}

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
			child_ctx, cancel := context.WithCancel(ctx)
			query_chan := stored_query.Eval(child_ctx, child_scope)
			for {
				query_chan_item, ok := <-query_chan
				if !ok {
					break
				}
				output_chan <- query_chan_item
			}
			// Cancel the context when the child query is
			// done. This will force any cleanup functions
			// used by the child query to be run now
			// instead of waiting for our parent query to
			// complete.
			cancel()
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

		ArgType: type_map.AddType(&_ForeachPluginImplArgs{}),

		// Our type is not known - it depends on the
		// delegate's type.
		RowType: "",
	}
}
