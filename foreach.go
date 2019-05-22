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

	go func() {
		defer close(output_chan)

		arg := _ForeachPluginImplArgs{}
		err := ExtractArgs(scope, args, &arg)
		if err != nil {
			scope.Log("foreach: %v", err)
			return
		}

		row_chan := arg.Row.Eval(ctx, scope)
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
			query_chan := arg.Query.Eval(child_ctx, child_scope)
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

func (self _ForeachPluginImpl) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "foreach",
		Doc:  "Executes 'query' once for each row in the 'row' query.",

		ArgType: type_map.AddType(scope, &_ForeachPluginImplArgs{}),

		// Our type is not known - it depends on the
		// delegate's type.
		RowType: "",
	}
}
