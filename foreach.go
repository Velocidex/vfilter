package vfilter

import (
	"context"
	"sync"

	"github.com/Velocidex/ordereddict"
)

type _ForeachPluginImplArgs struct {
	Row   LazyExpr    `vfilter:"required,field=row,doc=A query or slice which generates rows."`
	Query StoredQuery `vfilter:"required,field=query,doc=Run this query for each row."`
	Async bool        `vfilter:"optional,field=async,doc=If set we run all queries asyncronously."`
}

type _ForeachPluginImpl struct{}

func (self _ForeachPluginImpl) Call(ctx context.Context,
	scope *Scope,
	args *ordereddict.Dict) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		arg := _ForeachPluginImplArgs{}
		err := ExtractArgs(scope, args, &arg)
		if err != nil {
			scope.Log("foreach: %v", err)
			return
		}

		// If it is a stored query call it otherwise wrap the
		// object - this allows us to iterate on arrays.
		stored_query := arg.Row.ToStoredQuery(scope)

		wg := sync.WaitGroup{}
		for row_item := range stored_query.Eval(ctx, scope) {
			wg.Add(1)

			// Evaluate the query on a new sub scope. The
			// query can refer to rows returned by the
			// "row" query.
			child_scope := scope.Copy()
			child_scope.AppendVars(row_item)
			child_ctx, cancel := context.WithCancel(ctx)

			run_query := func() {
				defer wg.Done()

				// Cancel the context when the child query is
				// done. This will force any cleanup functions
				// used by the child query to be run now
				// instead of waiting for our parent query to
				// complete.
				defer cancel()

				query_chan := arg.Query.Eval(child_ctx, child_scope)
				for query_chan_item := range query_chan {
					output_chan <- query_chan_item
				}
			}

			// Maybe run it asyncronously.
			if arg.Async {
				go run_query()
			} else {
				run_query()
			}

			wg.Wait()
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
	}
}
