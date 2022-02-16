package plugins

import (
	"context"
	"sync"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/types"
)

type _ForeachPluginImplArgs struct {
	Row     types.LazyExpr    `vfilter:"required,field=row,doc=A query or slice which generates rows."`
	Query   types.StoredQuery `vfilter:"optional,field=query,doc=Run this query for each row."`
	Async   bool              `vfilter:"optional,field=async,doc=If set we run all queries asynchronously (implies workers=1000)."`
	Workers int64             `vfilter:"optional,field=workers,doc=Total number of asynchronous workers."`
	Column  string            `vfilter:"optional,field=column,doc=If set we only extract the column from row."`
}

type _ForeachPluginImpl struct{}

func (self _ForeachPluginImpl) Call(ctx context.Context,
	scope types.Scope,
	args *ordereddict.Dict) <-chan types.Row {
	output_chan := make(chan types.Row)

	go func() {
		defer close(output_chan)

		arg := _ForeachPluginImplArgs{}
		err := arg_parser.ExtractArgs(scope, args, &arg)
		if err != nil {
			scope.Log("foreach: %v", err)
			return
		}

		if arg.Async && arg.Workers == 0 {
			arg.Workers = 100
		}

		// At least one worker
		if arg.Workers == 0 {
			arg.Workers = 1
		}

		// Create a worker pool to run the subquery in.
		if arg.Workers > 1 {
			scope.Log("Creating %v workers for foreach plugin\n", arg.Workers)
		}
		pool := newWorkerPool(ctx, arg.Query, output_chan, int(arg.Workers))
		defer pool.Close()

		row_chan := scope.Iterate(ctx, arg.Row)

		for {
			select {
			case <-ctx.Done():
				return

			case row_item, ok := <-row_chan:
				if !ok {
					return
				}

				// This allows callers to deconstruct
				// a SELECT with dicts as columns into
				// entire rows.
				if arg.Column != "" {
					value, pres := scope.Associative(row_item, arg.Column)
					if pres {
						row_item = value
					} else {
						row_item = types.Null{}
					}
				}

				if arg.Query == nil {
					select {
					case <-ctx.Done():
						return
					case output_chan <- row_item:
					}
					continue
				}

				// Evaluate the query on a new sub
				// scope. The query can refer to rows
				// returned by the "row" query.
				child_scope := scope.Copy()
				// child_scope is closed in the pool worker.

				child_scope.AppendVars(row_item)
				pool.RunScope(child_scope)
			}
		}
	}()

	return output_chan
}

func (self _ForeachPluginImpl) Name() string {
	return "foreach"
}

func (self _ForeachPluginImpl) Info(scope types.Scope, type_map *types.TypeMap) *types.PluginInfo {
	return &types.PluginInfo{
		Name: "foreach",
		Doc:  "Executes 'query' once for each row in the 'row' query.",

		ArgType: type_map.AddType(scope, &_ForeachPluginImplArgs{}),
	}
}

type workerPool struct {
	wg          sync.WaitGroup
	ch          chan types.Scope
	query       types.StoredQuery
	output_chan chan types.Row
}

func (self *workerPool) RunScope(scope types.Scope) {
	self.ch <- scope
}

func (self *workerPool) Close() {
	close(self.ch)
	self.wg.Wait()
}

func (self *workerPool) runQuery(ctx context.Context, scope types.Scope) {
	child_ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer scope.Close()

	query_chan := self.query.Eval(child_ctx, scope)
	for {
		select {
		case <-ctx.Done():
			return

		case query_chan_item, ok := <-query_chan:
			if !ok {
				return
			}
			select {
			case <-ctx.Done():
				return
			case self.output_chan <- query_chan_item:
			}
		}
	}
}

func (self *workerPool) worker(ctx context.Context) {
	defer self.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return

			// Take a scope from the channel and re-run
			// the query with the new scope. Prepare for
			// cancellations at any point.
		case scope, ok := <-self.ch:
			if !ok {
				return
			}
			self.runQuery(ctx, scope)
		}
	}
}

func newWorkerPool(ctx context.Context, query types.StoredQuery,
	output_chan chan types.Row, size int) *workerPool {
	self := &workerPool{
		ch:          make(chan types.Scope),
		query:       query,
		output_chan: output_chan,
	}

	for i := 0; i < size; i++ {
		self.wg.Add(1)
		go self.worker(ctx)
	}

	return self
}
