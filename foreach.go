package vfilter

import (
	"context"
	"sync"

	"github.com/Velocidex/ordereddict"
)

type _ForeachPluginImplArgs struct {
	Row     LazyExpr    `vfilter:"required,field=row,doc=A query or slice which generates rows."`
	Query   StoredQuery `vfilter:"optional,field=query,doc=Run this query for each row."`
	Async   bool        `vfilter:"optional,field=async,doc=If set we run all queries asyncronously (implies workers=1000)."`
	Workers int64       `vfilter:"optional,field=workers,doc=Total number of asyncronous workers."`
	Column  string      `vfilter:"optional,field=column,doc=If set we only extract the column from row."`
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

		if arg.Async && arg.Workers == 0 {
			arg.Workers = 1000
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
						row_item = Null{}
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

				// Evaluate the query on a new sub scope. The
				// query can refer to rows returned by the
				// "row" query.
				child_scope := scope.Copy()
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

func (self _ForeachPluginImpl) Info(scope *Scope, type_map *TypeMap) *PluginInfo {
	return &PluginInfo{
		Name: "foreach",
		Doc:  "Executes 'query' once for each row in the 'row' query.",

		ArgType: type_map.AddType(scope, &_ForeachPluginImplArgs{}),
	}
}

type workerPool struct {
	wg          sync.WaitGroup
	ch          chan *Scope
	query       StoredQuery
	output_chan chan Row
}

func (self *workerPool) RunScope(scope *Scope) {
	self.ch <- scope
}

func (self *workerPool) Close() {
	close(self.ch)
	self.wg.Wait()
}

func (self *workerPool) runQuery(ctx context.Context, scope *Scope) {
	child_ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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

func newWorkerPool(ctx context.Context, query StoredQuery,
	output_chan chan Row, size int) *workerPool {
	self := &workerPool{
		ch:          make(chan *Scope),
		query:       query,
		output_chan: output_chan,
	}

	for i := 0; i < size; i++ {
		self.wg.Add(1)
		go self.worker(ctx)
	}

	return self
}
