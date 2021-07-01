// Immplements group by operation

package grouper

import (
	"context"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

type DefaultGrouper struct{}

func (self *DefaultGrouper) Group(
	ctx context.Context, scope types.Scope, actor types.GroupbyActor) <-chan types.Row {
	output_chan := make(chan types.Row)

	go func() {
		defer close(output_chan)

		// Aggregate functions (count, sum etc)
		// operate by storing data in the scope
		// context between rows. When we group by we
		// create a different scope context for each
		// bin - all the rows with the same group by
		// value are placed in the same bin and share
		// the same context.
		type AggregateContext struct {
			row     *ordereddict.Dict
			context *ordereddict.Dict
		}

		// Collect all the rows with the same group_by
		// member. This is a map between unique group
		// by values and an aggregate context.
		bins := ordereddict.NewDict() //(map[string]*AggregateContext)

		// Append this row to a bin based on a unique
		// value of the group by column.
		for {
			row, bin_idx, new_scope, err := actor.GetNextRow(ctx, scope)
			if err != nil {
				break
			}

			var aggregate_ctx *AggregateContext

			// Try to find the context in the map
			aggregate_ctx_any, pres := bins.Get(bin_idx)
			// No previous aggregate_row - initialize with a new context.
			if !pres {
				aggregate_ctx = &AggregateContext{
					context: ordereddict.NewDict(),
				}
				bins.Set(bin_idx, aggregate_ctx)

			} else {
				aggregate_ctx = aggregate_ctx_any.(*AggregateContext)
			}

			// The transform function receives its
			// own unique context for the specific
			// aggregate group.
			new_scope.SetContextDict(aggregate_ctx.context)

			// Update the row with the transformed
			// columns. Note we must materialize these
			// rows because evaluating the row may have
			// side effects (e.g. for aggregate
			// functions).
			new_row := actor.MaterializeRow(ctx, row, new_scope)

			aggregate_ctx.row = new_row
		}

		// Emit the binned set as a new result set.
		for _, key := range bins.Keys() {
			aggregate_ctx_any, _ := bins.Get(key)
			aggregate_ctx, ok := aggregate_ctx_any.(*AggregateContext)
			if ok {
				output_chan <- aggregate_ctx.row
			}
		}
	}()

	return output_chan

}
