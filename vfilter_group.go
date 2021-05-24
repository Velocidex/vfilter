package vfilter

import (
	"context"
	"fmt"
	"io"

	"github.com/Velocidex/ordereddict"
	scope_module "www.velocidex.com/golang/vfilter/scope"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

type GroupbyActor struct {
	delegate   *_Select
	row_source <-chan types.Row
	scope      types.Scope
}

// Pull the next row off the query possibly filtering it.
func (self *GroupbyActor) GetNextRow(ctx context.Context, scope types.Scope) (
	types.LazyRow, string, types.Scope, error) {
	for row := range self.row_source {
		transformed_row, closer := self.delegate.SelectExpression.Transform(
			ctx, self.scope, row)
		defer closer()

		// Create a new scope over
		// which we can evaluate the
		// filter clause.
		new_scope := self.scope.Copy()
		defer new_scope.Close()

		// Order matters - transformed
		// row (from column
		// specifiers) may mask
		// original row (from plugin).
		new_scope.AppendVars(row)
		new_scope.AppendVars(transformed_row)

		if self.delegate.Where != nil {
			expression := self.delegate.Where.Reduce(ctx, new_scope)
			// If the filtered expression returns
			// a bool false, then skip the row.
			if expression == nil || !scope.Bool(expression) {
				scope.Trace("During Groupby: Row rejected")
				continue
			}
		}

		gb_element := self.delegate.GroupBy.Reduce(ctx, new_scope)

		// Emit a single row.
		return transformed_row, fmt.Sprintf("%v", gb_element), new_scope, nil
	}

	return nil, "", nil, io.EOF
}

func (self *GroupbyActor) MaterializeRow(ctx context.Context,
	row types.Row, scope types.Scope) *ordereddict.Dict {
	new_transformed_row, closer := self.delegate.SelectExpression.Transform(
		ctx, scope, row)
	defer closer()

	return MaterializedLazyRow(ctx, new_transformed_row, scope)
}

func (self *_Select) EvalGroupBy(ctx context.Context, scope types.Scope) <-chan Row {
	// Build an actor to send to the grouper.
	actor := &GroupbyActor{self, self.From.Eval(ctx, scope), scope}

	// Get a grouper implementation
	grouper_output_chan := GetIntScope(scope).Group(ctx, scope, actor)

	// Do we need to sort it as well?
	if self.OrderBy == nil {
		return grouper_output_chan
	}

	desc := false
	if self.OrderByDesc != nil {
		desc = *self.OrderByDesc
	}

	// Sort the output groups
	sorter_input_chan := make(chan Row)
	sorted_chan := scope.(*scope_module.Scope).Sort(
		ctx, scope, sorter_input_chan,
		utils.Unquote_ident(*self.OrderBy), desc)

	// Feed all the aggregate rows into the sorter.
	go func() {
		defer close(sorter_input_chan)

		// Re-run the same query with no order by clause then
		// we sort the results.
		self_copy := *self
		self_copy.OrderBy = nil

		for row := range grouper_output_chan {
			sorter_input_chan <- row
		}
	}()

	return sorted_chan
}

func (self *_Select) EvalGroupByXXXX(ctx context.Context, scope types.Scope) <-chan Row {
	output_chan := make(chan Row)
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
		bins := make(map[Any]*AggregateContext)

		sub_ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		// Append this row to a bin based on a unique
		// value of the group by column.
		for row := range self.From.Eval(sub_ctx, scope) {
			func(row types.Row) {
				transformed_row, closer := self.SelectExpression.Transform(
					ctx, scope, row)
				defer closer()

				// Create a new scope over
				// which we can evaluate the
				// filter clause.
				new_scope := scope.Copy()
				defer new_scope.Close()

				// Order matters - transformed
				// row (from column
				// specifiers) may mask
				// original row (from plugin).
				new_scope.AppendVars(row)
				new_scope.AppendVars(transformed_row)

				if self.Where != nil {
					expression := self.Where.Reduce(ctx, new_scope)
					// If the filtered expression returns
					// a bool false, then skip the row.
					if expression == nil || !scope.Bool(expression) {
						scope.Trace("During Groupby: Row rejected")
						return
					}
				}

				// Evaluate the group by expression on the transformed row.
				gb_element := self.GroupBy.Reduce(ctx, new_scope)

				// Stringify the result to index into the bins.
				bin_idx := fmt.Sprintf("%v", gb_element)

				aggregate_ctx, pres := bins[bin_idx]
				// No previous aggregate_row - initialize with a new context.
				if !pres {
					aggregate_ctx = &AggregateContext{
						context: ordereddict.NewDict(),
					}
					bins[bin_idx] = aggregate_ctx
				}

				// The transform function receives
				// its own unique context for the
				// specific aggregate group.
				GetIntScope(new_scope).SetContextDict(aggregate_ctx.context)

				// Update the row with the transformed
				// columns. Note we must materialize
				// these rows because evaluating the
				// row may have side effects (e.g. for
				// aggregate functions). NOTE:
				// Transform does not evaluate the row
				// - it simply wraps it in lazy
				// evaluators - only Materialize below
				// will evaluate expressions if
				// needed.
				new_transformed_row, closer := self.SelectExpression.Transform(
					ctx, new_scope, row)
				defer closer()

				new_row := MaterializedLazyRow(ctx, new_transformed_row, scope)
				new_row.Set("$groupby", bin_idx)

				aggregate_ctx.row = new_row
			}(row)
		}

		// Now sort the output according to the
		// aggregate rows. NOTE: We always sort to
		// maintain a stable output (since bins are a
		// map)
		desc := false
		if self.OrderByDesc != nil {
			desc = *self.OrderByDesc
		}

		// By default order by the group by column
		// unless the query specified a different
		// order by.
		order_by := "$groupby"
		if self.OrderBy != nil {
			order_by = *self.OrderBy
		}

		// Sort the output groups
		sorter_input_chan := make(chan Row)
		sorted_chan := scope.(*scope_module.Scope).Sort(
			ctx, scope, sorter_input_chan, order_by, desc)

		// Feed all the aggregate rows into the sorter.
		go func() {
			defer close(sorter_input_chan)

			// Emit the binned set as a new result set.
			for _, aggregate_ctx := range bins {
				sorter_input_chan <- aggregate_ctx.row
			}

		}()

		// Remove the group by column prior to sending
		// the row.
		idx := 0
		for row := range sorted_chan {
			if self.Limit != nil && idx >= int(*self.Limit) {
				break
			}
			idx++

			emitted_row := MaterializedLazyRow(ctx, row, scope)
			emitted_row.Delete("$groupby")
			select {
			case <-ctx.Done():
				return
			case output_chan <- emitted_row:
			}

		}
	}()

	return output_chan

}
