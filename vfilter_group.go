package vfilter

import (
	"context"
	"io"

	"github.com/Velocidex/ordereddict"
	scope_module "www.velocidex.com/golang/vfilter/scope"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

type GroupbyActor struct {
	delegate   *_Select
	row_source <-chan types.Row
}

func (self *GroupbyActor) Transform(ctx context.Context,
	scope types.Scope, row types.Row) (types.LazyRow, func()) {
	return self.delegate.SelectExpression.Transform(ctx, scope, row)
}

// Pull the next row off the query possibly filtering it. This
// function adds the row to a new child scope, which the caller must
// close.
func (self *GroupbyActor) GetNextRow(ctx context.Context, scope types.Scope) (
	types.LazyRow, types.Row, string, types.Scope, error) {

	for row := range self.row_source {
		// Create a new scope to carry the new vars. This ensures that
		// when the scope is closed, the vars can be removed for the next
		// row.
		new_scope := scope.Copy()

		// The transform captures the scope inside the LazyRow so when
		// it gets evaluated it can see previous values.
		transformed_row, closer := self.delegate.SelectExpression.Transform(
			ctx, new_scope, row)

		// Now we mask the original columns with the LazyRow
		// implementation. The Where clause below will access the
		// transformed row, which will materialize based on the new
		// scope.

		// Order matters - transformed row (from column specifiers)
		// may mask original row (from plugin).
		new_scope.AppendVars(row)
		new_scope.AppendVars(transformed_row)

		if self.delegate.Where != nil {
			expression := self.delegate.Where.Reduce(ctx, new_scope)

			// If the filtered expression returns a bool false, then
			// skip the row.
			if expression == nil || !scope.Bool(expression) {
				new_scope.Trace("During Groupby: Row rejected")

				// Prepare the next row
				new_scope.Close()
				closer()
				continue
			}
		}

		// Materialize the group by value as much as possible - we
		// dont want a lazy item here.
		gb_element := types.ToString(ctx, new_scope,
			self.delegate.GroupBy.Reduce(ctx, new_scope))

		closer()

		// Emit a single row.
		return transformed_row, row, gb_element, new_scope, nil
	}

	return nil, nil, "", nil, io.EOF
}

func (self *GroupbyActor) MaterializeRow(ctx context.Context,
	row types.Row, scope types.Scope) *ordereddict.Dict {
	return MaterializedLazyRow(ctx, row, scope)
}

func (self *_Select) EvalGroupBy(ctx context.Context, scope types.Scope) <-chan Row {
	// Build an actor to send to the grouper.
	actor := &GroupbyActor{self, self.From.Eval(ctx, scope)}

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
