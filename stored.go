// A stored query encapsulates a VQL query which is yet to
// execute. Readers can request the query's channel and can read from
// it to drain its results.

// Stored queries implement the LET VQL directive. The LET keyword
// defines a stored query which is evaluated on demand. It looks just
// like a subselect but it is an efficient mechanism of passing the
// result of one query into another. Consider the following query:

// LET files = select * from glob(globs="/**") where Size < 100
// SELECT FullPath from files

// The LET keyword creates a stored query. This query does not
// immediately run until it is used as the subject of the second
// query. Most importantly, the second query does not need to wait for
// the stored query to completely produce its output. The first query
// can immediately feed rows to the second query for additional
// filtering. This leads to zero memory overhead as the rows do not
// need to be queued in memory.

package vfilter

import (
	"context"
)

// A plugin like object which takes no arguments but may be inserted
// into the scope to select from it.
type StoredQuery interface {
	Eval(ctx context.Context) <-chan Row
	Columns() *[]string
}

type _StoredQuery struct {
	// Capture the scope at the point of definition. We will use
	// this scope when we run the query.
	scope *Scope
	query *_Select
}

func NewStoredQuery(query *_Select, scope *Scope) *_StoredQuery {
	return &_StoredQuery{
		query: query,
		scope: scope.Copy(),
	}
}

func (self *_StoredQuery) Eval(ctx context.Context) <-chan Row {
	return self.query.Eval(ctx, self.scope)
}

func (self *_StoredQuery) Columns() *[]string {
	if self.query.SelectExpression.All {
		return self.query.From.Plugin.Columns(self.scope)
	}

	return self.query.SelectExpression.Columns(self.scope)
}
