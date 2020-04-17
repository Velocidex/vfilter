/*

The veloci-filter (vfilter) library implements a generic SQL like
query language.

Overview::

There are many applications in which it is useful to provide a
flexible query language for the end user. Velocifilter has the
following design goals:

- It should be generic and easily adaptable to be used by any project.

- It should be fast and efficient.

An example makes the use case very clear. Suppose you are writing an
archiving application. Most archiving tools require a list of files to
be archived (e.g. on the command line).

You launch your tool and a user requests a new flag that allows them
to specify the files using a glob expression. For example, a user
might wish to only select the files ending with the ".go"
extension. While on a unix system one might use shell expansion to
support this, on other operating systems shell expansion may not work
(e.g. on windows).

You then add the ability to specify a glob expression directly to your
tool (suppose you add the flag --glob). A short while later, a user
requires filtering the files to archive by their size - suppose they
want to only archive a file smaller than a certain size. You
studiously add another set of flags (e.g. --size with a special syntax
for greater than or less than semantics).

Now a user wishes to be able to combine these conditions logically
(e.g. all files with ".go" extension newer than 5 days and smaller
than 5kb).

Clearly this approach is limited, if we wanted to support every
possible use case, our tool would add many flags with a complex syntax
making it harder for our users. One approach is to simply rely on the
unix "find" tool (with its many obscure flags) to support the file
selection problem. This is not ideal either since the find tool may
not be present on the system (E.g. on Windows) or may have varying
syntax. It may also not support every possible condition the user may
have in mind (e.g. files containing a RegExp or files not present in
the archive).

There has to be a better way. You wish to provide your users with a
powerful and flexible way to specify which files to archive, but we do
not want to write complicated logic and make our tool more complex to
use.

This is where velocifilter comes in. By using the library we can
provide a single flag where the user may specify a flexible VQL query
(Velocidex Query Language - a simplified SQL dialect) allowing the
user to specify arbirarily complex filter expressions. For example:

SELECT file from glob(pattern=["*.go", "*.py"]) where file.Size < 5000
and file.Mtime < now() - "5 days"

Not only does VQL allow for complex logical operators, but it is also
efficient and optimized automatically. For example, consider the
following query:

SELECT file from glob(pattern="*") where grep(file=file,
pattern="foobar") and file.Size < 5k

The grep() function will open the file and search it for the
pattern. If the file is large, this might take a long time. However
velocifilter will automatically abort the grep() function if the file
size is larger than 5k bytes. Velocifilter correctly handles such
cancellations automatically in order to reduce query evaluation
latency.

Protocols - supporting custom types::

Velocifilter uses a plugin system to allow clients to define how
their own custom types behave within the VQL evaluator.

Note that this is necessary because Go does not allow an external
package to add an interface to an existing type without creating a new
type which embeds it. Clients who need to handle the original third
party types must have a way to attach new protocols to existing types
defined outside their own codebase. Velocifilter achieves this by
implementing a registration systen in the Scope{} object.

For example, consider a client of the library wishing to pass custom
types in queries:

  type Foo struct {
     ...
     bar Bar
  }

Where both Foo and Bar are defined and produced by some other library
which our client uses. Suppose our client wishes to allow addition of
Foo objects. We would therefore need to implement the AddProtocol
interface on Foo structs. Since Foo structs are defined externally we
can not simply add a new method to Foo struct (we could embed Foo
struct in a new struct, but then we would also need to wrap the bar
field to produce an extended Bar. This is typically impractical and
not maintainable for heavily nested complex structs). We define a
FooAdder{} object which implements the Addition protocol on behalf of
the Foo object.

  // This is an object which implements addition between two Foo objects.
  type FooAdder struct{}

  // This method will be run to see if this implementation is
  // applicable. We only want to run when we add two Foo objects together.
  func (self FooAdder) Applicable(a Any, b Any) bool {
	_, a_ok := a.(Foo)
	_, b_ok := b.(Foo)
	return a_ok && b_ok
  }

  // Actually implement the addition between two Foo objects.
  func (self FooAdder) Add(scope *Scope, a Any, b Any) Any {
    ... return new object (does not have to be Foo{}).
  }

Now clients can add this protocol to the scope before evaluating a
query:

scope := NewScope().AddProtocolImpl(FooAdder{})


*/
package vfilter

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/Velocidex/ordereddict"
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	errors "github.com/pkg/errors"
)

var (
	vqlLexer = lexer.Must(lexer.Regexp(
		`(?ms)` +
			`(\s+)` +
			`|(?P<MLineComment>^/[*].*?[*]/$)` + // C Style comment.
			`|(?P<VQLComment>^--.*?$)` + // SQL style one line comment.
			`|(?P<Comment>^//.*?$)` + // C++ style one line comment.
			`|(?ims)(?P<SELECT>\bSELECT\b)` +
			`|(?ims)(?P<WHERE>\bWHERE\b)` +
			`|(?ims)(?P<AND>\bAND\b)` +
			`|(?ims)(?P<OR>\bOR\b)` +
			`|(?ims)(?P<FROM>\bFROM\b)` +
			`|(?ims)(?P<NOT>\bNOT\b)` +
			`|(?ims)(?P<AS>\bAS\b)` +
			`|(?ims)(?P<IN>\bIN\b)` +
			`|(?ims)(?P<LIMIT>\bLIMIT\b)` +
			`|(?ims)(?P<NULL>\bNULL\b)` +
			`|(?ims)(?P<DESC>\bDESC\b)` +
			`|(?ims)(?P<GROUPBY>\bGROUP\s+BY\b)` +
			`|(?ims)(?P<ORDERBY>\bORDER\s+BY\b)` +
			`|(?ims)(?P<BOOL>\bTRUE\b|\bFALSE\b)` +
			`|(?ims)(?P<LET>\bLET\b)` +
			`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)` +
			`|(?P<String>'([^'\\]*(\\.[^'\\]*)*)'|"([^"\\]*(\\.[^"\\]*)*)")` +
			`|(?P<Number>[-+]?(0x)?\d*\.?\d+([eE][-+]?\d+)?)` +
			`|(?P<Operators><>|!=|<=|>=|=~|[-+*/%,.()=<>{}\[\]])`,
	))

	vqlParser = participle.MustBuild(
		&VQL{},
		participle.Lexer(vqlLexer),
		participle.Upper("IN", "DESC"),
		participle.Elide("Comment", "MLineComment", "VQLComment"),
	// Need to solve left recursion detection first, if possible.
	// participle.UseLookahead(),
	)

	multiVQLParser = participle.MustBuild(
		&MultiVQL{},
		participle.Lexer(vqlLexer),
		participle.Upper("IN", "DESC"),
		participle.Elide("Comment", "MLineComment", "VQLComment"),
	)
)

// Parse the VQL expression. Returns a VQL object which may be
// evaluated.
func Parse(expression string) (*VQL, error) {
	vql := &VQL{}
	err := vqlParser.ParseString(expression, vql)
	switch t := err.(type) {
	case *lexer.Error:
		end := t.Pos.Offset + 10
		if end >= len(expression) {
			end = len(expression) - 1
		}
		if end < 0 {
			end = 0
		}

		start := t.Pos.Offset - 10
		if start < 0 {
			start = 0
		}

		pos := t.Pos.Offset
		if pos >= len(expression) {
			pos = len(expression) - 1
		}

		if pos < 0 {
			pos = 0
		}

		return vql, errors.Wrap(
			err,
			expression[start:pos]+"|"+expression[pos:end])
	default:

		return vql, err
	}
}

// Parse a string into multiple VQL statements.
func MultiParse(expression string) ([]*VQL, error) {
	vql := &MultiVQL{}
	err := multiVQLParser.ParseString(expression, vql)
	switch t := err.(type) {
	case *lexer.Error:
		end := t.Pos.Offset + 10
		if end >= len(expression) {
			end = len(expression) - 1
		}
		if end < 0 {
			end = 0
		}

		start := t.Pos.Offset - 10
		if start < 0 {
			start = 0
		}

		pos := t.Pos.Offset
		if pos >= len(expression) {
			pos = len(expression) - 1
		}

		if pos < 0 {
			pos = 0
		}

		return nil, errors.Wrap(
			err,
			expression[start:pos]+"|"+expression[pos:end])
	default:
		return vql.GetStatements(), err
	}
}

type MultiVQL struct {
	VQL1 *VQL      ` @@ `
	VQL2 *MultiVQL ` { @@ } `
}

func (self *MultiVQL) GetStatements() []*VQL {
	result := []*VQL{self.VQL1}
	if self.VQL2 != nil {
		return append(result, self.VQL2.GetStatements()...)
	}
	return result
}

// An opaque object representing the VQL expression.
type VQL struct {
	Let         string   `{ LET  @Ident `
	LetOperator string   ` ( @"=" | @"<=" ) }`
	Query       *_Select ` @@ `
}

// Returns the type of statement it is:
// LAZY_LET - A lazy stored query
// MATERIALIZED_LET - A stored meterialized query.
// SELECT - A query
func (self *VQL) Type() string {
	if self.LetOperator == "=" {
		return "LAZY_LET"
	} else if self.LetOperator == "<=" {
		return "MATERIALIZED_LET"
	} else if self.Query != nil {
		return "SELECT"
	}
	return ""
}

// Evaluate the expression. Returns a channel which emits a series of
// rows.
func (self VQL) Eval(ctx context.Context, scope *Scope) <-chan Row {
	// If this is a Let expression we need to create a stored
	// query and assign to the scope.
	if len(self.Let) > 0 {
		output_chan := make(chan Row)

		// Check if we are about to trash a scope
		// variable. The _ variable is special - it can be
		// trashed without a warning.
		if self.Let != "_" {
			_, pres := scope.Resolve(self.Let)
			if pres {
				scope.Log("WARNING: LET query overrides a variable for %s",
					self.Let)
			}
		}

		switch self.LetOperator {
		case "=":
			stored_query := NewStoredQuery(self.Query)
			scope.AppendVars(ordereddict.NewDict().Set(self.Let, stored_query))

		case "<=":
			scope.AppendVars(ordereddict.NewDict().Set(
				self.Let, Materialize(ctx, scope, self.Query)))
		}

		close(output_chan)
		return output_chan

	} else {
		return self.Query.Eval(ctx, scope)
	}
}

// Encodes the query into a string again.
func (self VQL) ToString(scope *Scope) string {
	result := ""
	if len(self.Let) > 0 {
		operator := " = "
		if self.LetOperator != "" {
			operator = self.LetOperator
		}

		result += "LET " + self.Let + operator
	}

	return result + self.Query.ToString(scope)
}

// Provides a list of column names from this query. These columns will
// serve as Row keys for rows that are published on the output channel
// by Eval().
func (self *VQL) Columns(scope *Scope) *[]string {
	return self.Query.Columns(scope)
}

type _Select struct {
	SelectExpression *_SelectExpression `SELECT @@`
	From             *_From             `FROM @@`
	Where            *_CommaExpression  `[ WHERE @@ ]`
	GroupBy          *string            `[ GROUPBY @Ident ]`
	OrderBy          *string            `[ ORDERBY @Ident `
	OrderByDesc      *bool              ` [ @DESC ] ]`
	Limit            *int64             `[ LIMIT @Number ]`
}

// Provides a list of column names from this query. These columns will
// serve as Row keys for rows that are published on the output channel
// by Eval().
func (self *_Select) Columns(scope *Scope) *[]string {
	if self.SelectExpression.All {
		return self.From.Plugin.Columns(scope)
	}

	return self.SelectExpression.Columns(scope)
}

func (self *_Select) ToString(scope *Scope) string {
	result := "SELECT "
	if self.SelectExpression != nil {
		result += self.SelectExpression.ToString(scope)
	}

	if self.From != nil {
		result += " FROM "
		result += self.From.ToString(scope)

	}

	if self.Where != nil {
		result += " WHERE " + self.Where.ToString(scope)
	}

	if self.GroupBy != nil {
		result += " GROUP BY " + *self.GroupBy
	}

	if self.OrderBy != nil {
		result += " ORDER BY " + *self.OrderBy

		if self.OrderByDesc != nil && *self.OrderByDesc {
			result += " DESC "
		}
	}

	if self.Limit != nil {
		result += fmt.Sprintf(
			" LIMIT %d ", int(*self.Limit))
	}

	return result
}

func (self *_Select) Eval(ctx context.Context, scope *Scope) <-chan Row {
	output_chan := make(chan Row)

	if self.GroupBy != nil {
		go func() {
			defer close(output_chan)

			group_by := *self.GroupBy

			// Aggregate functions (count, sum etc)
			// operate by storing data in the scope
			// context between rows. When we group by we
			// create a different scope context for each
			// bin - all the rows with the same group by
			// value are placed in the same bin and share
			// the same context.
			type AggregateContext struct {
				row     Row
				context *ordereddict.Dict
			}

			// Collect all the rows with the same group_by
			// member. This is a map between unique group
			// by values and an aggregate context.
			bins := make(map[Any]*AggregateContext)

			sub_ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			new_scope := scope.Copy()

			// Append this row to a bin based on a unique
			// value of the group by column.
			for row := range self.From.Eval(sub_ctx, scope) {
				transformed_row := self.SelectExpression.Transform(
					ctx, scope, row)

				if self.Where != nil {
					new_scope := scope.Copy()

					// Order matters - transformed
					// row may mask original row.
					new_scope.AppendVars(row)
					new_scope.AppendVars(transformed_row)

					expression := self.Where.Reduce(ctx, new_scope)
					// If the filtered expression returns
					// a bool false, then skip the row.
					if expression == nil || !scope.Bool(expression) {
						scope.Trace("During Groupby: Row rejected")
						continue
					}
				}

				gb_element, pres := scope.Associative(
					transformed_row, group_by)
				if !pres {
					// This should not happen -
					// the group by column is not
					// present in the row.
					gb_element = Null{}
				}

				// We can not aggregate by arrays. Should we serialize them?
				if is_array(gb_element) {
					gb_element = Null{}
				}

				aggregate_ctx, pres := bins[gb_element]
				// No previous aggregate_row - initialize with a new context.
				if !pres {
					aggregate_ctx = &AggregateContext{
						context: ordereddict.NewDict(),
					}
					bins[gb_element] = aggregate_ctx
				}

				// The transform function receives
				// its own unique context for the
				// specific aggregate group.
				new_scope.context = aggregate_ctx.context

				// Update the row with the transformed
				// columns. Note we must materialize
				// these rows because evaluating the
				// row may have side effects (e.g. for
				// aggregate functions).
				aggregate_ctx.row = MaterializedLazyRow(
					self.SelectExpression.Transform(
						ctx, new_scope, row), scope)
			}

			result_set := &ResultSet{
				OrderBy: group_by,
				scope:   scope,
			}

			if self.OrderBy != nil {
				result_set.OrderBy = *self.OrderBy
			}

			if self.OrderByDesc != nil {
				result_set.Desc = *self.OrderByDesc
			}

			// Emit the binned set as a new result set.
			for _, aggregate_ctx := range bins {
				result_set.Items = append(result_set.Items, aggregate_ctx.row)
			}

			// Sort the results based on the OrderBy
			sort.Sort(result_set)

			for idx, row := range result_set.Items {
				if self.Limit != nil && idx >= int(*self.Limit) {
					break
				}
				output_chan <- MaterializedLazyRow(row, new_scope)
			}
		}()

		return output_chan
	}

	if self.Limit != nil {
		go func() {
			defer close(output_chan)

			limit := int(*self.Limit)
			count := 1

			self_copy := *self
			self_copy.Limit = nil

			// Cancel the query when we hit the limit.
			sub_ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			for row := range self_copy.Eval(sub_ctx, scope) {
				output_chan <- row
				count += 1
				if count > limit {
					return
				}
			}
		}()

		return output_chan
	}

	if self.OrderBy != nil {
		result_set := &ResultSet{
			OrderBy: *self.OrderBy,
			scope:   scope,
		}

		if self.OrderByDesc != nil {
			result_set.Desc = *self.OrderByDesc
		}

		// Re-run the same query with no order by clause then
		// we sort the results.
		self_copy := *self
		self_copy.OrderBy = nil

		for row := range self_copy.Eval(ctx, scope) {
			result_set.Items = append(result_set.Items, row)
		}

		// Sort the results based on the
		sort.Sort(result_set)

		go func() {
			defer close(output_chan)

			for _, row := range result_set.Items {
				output_chan <- row
			}
		}()
		return output_chan
	}

	// Gets a row from the FROM clause, then transforms it
	// according to the SelectExpression. After transformation,
	// apply the WHERE clause to the row to determine if it should
	// be relayed. NOTE: We need to transform the row first in
	// order to assign aliases.
	go func() {
		from_chan := self.From.Eval(ctx, scope)

		defer close(output_chan)
		for {
			select {
			// Are we cancelled?
			case <-ctx.Done():
				return

				// Get a row
			case row, ok := <-from_chan:
				if !ok {
					return
				}

				transformed_row := self.SelectExpression.Transform(
					ctx, scope, row)

				if self.Where == nil {
					output_chan <- MaterializedLazyRow(transformed_row, scope)
				} else {
					// If there is a filter clause, we
					// need to filter the row using a new
					// scope.
					new_scope := scope.Copy()

					// Filters can access both the
					// untransformed row and the
					// transformed row. This
					// allows WHERE clause to
					// refer to both the raw
					// plugin output as well as
					// aliases of transformations
					// on the row.
					new_scope.AppendVars(row)
					new_scope.AppendVars(transformed_row)

					expression := self.Where.Reduce(ctx, new_scope)
					// If the filtered expression returns
					// a bool true, then pass the row to
					// the output.
					if expression != nil && scope.Bool(expression) {
						output_chan <- MaterializedLazyRow(
							transformed_row, new_scope)
					} else {
						scope.Trace("Row rejected")
					}
				}
			}
		}
	}()

	return output_chan
}

type _From struct {
	Plugin _Plugin ` @@ `
}

type _Plugin struct {
	Name string   `@Ident { @"." @Ident } `
	Call bool     `[ @"("`
	Args []*_Args ` [ @@  { "," @@ } ] ")" ]`
}

type _Args struct {
	Left      string            `@Ident "=" `
	SubSelect *_Select          `( "{" @@ "}" | `
	Array     *_CommaExpression ` "[" @@ "]" | `
	Right     *_AndExpression   ` @@ )`
}

type _SelectExpression struct {
	All         bool                  ` [ @"*" ","? ] `
	Expressions []*_AliasedExpression ` [ @@ { "," @@ } ]`
}

type _AliasedExpression struct {
	SubSelect  *_Select        `( "{" @@ "}" |`
	Expression *_AndExpression ` @@ )`

	As string `[ AS @Ident ]`

	mu    sync.Mutex
	cache *string
}

func (self *_AliasedExpression) GetName(scope *Scope) string {
	if self.As != "" {
		return self.As
	}
	return self.ToString(scope)
}

func (self *_AliasedExpression) IsAggregate(scope *Scope) bool {
	if self.SubSelect != nil {
		return true
	}

	if self.Expression.IsAggregate(scope) {
		return true
	}

	return false
}

func (self *_AliasedExpression) Reduce(ctx context.Context, scope *Scope) Any {
	if self.Expression != nil {
		return self.Expression.Reduce(ctx, scope)
	}

	if self.SubSelect != nil {
		var rows []Row
		for item := range self.SubSelect.Eval(ctx, scope) {
			members := scope.GetMembers(item)
			if len(members) == 1 {
				item_column, pres := scope.Associative(item, members[0])
				if pres {
					rows = append(rows, item_column)
				}
			} else {
				rows = append(rows, item)
			}
		}

		// If the subselect returns only a single row
		// we just pass that item. This allows a
		// subselect in row spec to just substitute
		// one value instead of needlessly creating a
		// slice of one item.
		if len(rows) == 1 {
			return rows[0]
		} else {
			return rows
		}
	}

	return nil
}

func (self *_AliasedExpression) ToString(scope *Scope) string {
	self.mu.Lock()
	defer self.mu.Unlock()

	if self.cache != nil {
		return *self.cache
	}

	if self.Expression != nil {
		result := self.Expression.ToString(scope)
		if self.As != "" {
			result += " AS " + self.As
		}
		self.cache = &result
		return result

	} else if self.SubSelect != nil {
		result := self.SubSelect.ToString(scope)
		result = "{ " + result + " }"
		if self.As != "" {
			result += " AS " + self.As
		}
		self.cache = &result
		return result
	} else {
		return ""
	}
}

// Expressions separated by addition or subtraction.
type _AdditionExpression struct {
	Left  *_MultiplicationExpression `@@`
	Right []*_OpAddTerm              `{ @@ }`
}

type _OpAddTerm struct {
	Operator string                     `@("+" | "-")`
	Term     *_MultiplicationExpression `@@`
}

// Expressions separated by multiplication or division.
type _MultiplicationExpression struct {
	Left  *_MemberExpression `@@`
	Right []*_OpFactor       `{ @@ }`
}

type _OpFactor struct {
	Operator string  `@("*" | "/")`
	Factor   *_Value `@@`
}

// Expression for membership access (dot operator).
// e.g. x.y.z
type _MemberExpression struct {
	Left  *_Value              `@@`
	Right []*_OpMembershipTerm `[{ @@ }] `
}

type _OpMembershipTerm struct {
	Index *int64 `( "[" @Number "]" | `
	Term  string `  "." @Ident )`
}

// ---------------------------------------

// The Top level precedence expression. Precedence table:
// 1) , (Array)
// 2) AND
// 3) OR
// 4) * /
// 5) + -
// 6) . (dereference operator)

// Comma separated expressions create a list.
// e.g. 1, 2, 3 -> (1, 2, 3)
type _CommaExpression struct {
	Left  *_AndExpression `@@`
	Right []*_OpArrayTerm `{ @@ }`
}

type _OpArrayTerm struct {
	Operator string          `@","`
	Term     *_AndExpression `{ @@ }`
}

// Expressions separated by AND.
type _AndExpression struct {
	Left  *_OrExpression `(@@`
	Right []*_OpAndTerm  `{ @@ })`
}

type _OpAndTerm struct {
	Operator string         ` AND `
	Term     *_OrExpression `@@`
}

// Expressions separated by OR
type _OrExpression struct {
	Left  *_ConditionOperand `@@`
	Right []*_OpOrTerm       `{ @@ }`
}

type _OpOrTerm struct {
	Operator string             `OR `
	Term     *_ConditionOperand `@@`
}

// Conditional expressions imply comparison.
type _ConditionOperand struct {
	Not   *_ConditionOperand   `(NOT @@ | `
	Left  *_AdditionExpression `@@)`
	Right *_OpComparison       `{ @@ }`
}

type _OpComparison struct {
	Operator string               `@( "<>" | "<=" | ">=" | "=" | "<" | ">" | "!=" | IN | "=~")`
	Right    *_AdditionExpression `@@`
}

type _Term struct {
	Select        *_Select          `| @@`
	SymbolRef     *_SymbolRef       `| @@`
	Value         *_Value           `| @@`
	SubExpression *_CommaExpression `| "(" @@ ")"`
}

type _SymbolRef struct {
	Symbol     string   `@Ident`
	Parameters []*_Args `[ "(" [ @@ { "," @@ } ] ")" ] `

	mu       sync.Mutex
	function FunctionInterface
}

type _Value struct {
	Negated       bool              `[ "-" | "+" ]`
	SymbolRef     *_SymbolRef       `( @@ `
	Subexpression *_CommaExpression `| "(" @@ ")"`

	String *string ` | @String`

	// Figure out if this is an int or float.
	StrNumber *string ` | @Number`
	Float     *float64
	Int       *int64

	Boolean *string ` | @BOOL `
	Null    bool    ` | @NULL)`

	mu    sync.Mutex
	cache Any
}

// A Generic object which may be returned in a row from a plugin.
type Any interface{}

// Plugins may return anything as long as there is a valid
// Associative() protocol handler. VFilter will simply call
// scope.Associative(row, column) to retrieve the cell value for each
// column. Note that VFilter will use reflection to implement the
// DefaultAssociative{} protocol - this means that plugins may just
// return any struct with exported methods and fields and it will be
// supported automatically.
type Row interface{}

// Receives a row from the FROM clause and transforms it according to
// the select expression to produce a new row.
func (self *_SelectExpression) Transform(
	ctx context.Context, scope *Scope, row Row) Row {
	// The select uses a * to relay all the rows without
	// filtering

	// The select expression consists of multiple
	// columns, each may be an
	// expression. Expressions may also be
	// repeated. VQL produces unique column names
	// so each column must be a unique string.

	// If an AS keyword is used to name the
	// column, then we use that name, otherwise we
	// generate the name by converting the
	// expression to a string using its ToString()
	// method.
	new_row := NewLazyRow(ctx)
	new_scope := scope.Copy()
	new_scope.AppendVars(row)

	// If there is a * expression in addition to the
	// column expressions, this is equivalent to adding
	// all the columns as defined by the * as if they were
	// explicitely defined.
	if self.All {
		for _, member := range scope.GetMembers(row) {
			value, pres := scope.Associative(row, member)
			if pres {
				new_row.AddColumn(member,
					func(ctx context.Context, scope *Scope) Any {
						return value
					})
			}
		}
	}

	for _, expr_ := range self.Expressions {
		// A copy of the expression for the lambda capture.
		expr := expr_

		// Figure out the column name.
		var column_name string
		if expr.As != "" {
			column_name = expr.As
		} else {
			column_name = expr.ToString(scope)
		}

		new_row.AddColumn(
			column_name,

			// Use the new scope rather than the
			// callers scope since the lazy row
			// may be accessed in any scope but
			// needs to resolve members in the
			// scope it was created from.
			func(ctx context.Context, scope *Scope) Any {
				return expr.Reduce(ctx, new_scope)
			})
	}

	return new_row
}

func (self *_SelectExpression) Columns(scope *Scope) *[]string {
	var result []string

	for _, expr := range self.Expressions {
		if expr.As != "" {
			result = append(result, expr.As)
		} else {
			result = append(result, expr.ToString(scope))
		}
	}
	return &result
}

func (self *_SelectExpression) ToString(scope *Scope) string {
	var substrings []string
	if self.All {
		substrings = append(substrings, "*")
	}
	for _, item := range self.Expressions {
		substrings = append(substrings, item.ToString(scope))
	}

	return strings.Join(substrings, ", ")
}

// The From expression runs the Plugin and then filters each row
// according to the Where clause.
func (self *_From) Eval(ctx context.Context, scope *Scope) <-chan Row {
	output_chan := make(chan Row)

	input_chan := self.Plugin.Eval(ctx, scope)
	go func() {
		defer close(output_chan)
		for {
			select {
			case <-ctx.Done():
				return

			case row, ok := <-input_chan:
				{
					if !ok {
						return
					}
					output_chan <- row
				}
			}
		}
	}()

	return output_chan
}

func (self *_From) ToString(scope *Scope) string {
	result := self.Plugin.ToString(scope)
	return result
}

func (self *_Plugin) getPlugin(scope *Scope, plugin_name string) (
	PluginGeneratorInterface, bool) {
	components := strings.Split(plugin_name, ".")
	// Single plugin reference.
	if len(components) == 1 {
		plugin, pres := scope.plugins[plugin_name]
		return plugin, pres
	}

	// Plugins with "." resolve themselves recursively.
	var result Any = scope
	for _, component := range components {
		subcomponent, pres := scope.Associative(result, component)
		if !pres {
			return nil, false
		}

		result = subcomponent
	}

	// It is a plugin
	plugin, ok := result.(PluginGeneratorInterface)
	if ok {
		return plugin, true
	}

	// Not a plugin - do not return it.
	return nil, false
}

func (self *_Plugin) Eval(ctx context.Context, scope *Scope) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		// The FROM clause refers to a var and not a
		// plugin. Just read the var from the scope.
		if !self.Call {
			if variable, pres := scope.Resolve(self.Name); pres {
				// If the variable is a stored query
				// we just copy from its channel to
				// the output.
				stored_query, ok := variable.(StoredQuery)
				if ok {
					from_chan := stored_query.Eval(ctx, scope)
					for row := range from_chan {
						output_chan <- row
					}

				} else if is_array(variable) {
					var_slice := reflect.ValueOf(variable)
					for i := 0; i < var_slice.Len(); i++ {
						output_chan <- var_slice.Index(i).Interface()
					}
				} else {
					output_chan <- variable
				}
			} else {
				scope.Log("SELECTing from %v failed! No such var in scope",
					self.Name)
			}
			return
		}

		// Build up the args to pass to the function. The
		// plugin implementation can extract these using the
		// ExtractArgs() helper.
		args := ordereddict.NewDict()
		for _, arg := range self.Args {
			if arg.Right != nil {
				args.Set(arg.Left, LazyExpr{
					Expr:  arg.Right,
					ctx:   ctx,
					scope: scope})

			} else if arg.Array != nil {
				value := arg.Array.Reduce(ctx, scope)
				if value == nil {
					output_chan <- Null{}
					return
				}
				args.Set(arg.Left, value)

			} else if arg.SubSelect != nil {
				args.Set(arg.Left, arg.SubSelect)
			}
		}

		if plugin, pres := self.getPlugin(scope, self.Name); pres {
			for row := range plugin.Call(ctx, scope, args) {
				output_chan <- row
			}
		} else {
			options := getSimilarPlugins(scope, self.Name)
			message := fmt.Sprintf("Plugin %v not found. ", self.Name)
			if len(options) > 0 {
				message += fmt.Sprintf(
					"Did you mean %v? ",
					strings.Join(options, " "))
			}

			_, pres := scope.functions[self.Name]
			if pres {
				message += fmt.Sprintf(
					"There is a VQL function called \"%v\" "+
						"- did you mean to call this "+
						"function instead?", self.Name)
			}

			scope.Log("%v", message)
		}
	}()

	return output_chan
}

func (self *_Plugin) Columns(scope *Scope) *[]string {
	var result []string

	// If the plugin is a callable then get the scope to list its columns.
	if self.Call {
		type_map := NewTypeMap()
		if plugin_info, pres := scope.Info(type_map, self.Name); pres {
			type_ref, pres := type_map.Get(scope, plugin_info.RowType)
			if pres {
				for _, k := range type_ref.Fields.Keys() {
					result = append(result, k)
				}
			}
		}

		// If it is a variable then get its columns through
		// the GetMembers protocol.
	} else {
		value, pres := scope.Resolve(self.Name)
		if pres {
			// If it is a stored query we just delegate
			// the Columns() method to it.
			stored_query, ok := value.(StoredQuery)
			if ok {
				return stored_query.Columns(scope)
			}

			for _, item := range scope.GetMembers(value) {
				result = append(result, item)
			}
		}
	}

	return &result
}

func (self *_Plugin) ToString(scope *Scope) string {
	result := self.Name
	if self.Call {
		var substrings []string
		for _, arg := range self.Args {
			substrings = append(substrings, arg.ToString(scope))
		}

		result += "(" + strings.Join(substrings, ", ") + ")"
	}

	return result
}

func (self *_Args) ToString(scope *Scope) string {
	if self.Right != nil {
		return self.Left + "=" + self.Right.ToString(scope)
	} else if self.SubSelect != nil {
		return self.Left + "= { " + self.SubSelect.ToString(scope) + "}"
	} else if self.Array != nil {
		return self.Left + "= [" + self.Array.ToString(scope) + "]"
	}
	return ""
}

func (self *_MemberExpression) IsAggregate(scope *Scope) bool {
	if self.Left != nil && self.Left.IsAggregate(scope) {
		return true
	}

	return false
}

func (self *_MemberExpression) Reduce(ctx context.Context, scope *Scope) Any {
	lhs := self.Left.Reduce(ctx, scope)
	for _, term := range self.Right {
		var pres bool

		// Slice index implementation via Associative protocol.
		if term.Index != nil {
			lhs, pres = scope.Associative(lhs, term.Index)
		} else {
			lhs, pres = scope.Associative(lhs, term.Term)
		}
		if !pres {
			return Null{}
		}
	}

	return lhs
}

func (self *_MemberExpression) ToString(scope *Scope) string {
	result := self.Left.ToString(scope)

	for _, right := range self.Right {
		if right.Index != nil {
			result += fmt.Sprintf("[%d]", *right.Index)
		} else {
			result += fmt.Sprintf(".%s", right.Term)
		}
	}

	return result
}

func (self *_CommaExpression) IsAggregate(scope *Scope) bool {
	if self.Left != nil && self.Left.IsAggregate(scope) {
		return true
	}

	for _, i := range self.Right {
		if i.Term != nil && i.Term.IsAggregate(scope) {
			return true
		}
	}

	return false
}

func (self _CommaExpression) Reduce(ctx context.Context, scope *Scope) Any {
	lhs := self.Left.Reduce(ctx, scope)
	if lhs == nil {
		return Null{}
	}

	// Where there is no comma we return the actual element and
	// not an array of length one.
	if self.Right == nil {
		return lhs
	}

	result := []Any{lhs}
	for _, term := range self.Right {
		if term.Term == nil {
			return result
		}
		result = append(result, term.Term.Reduce(ctx, scope))
	}

	return result
}

func (self _CommaExpression) ToString(scope *Scope) string {
	result := []string{self.Left.ToString(scope)}

	for _, right := range self.Right {
		if right.Term == nil {
			result = append(result, "")
		} else {
			result = append(result, right.Term.ToString(scope))
		}
	}
	return strings.Join(result, ", ")
}

func (self *_AndExpression) IsAggregate(scope *Scope) bool {
	if self.Left.IsAggregate(scope) {
		return true
	}

	for _, i := range self.Right {
		if i.Term != nil && i.Term.IsAggregate(scope) {
			return true
		}
	}

	return false
}

func (self _AndExpression) Reduce(ctx context.Context, scope *Scope) Any {
	result := self.Left.Reduce(ctx, scope)
	if self.Right == nil {
		return result
	}

	if scope.Bool(result) == false {
		return false
	}

	for _, term := range self.Right {
		if scope.Bool(term.Term.Reduce(ctx, scope)) == false {
			return false
		}
	}

	return true
}

func (self _AndExpression) ToString(scope *Scope) string {
	result := []string{self.Left.ToString(scope)}

	for _, right := range self.Right {
		result = append(result, right.Term.ToString(scope))
	}
	return strings.Join(result, " AND ")
}

func (self *_OrExpression) IsAggregate(scope *Scope) bool {
	if self.Left.IsAggregate(scope) {
		return true
	}
	for _, i := range self.Right {
		if i.Term != nil && i.Term.IsAggregate(scope) {
			return true
		}
	}

	return false
}

func (self _OrExpression) Reduce(ctx context.Context, scope *Scope) Any {
	result := self.Left.Reduce(ctx, scope)
	if self.Right == nil {
		return result
	}

	if scope.Bool(result) == true {
		return true
	}

	for _, term := range self.Right {
		result = term.Term.Reduce(ctx, scope)
		if scope.Bool(result) == true {
			return true
		}
	}

	return false
}

func (self _OrExpression) ToString(scope *Scope) string {
	result := []string{self.Left.ToString(scope)}

	for _, right := range self.Right {
		result = append(result, right.Term.ToString(scope))
	}
	return strings.Join(result, " OR ")
}

func (self _AdditionExpression) IsAggregate(scope *Scope) bool {
	if self.Left != nil && self.Left.IsAggregate(scope) {
		return true
	}

	for _, i := range self.Right {
		if i.Term.IsAggregate(scope) {
			return true
		}
	}
	return false
}

func (self _AdditionExpression) Reduce(ctx context.Context, scope *Scope) Any {
	result := self.Left.Reduce(ctx, scope)
	for _, term := range self.Right {
		term_value := term.Term.Reduce(ctx, scope)
		switch term.Operator {
		case "+":
			result = scope.Add(result, term_value)
		case "-":
			result = scope.Sub(result, term_value)
		}
	}

	return result
}

func (self _AdditionExpression) ToString(scope *Scope) string {
	result := self.Left.ToString(scope)

	for _, right := range self.Right {
		result += " " + right.Operator + " " + right.Term.ToString(scope)
	}
	return result
}

func (self _ConditionOperand) IsAggregate(scope *Scope) bool {
	if self.Not != nil && self.Not.IsAggregate(scope) {
		return true
	}

	if self.Left != nil && self.Left.IsAggregate(scope) {
		return true
	}

	if self.Right != nil &&
		self.Right.Right != nil &&
		self.Right.Right.IsAggregate(scope) {
		return true
	}

	return false
}

func (self _ConditionOperand) Reduce(ctx context.Context, scope *Scope) Any {
	if self.Not != nil {
		value := self.Not.Reduce(ctx, scope)
		return !scope.Bool(value)
	}

	lhs := self.Left.Reduce(ctx, scope)
	if self.Right == nil {
		return lhs
	}

	rhs := self.Right.Right.Reduce(ctx, scope)

	var result Any = false

	switch self.Right.Operator {
	case "IN":
		result = scope.membership.Membership(scope, lhs, rhs)
	case "<":
		result = scope.Lt(lhs, rhs)
	case "=":
		result = scope.Eq(lhs, rhs)
	case "!=":
		result = !scope.Eq(lhs, rhs)
	case "<=":
		result = scope.Lt(lhs, rhs) || scope.Eq(lhs, rhs)
	case ">":
		// This only works if there is a matching lt
		// operation.
		if scope.lt.Applicable(scope, lhs, rhs) && !scope.Eq(lhs, rhs) {
			result = !scope.Lt(lhs, rhs)
		}
	case ">=":
		if scope.lt.Applicable(scope, lhs, rhs) {
			result = !scope.Lt(lhs, rhs) || scope.Eq(lhs, rhs)
		}
	case "=~":
		result = scope.Match(rhs, lhs)
	}

	scope.Trace("Operation %v %v %v gave %v", lhs, self.Right.Operator, rhs, result)

	return result
}

func (self _ConditionOperand) ToString(scope *Scope) string {
	if self.Not != nil {
		return "NOT " + self.Not.ToString(scope)
	}

	result := self.Left.ToString(scope)

	if self.Right != nil {
		result += " " + self.Right.Operator + " " +
			self.Right.Right.ToString(scope)
	}

	return result
}

func (self _MultiplicationExpression) IsAggregate(scope *Scope) bool {
	if self.Left != nil && self.Left.IsAggregate(scope) {
		return true
	}

	for _, i := range self.Right {
		if i.Factor.IsAggregate(scope) {
			return true
		}
	}
	return false
}

func (self _MultiplicationExpression) Reduce(ctx context.Context, scope *Scope) Any {
	result := self.Left.Reduce(ctx, scope)
	for _, term := range self.Right {
		term_value := term.Factor.Reduce(ctx, scope)
		switch term.Operator {
		case "*":
			result = scope.Mul(result, term_value)
		case "/":
			result = scope.Div(result, term_value)
		}
	}

	return result
}

func (self _MultiplicationExpression) ToString(scope *Scope) string {
	result := self.Left.ToString(scope)

	for _, right := range self.Right {
		result += " " + right.Operator + " " + right.Factor.ToString(scope)
	}
	return result
}

func (self _Value) IsAggregate(scope *Scope) bool {
	if self.SymbolRef != nil && self.SymbolRef.IsAggregate(scope) {
		return true
	}

	if self.Subexpression != nil && self.Subexpression.IsAggregate(scope) {
		return true
	}

	return false
}

func (self *_Value) maybeParseStrNumber(scope *Scope) {
	if self.Int != nil || self.Float != nil {
		return
	}

	if self.StrNumber != nil {
		// Try to parse it as an integer.
		value, err := strconv.ParseInt(*self.StrNumber, 0, 64)
		if err == nil {
			self.Int = &value
			return
		}

		// Try a float now.
		float_value, err := strconv.ParseFloat(*self.StrNumber, 64)
		if err == nil {
			self.Float = &float_value
			return
		}

		scope.Log("Unable to parse %s as a number.", *self.StrNumber)
	}
}

func unquote(s string) (string, error) {
	quote := s[0]
	s = s[1 : len(s)-1]
	out := ""
	for s != "" {
		value, _, tail, err := strconv.UnquoteChar(s, quote)
		if err != nil {
			return "", err
		}
		s = tail
		out += string(value)
	}
	return out, nil
}

func (self *_Value) Reduce(ctx context.Context, scope *Scope) Any {
	self.maybeParseStrNumber(scope)

	if self.Subexpression != nil {
		return self.Subexpression.Reduce(ctx, scope)
	} else if self.SymbolRef != nil {
		return self.SymbolRef.Reduce(ctx, scope)

	} else if self.Int != nil {
		return *self.Int

	} else if self.Float != nil {
		return *self.Float
	}

	self.mu.Lock()
	defer self.mu.Unlock()
	// The following are static constants and can be cached.
	if self.cache != nil {
		return self.cache
	}

	if self.String != nil {
		result, err := unquote(*self.String)
		if err != nil {
			self.cache = &Null{}
		} else {
			self.cache = result
		}

	} else if self.Boolean != nil {
		self.cache = strings.ToLower(*self.Boolean) == "true"

	} else {
		self.cache = Null{}
	}

	return self.cache
}

func (self _Value) ToString(scope *Scope) string {
	self.maybeParseStrNumber(scope)

	factor := 1.0
	if self.Negated {
		factor = -1.0
	}

	if self.SymbolRef != nil {
		return self.SymbolRef.ToString(scope)
	} else if self.Subexpression != nil {
		return "(" + self.Subexpression.ToString(scope) + ")"

	} else if self.String != nil {
		return *self.String

	} else if self.Int != nil {
		factor := int64(1)
		if self.Negated {
			factor = -1
		}

		return strconv.FormatInt(factor**self.Int, 10)

	} else if self.Float != nil {
		result := strconv.FormatFloat(factor**self.Float, 'f', -1, 64)
		if !strings.Contains(result, ".") {
			result = result + ".0"
		}

		return result

	} else if self.Boolean != nil {
		return *self.Boolean
	} else if self.Null {
		return "NULL"
	} else {
		return "FALSE"
	}
}

func (self *_SymbolRef) IsAggregate(scope *Scope) bool {
	self.mu.Lock()
	defer self.mu.Unlock()

	// If it is not a function then it can not be an aggregate.
	if self.Parameters == nil {
		return false
	}

	// The symbol is a function.
	value, pres := scope.functions[self.Symbol]
	if !pres {
		return false
	}

	return value.Info(scope, NewTypeMap()).IsAggregate
}

func (self *_SymbolRef) Reduce(ctx context.Context, scope *Scope) Any {
	self.mu.Lock()
	defer self.mu.Unlock()

	// Build up the args to pass to the function.
	args := ordereddict.NewDict()
	for _, arg := range self.Parameters {
		if arg.Right != nil {
			// Lazily evaluate right hand side.
			args.Set(arg.Left, LazyExpr{
				Expr:  arg.Right,
				ctx:   ctx,
				scope: scope})

		} else if arg.Array != nil {
			value := arg.Array.Reduce(ctx, scope)
			args.Set(arg.Left, value)

		} else if arg.SubSelect != nil {
			args.Set(arg.Left, arg.SubSelect)
		}
	}

	// If this AST node previously called a function, we use the
	// same function copy to ensure it may store internal state.
	if self.function != nil {
		return self.function.Call(ctx, scope, args)
	}

	// Lookup the symbol in the scope. Functions take
	// precedence over symbols.

	// The symbol is a function.
	func_obj, pres := scope.functions[self.Symbol]
	if pres {
		// Make a copy of the function for next time.
		self.function = func_obj
		return self.function.Call(ctx, scope, args)
	}

	// The symbol is just a constant in the scope.
	value, pres := scope.Resolve(self.Symbol)
	if pres {
		return value
	}

	scope.Log("Symbol %v not found. %s", self.Symbol,
		scope.PrintVars())
	return Null{}
}

func (self *_SymbolRef) ToString(scope *Scope) string {
	self.mu.Lock()
	defer self.mu.Unlock()

	symbol := self.Symbol
	if self.Parameters == nil {
		return symbol
	}

	var substrings []string
	for _, arg := range self.Parameters {
		substrings = append(substrings, arg.ToString(scope))
	}

	return symbol + "(" + strings.Join(substrings, ", ") + ")"
}
