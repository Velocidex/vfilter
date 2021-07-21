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
  func (self FooAdder) Add(scope types.Scope, a Any, b Any) Any {
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
	"strconv"
	"strings"
	"sync"

	"github.com/Velocidex/ordereddict"
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	errors "github.com/pkg/errors"
	"www.velocidex.com/golang/vfilter/functions"
	"www.velocidex.com/golang/vfilter/scope"
	scope_module "www.velocidex.com/golang/vfilter/scope"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
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
			`|(?ims)(?P<AlternativeOR>\|+)` +
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
			"|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*|`[^`]+`)" +
			`|''(?P<MultilineString>'.*?')''` +
			`|(?P<String>'([^'\\]*(\\.[^'\\]*)*)'|"([^"\\]*(\\.[^"\\]*)*)")` +
			`|(?P<Number>[-+]?(0x)?\d*\.?\d+([eE][-+]?\d+)?)` +
			`|(?P<Operators><>|!=|<=|>=|=>|=~|[-+*/%,.()=<>{}\[\]])`,
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
		end := t.Tok.Pos.Offset + 10
		if end >= len(expression) {
			end = len(expression) - 1
		}
		if end < 0 {
			end = 0
		}

		start := t.Tok.Pos.Offset - 10
		if start < 0 {
			start = 0
		}

		pos := t.Tok.Pos.Offset
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
		end := t.Tok.Pos.Offset + 10
		if end >= len(expression) {
			end = len(expression) - 1
		}
		if end < 0 {
			end = 0
		}

		start := t.Tok.Pos.Offset - 10
		if start < 0 {
			start = 0
		}

		pos := t.Tok.Pos.Offset
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
	Let         string          `( LET  @Ident `
	Parameters  *_ParameterList `{ "(" @@ ")" }`
	LetOperator string          ` ( @"=" | @"<=" ) `
	StoredQuery *_Select        ` ( @@ |  `
	Expression  *_AndExpression ` @@ ) |`
	Query       *_Select        `  @@ )  `
}

type _ParameterList struct {
	Left  string              ` @Ident `
	Right *_ParameterListTerm `{ @@ }`
}

func (self _ParameterList) ToString(scope types.Scope) string {
	result := self.Left

	if self.Right != nil {
		result += ", " + self.Right.Term.ToString(scope)
	}
	return result
}

type _ParameterListTerm struct {
	Operator string          `@","`
	Term     *_ParameterList ` @@ `
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
func (self VQL) Eval(ctx context.Context, scope types.Scope) <-chan Row {
	output_chan := make(chan Row)

	// If this is a Let expression we need to create a stored
	// query and assign to the scope.
	if len(self.Let) > 0 {
		if self.Parameters != nil && self.LetOperator == "<=" {
			scope.Log("Expression %v takes parameters but is "+
				"materialized! Did you mean to use '='? ", self.Let)
		}

		_, pres := scope.GetFunction(self.Let)
		if pres {
			scope.Log("LET expression is masking a built in function %v", self.Let)
		}

		name := utils.Unquote_ident(self.Let)

		// Let assigning an expression.
		if self.Expression != nil {
			expr := &StoredExpression{
				Expr: self.Expression,
				name: name,
			}

			if self.Parameters != nil {
				expr.parameters = self.getParameters()
			}

			switch self.LetOperator {
			// Store the expression in the scope for later.
			case "=":
				scope.AppendVars(ordereddict.NewDict().
					Set(name, expr))

				// If we are materializing here,
				// reduce it now.
			case "<=":
				// It may yield a stored query - in
				// that case we materialize it in
				// place.
				value := expr.Reduce(ctx, scope)
				stored_query, ok := value.(types.StoredQuery)
				if ok {
					value = types.Materialize(ctx, scope, stored_query)
				}
				scope.AppendVars(ordereddict.NewDict().Set(name, value))
			}
			close(output_chan)
			return output_chan
		}

		// LET is for stored query: LET X = SELECT ...
		switch self.LetOperator {
		case "=":
			stored_query := NewStoredQuery(self.StoredQuery, name)
			if self.Parameters != nil {
				stored_query.parameters = self.getParameters()
			}

			scope.AppendVars(ordereddict.NewDict().Set(name, stored_query))
		case "<=":
			scope.AppendVars(ordereddict.NewDict().Set(
				name, types.Materialize(ctx, scope, self.StoredQuery)))
		}

		close(output_chan)
		return output_chan

	} else {
		subscope := scope.Copy()
		subscope.AppendVars(ordereddict.NewDict().Set("$Query", self.ToString(scope)))

		go func() {
			defer close(output_chan)
			defer subscope.Close()

			row_chan := self.Query.Eval(ctx, subscope)
			for {
				select {
				case <-ctx.Done():
					return

				case row, ok := <-row_chan:
					if !ok {
						return
					}
					output_chan <- row
				}
			}
		}()

		return output_chan
	}
}

// Walk the parameters list and collect all the parameter names.
func visitor(parameters *_ParameterList, result *[]string) {
	*result = append(*result, parameters.Left)
	if parameters.Right != nil {
		visitor(parameters.Right.Term, result)
	}
}

func (self VQL) getParameters() []string {
	result := []string{}

	if self.Let != "" && self.Parameters != nil {
		visitor(self.Parameters, &result)
	}

	return result
}

// Encodes the query into a string again.
func (self VQL) ToString(scope types.Scope) string {
	if self.Let != "" {
		operator := " = "
		if self.LetOperator != "" {
			operator = self.LetOperator
		}

		parameters := ""
		if self.Parameters != nil {
			parameters = "(" +
				strings.Join(self.getParameters(), ",") +
				")"
		}

		if self.Expression != nil {
			return "LET " + self.Let + parameters +
				operator + self.Expression.ToString(scope)
		}
		return "LET " + self.Let + parameters +
			operator + self.StoredQuery.ToString(scope)
	}

	return self.Query.ToString(scope)
}

type _Select struct {
	SelectExpression *_SelectExpression `SELECT @@`
	From             *_From             `FROM @@`
	Where            *_CommaExpression  `[ WHERE @@ ]`
	GroupBy          *_CommaExpression  `[ GROUPBY @@ ]`
	OrderBy          *string            `[ ORDERBY @Ident `
	OrderByDesc      *bool              ` [ @DESC ] ]`
	Limit            *int64             `[ LIMIT @Number ]`
}

func (self *_Select) ToString(scope types.Scope) string {
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
		result += " GROUP BY " + self.GroupBy.ToString(scope)
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

func (self *_Select) Eval(ctx context.Context, scope types.Scope) <-chan Row {
	if self.GroupBy != nil {
		return self.EvalGroupBy(ctx, scope)
	}

	output_chan := make(chan Row)

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
				select {
				case <-ctx.Done():
					return
				case output_chan <- row:
				}
				count += 1
				if count > limit {
					return
				}
			}
		}()

		return output_chan
	}

	if self.OrderBy != nil {
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

			for row := range self_copy.Eval(ctx, scope) {
				sorter_input_chan <- row
			}
		}()

		return sorted_chan
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
				self.processSingleRow(ctx, scope, row, output_chan)
			}
		}
	}()

	return output_chan
}

func (self *_Select) processSingleRow(
	ctx context.Context, scope types.Scope, row Row, output_chan chan Row) {
	subscope := scope.Copy()
	defer subscope.Close()

	transformed_row, closer := self.SelectExpression.Transform(
		ctx, subscope, row)
	defer closer()

	if self.Where == nil {
		select {
		case <-ctx.Done():
			return
		case output_chan <- MaterializedLazyRow(
			ctx, transformed_row, subscope):
		}
	} else {
		// If there is a filter clause, we need to filter the
		// row using a new scope.
		new_scope := subscope.Copy()
		defer new_scope.Close()

		// Filters can access both the untransformed row and
		// the transformed row. This allows WHERE clause to
		// refer to both the raw plugin output as well as
		// aliases of transformations on the row.
		new_scope.AppendVars(row)
		new_scope.AppendVars(transformed_row)

		expression := self.Where.Reduce(ctx, new_scope)

		// If the filtered expression returns a bool true,
		// then pass the row to the output.
		if expression != nil && scope.Bool(expression) {
			select {
			case <-ctx.Done():
				return
			case output_chan <- MaterializedLazyRow(
				ctx, transformed_row, new_scope):
			}
		} else {
			scope.Trace("Row rejected")
		}
	}
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

	mu                 sync.Mutex
	cache, column_name *string
}

// Cache the column name since each row needs it
func (self *_AliasedExpression) GetName(scope types.Scope) string {
	self.mu.Lock()
	column_name := self.column_name
	self.mu.Unlock()

	if column_name != nil {
		return *column_name
	}

	if self.As != "" {
		name := utils.Unquote_ident(self.As)
		column_name = &name
	} else {
		name := utils.Unquote_ident(self.ToString(scope))
		column_name = &name
	}

	self.mu.Lock()
	self.column_name = column_name
	self.mu.Unlock()

	return *column_name
}

func (self *_AliasedExpression) IsAggregate(scope types.Scope) bool {
	if self.SubSelect != nil {
		return true
	}

	if self.Expression.IsAggregate(scope) {
		return true
	}

	return false
}

func (self *_AliasedExpression) Reduce(ctx context.Context, scope types.Scope) Any {
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

func (self *_AliasedExpression) ToString(scope types.Scope) string {
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
	Operator string         ` @AND `
	Term     *_OrExpression `@@`
}

// Expressions separated by OR
type _OrExpression struct {
	Left  *_ConditionOperand `@@`
	Right []*_OpOrTerm       `{ @@ }`
}

type _OpOrTerm struct {
	Operator string             ` (@OR | @AlternativeOR) `
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
	Symbol     string   `@Ident { @"." @Ident }`
	Called     bool     `{ @"(" `
	Parameters []*_Args ` [ @@ { "," @@ } ] ")" } `

	mu       sync.Mutex
	function FunctionInterface
}

type _Value struct {
	Negated       bool              `[ "-" | "+" ]`
	SymbolRef     *_SymbolRef       `( @@ `
	Subexpression *_CommaExpression `| "(" @@ ")"`

	String *string ` | @( MultilineString | String ) `

	// Figure out if this is an int or float.
	StrNumber *string ` | @Number`
	Float     *float64
	Int       *int64

	Boolean *string ` | @BOOL `
	Null    bool    ` | @NULL)`

	mu    sync.Mutex
	cache Any
}

// Receives a row from the FROM clause (i.e. the plugin) and
// transforms it according to the select expression to produce a new
// row. The transformation results in a lazy row - The column
// expressions are not evaluated, instead they are wrapped in an
// evaluator which will reduce when any column is accessed. The scope
// in which the lazy columns are evaluated is created by extending the
// existing scope with the row scope that came from the plugin.  NOTE:
// Returns a closer which should be called when the LazyRow is
// resolved and not needed any more.
func (self *_SelectExpression) Transform(
	ctx context.Context, scope types.Scope, row Row) (types.LazyRow, func()) {
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
	new_row := NewLazyRow(ctx, scope)

	// If there is a * expression in addition to the
	// column expressions, this is equivalent to adding
	// all the columns as defined by the * as if they were
	// explicitely defined.
	if self.All {
		for _, member := range scope.GetMembers(row) {
			value, pres := scope.Associative(row, member)
			if pres {
				new_row.AddColumn(member,
					func(ctx context.Context, scope types.Scope) Any {
						return value
					})
			}
		}
	}

	// Scope will be closed with the parent - need to keep alive
	// until the row is materialized.
	new_scope := scope.Copy()
	new_scope.AppendVars(row)

	for _, expr_ := range self.Expressions {
		// A copy of the expression for the lambda capture.
		expr := expr_

		new_row.AddColumn(
			expr.GetName(scope),

			// Use the new scope rather than the callers
			// scope since the lazy row may be accessed in
			// any scope but needs to resolve members in
			// the scope it was created from.
			func(ctx context.Context, scope types.Scope) Any {
				return expr.Reduce(ctx, new_scope)
			})
	}

	return new_row, new_scope.Close
}

func (self *_SelectExpression) ToString(scope types.Scope) string {
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
func (self *_From) Eval(ctx context.Context, scope types.Scope) <-chan Row {
	output_chan := make(chan Row)

	input_chan := self.Plugin.Eval(ctx, scope)
	go func() {
		defer close(output_chan)
		for row := range input_chan {
			select {
			case <-ctx.Done():
				return

			case output_chan <- row:
			}
		}
	}()

	return output_chan
}

func (self *_From) ToString(scope types.Scope) string {
	result := self.Plugin.ToString(scope)
	return result
}

func (self *_Plugin) getPlugin(scope types.Scope, plugin_name string) (
	PluginGeneratorInterface, bool) {
	components := strings.Split(plugin_name, ".")
	// Single plugin reference.
	if len(components) == 1 {
		// Try to find the plugin in the scope plugins.
		plugin, pres := scope.GetPlugin(plugin_name)
		if !pres {
			// Otherwise maybe there is a plugin-like
			// object in the scope.
			symbol, _ := scope.Resolve(plugin_name)
			plugin, pres = symbol.(PluginGeneratorInterface)
		}

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

func (self *_Plugin) Eval(ctx context.Context, scope types.Scope) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		if scope.CheckForOverflow() {
			return
		}

		// The FROM clause refers to a var and not a
		// plugin. Just read the var from the scope.
		if !self.Call {
			variable, pres := scope.Resolve(self.Name)
			if pres {
				// If the variable is a stored query
				// we just copy from its channel to
				// the output.
				stored_query, ok := variable.(StoredQuery)
				if ok {
					from_chan := stored_query.Eval(ctx, scope)
					for row := range from_chan {
						select {
						case <-ctx.Done():
							return
						case output_chan <- row:
						}
					}

				} else if utils.IsArray(variable) {
					var_slice := reflect.ValueOf(variable)
					for i := 0; i < var_slice.Len(); i++ {
						select {
						case <-ctx.Done():
							return
						case output_chan <- var_slice.Index(i).Interface():
						}
					}
				} else {
					select {
					case <-ctx.Done():
						return
					case output_chan <- variable:
					}
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
				name := utils.Unquote_ident(arg.Left)
				args.Set(name, NewLazyExpr(ctx, scope, arg.Right))

			} else if arg.Array != nil {
				value := arg.Array.Reduce(ctx, scope)
				if value == nil {
					select {
					case <-ctx.Done():
						return
					case output_chan <- Null{}:
					}
					return
				}
				args.Set(arg.Left, value)

			} else if arg.SubSelect != nil {
				args.Set(arg.Left, arg.SubSelect)
			}
		}

		if plugin, pres := self.getPlugin(scope, self.Name); pres {
			scope.GetStats().IncPluginsCalled()
			for row := range plugin.Call(ctx, scope, args) {
				select {
				case <-ctx.Done():
					return

				case output_chan <- row:
					scope.GetStats().IncRowsScanned()
				}
			}
		} else {
			options := scope.GetSimilarPlugins(self.Name)
			message := fmt.Sprintf("Plugin %v not found. ", self.Name)
			if len(options) > 0 {
				message += fmt.Sprintf(
					"Did you mean %v? ",
					strings.Join(options, " "))
			}

			_, pres := scope.GetFunction(self.Name)
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

func (self *_Plugin) ToString(scope types.Scope) string {
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

func (self *_Args) ToString(scope types.Scope) string {
	if self.Right != nil {
		return self.Left + "=" + self.Right.ToString(scope)
	} else if self.SubSelect != nil {
		return self.Left + "= { " + self.SubSelect.ToString(scope) + "}"
	} else if self.Array != nil {
		return self.Left + "= [" + self.Array.ToString(scope) + "]"
	}
	return ""
}

func (self *_MemberExpression) IsAggregate(scope types.Scope) bool {
	if self.Left != nil && self.Left.IsAggregate(scope) {
		return true
	}

	return false
}

func (self *_MemberExpression) Reduce(ctx context.Context, scope types.Scope) Any {
	lhs := self.Left.Reduce(ctx, scope)
	for _, term := range self.Right {
		var pres bool

		// Slice index implementation via Associative protocol.
		if term.Index != nil {
			lhs, pres = scope.Associative(lhs, term.Index)
		} else {
			lhs, pres = scope.Associative(lhs, utils.Unquote_ident(term.Term))
		}
		if !pres {
			return Null{}
		}
	}

	return lhs
}

func (self *_MemberExpression) ToString(scope types.Scope) string {
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

func (self *_CommaExpression) IsAggregate(scope types.Scope) bool {
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

func (self _CommaExpression) Reduce(ctx context.Context, scope types.Scope) Any {
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

func (self _CommaExpression) ToString(scope types.Scope) string {
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

func (self *_AndExpression) IsAggregate(scope types.Scope) bool {
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

func (self _AndExpression) Reduce(ctx context.Context, scope types.Scope) Any {
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

func (self _AndExpression) ToString(scope types.Scope) string {
	result := []string{self.Left.ToString(scope)}

	for _, right := range self.Right {
		result = append(result, right.Operator)
		result = append(result, right.Term.ToString(scope))
	}
	return strings.Join(result, " ")
}

func (self *_OrExpression) IsAggregate(scope types.Scope) bool {
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

func (self _OrExpression) Reduce(ctx context.Context, scope types.Scope) Any {
	result := self.Left.Reduce(ctx, scope)
	if self.Right == nil {
		return result
	}

	if scope.Bool(result) == true {
		return result
	}

	for _, term := range self.Right {
		result = term.Term.Reduce(ctx, scope)
		if scope.Bool(result) == true {
			if term.Operator == "||" {
				return result
			}
			return true
		}
	}

	return false
}

func (self _OrExpression) ToString(scope types.Scope) string {
	result := []string{self.Left.ToString(scope)}

	for _, right := range self.Right {
		result = append(result, right.Operator)
		result = append(result, right.Term.ToString(scope))
	}
	return strings.Join(result, " ")
}

func (self _AdditionExpression) IsAggregate(scope types.Scope) bool {
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

func (self _AdditionExpression) Reduce(ctx context.Context, scope types.Scope) Any {
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

func (self _AdditionExpression) ToString(scope types.Scope) string {
	result := self.Left.ToString(scope)

	for _, right := range self.Right {
		result += " " + right.Operator + " " + right.Term.ToString(scope)
	}
	return result
}

func (self _ConditionOperand) IsAggregate(scope types.Scope) bool {
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

func (self _ConditionOperand) Reduce(ctx context.Context, scope types.Scope) Any {
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
		result = scope.Membership(lhs, rhs)
	case "<":
		result = scope.Lt(lhs, rhs)
	case "=":
		result = scope.Eq(lhs, rhs)
	case "!=":
		result = !scope.Eq(lhs, rhs)
	case "<=":
		result = scope.Lt(lhs, rhs) || scope.Eq(lhs, rhs)
	case ">":
		result = scope.Gt(lhs, rhs)
	case ">=":
		result = scope.Gt(lhs, rhs) || scope.Eq(lhs, rhs)
	case "=~":
		result = scope.Match(rhs, lhs)
	}

	scope.Trace("Operation %v %v %v gave %v", lhs, self.Right.Operator, rhs, result)

	return result
}

func (self _ConditionOperand) ToString(scope types.Scope) string {
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

func (self _MultiplicationExpression) IsAggregate(scope types.Scope) bool {
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

func (self _MultiplicationExpression) Reduce(ctx context.Context, scope types.Scope) Any {
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

func (self _MultiplicationExpression) ToString(scope types.Scope) string {
	result := self.Left.ToString(scope)

	for _, right := range self.Right {
		result += " " + right.Operator + " " + right.Factor.ToString(scope)
	}
	return result
}

func (self _Value) IsAggregate(scope types.Scope) bool {
	if self.SymbolRef != nil && self.SymbolRef.IsAggregate(scope) {
		return true
	}

	if self.Subexpression != nil && self.Subexpression.IsAggregate(scope) {
		return true
	}

	return false
}

func (self *_Value) maybeParseStrNumber(scope types.Scope) {
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

func (self *_Value) Reduce(ctx context.Context, scope types.Scope) Any {
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
		self.cache = utils.Unquote(*self.String)
	} else if self.Boolean != nil {
		self.cache = strings.ToLower(*self.Boolean) == "true"

	} else {
		self.cache = Null{}
	}

	return self.cache
}

func (self _Value) ToString(scope types.Scope) string {
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

func (self *_SymbolRef) IsAggregate(scope types.Scope) bool {
	self.mu.Lock()
	// If it is not a function then it can not be an aggregate.
	if self.Parameters == nil {
		return false
	}

	symbol := self.Symbol
	self.mu.Unlock()

	// The symbol is a function.
	value, pres := scope.GetFunction(symbol)
	if !pres {
		return false
	}

	return value.Info(scope, types.NewTypeMap()).IsAggregate
}

func (self *_SymbolRef) getFunction(scope types.Scope, components []string) (types.Any, bool) {
	// Single item reference and called - call built in function.
	if len(components) == 1 && self.Called {
		res, pres := scope.GetFunction(self.Symbol)
		if pres {
			return res, pres
		}
	}

	// Plugins with "." resolve themselves recursively.
	var result Any = scope
	for idx, component := range components {
		subcomponent, pres := scope.Associative(result, component)
		if !pres {
			// Only warn when accessing a top level component:
			// SELECT Foobar FROM scope() -> warn if Foobar is not found
			// SELECT Foo.Bar FROM scope() -> warn
			// if Foo is not found but not if Foo is found but Bar is not found
			if idx == 0 {
				if len(components) > 1 {
					scope.Log("While resolving %v Symbol %v not found. %s",
						self.Symbol, components[0], scope.PrintVars())
				} else {
					scope.Log("Symbol %v not found. %s", self.Symbol, scope.PrintVars())
				}
			}

			return nil, false
		}

		result = subcomponent
	}

	return result, true
}

func (self *_SymbolRef) Reduce(ctx context.Context, scope types.Scope) Any {
	components := utils.SplitIdent(self.Symbol)

	// The symbol is just a constant in the scope. It may be a
	// stored expression, a function or a stored query or just a
	// plain value.
	value, pres := self.getFunction(scope, components)
	if value != nil && pres {
		switch t := value.(type) {
		case FunctionInterface:
			if !self.Called {
				scope.Log("Symbol %v is a function but it is not being called.",
					self.Symbol)
				return &Null{}
			}

			// The symbol is a function and this is a call site, e.g. Symbol(...)
			return self.callFunction(ctx, scope, t)

		// If the symbol is a stored expression we evaluated
		// it.
		case *StoredExpression:
			subscope := scope.Copy()
			defer subscope.Close()

			if subscope.CheckForOverflow() {
				return &Null{}
			}

			subscope.AppendVars(self.buildArgsFromParameters(
				ctx, scope))

			scope.GetStats().IncFunctionsCalled()
			return t.Reduce(ctx, subscope)

		case StoredQuery:
			// If the call site specifies parameters then
			// we materialize the plugin at this
			// point. Otherwise pass the stored query
			// through.
			if self.Parameters != nil {
				subscope := scope.Copy()
				defer subscope.Close()

				if subscope.CheckForOverflow() {
					return &Null{}
				}

				subscope.AppendVars(self.buildArgsFromParameters(
					ctx, scope))
				scope.GetStats().IncFunctionsCalled()

				// Wrap the query with the captured scope.
				return &StoredQueryCallSite{
					query: t,
					scope: subscope,
				}
			}
		}

		if self.Called {
			scope.Log("Symbol %v is not a function but it is being called.",
				self.Symbol)
			return &Null{}
		}

		// Every thing else is taken literally.
		return value
	}

	return Null{}
}

// Interpolate the parameters into a subscope to get ready to call
// into the VQL stored query with parameters
func (self *_SymbolRef) buildArgsFromParameters(
	ctx context.Context, scope types.Scope) *ordereddict.Dict {

	args := ordereddict.NewDict()

	// Not a function call - pass the scope as it is.
	if !self.Called {
		return args
	}

	self.mu.Lock()
	parameters := self.Parameters
	self.mu.Unlock()

	// When calling into a VQL stored function, we materialize all
	// args.
	for _, arg := range parameters {
		if arg.Right != nil {
			name := utils.Unquote_ident(arg.Left)
			args.Set(name, arg.Right.Reduce(ctx, scope))
		} else if arg.Array != nil {
			value := arg.Array.Reduce(ctx, scope)
			args.Set(arg.Left, value)

		} else if arg.SubSelect != nil {
			args.Set(arg.Left, arg.SubSelect)
		}
	}

	return args
}

// Call into a built in VQL function.
func (self *_SymbolRef) callFunction(
	ctx context.Context, scope types.Scope,
	func_obj FunctionInterface) Any {

	self.mu.Lock()
	parameters := self.Parameters
	function := self.function
	self.mu.Unlock()

	// Build up the args to pass to the function.
	args := ordereddict.NewDict()
	for _, arg := range parameters {
		if arg.Right != nil {
			// Lazily evaluate right hand side.
			name := utils.Unquote_ident(arg.Left)
			args.Set(name, NewLazyExpr(ctx, scope, arg.Right))

		} else if arg.Array != nil {
			value := arg.Array.Reduce(ctx, scope)
			args.Set(arg.Left, value)

		} else if arg.SubSelect != nil {
			args.Set(arg.Left, arg.SubSelect)
		}
	}

	// If this AST node previously called a function, we use the
	// same function copy to ensure it may store internal state.
	if function != nil {
		scope.GetStats().IncFunctionsCalled()
		result := function.Call(ctx, scope, args)
		if result == nil {
			return &Null{}
		}
		return result
	}

	// Make a copy of the function and cache it for next time -
	// this allows the function to store state since each
	// reference in the AST is unique.
	func_obj = CopyFunction(func_obj)

	self.mu.Lock()
	self.function = func_obj
	self.mu.Unlock()

	// Call the function now.
	scope.GetStats().IncFunctionsCalled()

	result := func_obj.Call(ctx, scope, args)

	// Do not allow nil in VQL since it is not compatible with
	// reflect package. The VQL plugin might accidentally pass nil
	if utils.IsNil(result) {
		return &Null{}
	}

	return result
}

func (self *_SymbolRef) ToString(scope types.Scope) string {
	self.mu.Lock()
	defer self.mu.Unlock()

	symbol := self.Symbol
	if !self.Called && self.Parameters == nil {
		return symbol
	}

	var substrings []string
	for _, arg := range self.Parameters {
		substrings = append(substrings, arg.ToString(scope))
	}

	return symbol + "(" + strings.Join(substrings, ", ") + ")"
}

func GetIntScope(scope_int types.Scope) *scope.Scope {
	result, ok := scope_int.(*scope.Scope)
	if ok {
		return result
	}
	// Should never happen
	panic("Unexpected scope seen!")
}

func CopyFunction(in types.Any) types.FunctionInterface {
	copier, ok := in.(types.FunctionCopier)
	if ok {
		return copier.Copy()
	}

	in_value := reflect.Indirect(reflect.ValueOf(in))
	result := reflect.New(in_value.Type()).Interface()

	// Handle aggregate functions specifically.
	aggregate_func, ok := result.(functions.AggregatorInterface)
	if ok {
		aggregate_func.SetNewAggregator()
	}

	return result.(types.FunctionInterface)
}
