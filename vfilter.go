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
	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	"strconv"
	"strings"
)

var (
	sqlLexer = lexer.Unquote(lexer.Upper(lexer.Must(lexer.Regexp(`(\s+)`+
		`|(?P<Keyword>(?i)SELECT|FROM|TOP|DISTINCT|ALL|WHERE|GROUP +BY|HAVING|UNION|MINUS|EXCEPT|INTERSECT|ORDER|LIMIT|OFFSET|TRUE|FALSE|NULL|IS|NOT|ANY|SOME|BETWEEN|AND |OR |LIKE |AS |IN )`+
		`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)`+
		`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+)?)`+
		`|(?P<String>'[^']*'|"[^"]*")`+
		`|(?P<Operators><>|!=|<=|>=|=~|[-+*/%,.()=<>])`,
	)), "Keyword"), "String")
	sqlParser = participle.MustBuild(&_Select{}, sqlLexer)
)

// Parse the VQL expression. Returns a VQL object which may be
// evaluated.
func Parse(expression string) (*VQL, error) {
	sql := &_Select{}
	err := sqlParser.ParseString(expression, sql)
	return &VQL{query: sql}, err
}

// An opaque object representing the VQL expression.
type VQL struct {
	query *_Select
}

// Evaluate the expression. Returns a channel which emits a series of
// rows.
func (self VQL) Eval(ctx context.Context, scope *Scope) <-chan Row {
	return self.query.Eval(ctx, scope)
}

// Encodes the query into a string again.
func (self VQL) ToString(scope *Scope) string {
	return "SELECT " + self.query.SelectExpression.ToString(scope) +
		" FROM " + self.query.From.ToString(scope)
}

// Provides a list of column names from this query. These columns will
// serve as Row keys for rows that are published on the output channel
// by Eval().
func (self *VQL) Columns(scope *Scope) *[]string {
	if self.query.SelectExpression.All {
		return self.query.From.Plugin.Columns(scope)
	}

	return self.query.SelectExpression.Columns(scope)
}


type _Select struct {
	Top              *_Term             `"SELECT" [ "TOP" @@ ]`
	Distinct         bool              `[  @"DISTINCT"`
	All              bool              ` | @"ALL" ]`
	SelectExpression *_SelectExpression `@@`
	From             *_From             `"FROM" @@`
	Limit            *_CommaExpression  `[ "LIMIT" @@ ]`
	Offset           *_CommaExpression  `[ "OFFSET" @@ ]`
	GroupBy          *_CommaExpression  `[ "GROUPBY" @@ ]`
}

func (self _Select) Eval(ctx context.Context, scope *Scope) <-chan Row {
	output_chan := make(chan Row)
	input_chan := self.From.Eval(ctx, scope)
	go func() {
		defer close(output_chan)
		for {
			select {
			// Are we cancelled?
			case <-ctx.Done():
				return

				// Get a row
			case row, ok := <-input_chan:
				if !ok {
					return
				}
				// We only read one value from the
				// expression and then immediately
				// cancel its context. This is done in
				// case any user function evaluations
				// are asyncronous, but the expression
				// can be evaluated without waiting
				// for them all to complete.
				row_filtered_chan := <-self.SelectExpression.Filter(
					ctx, scope, row)

				output_chan <- row_filtered_chan
			}
		}
	}()

	return output_chan
}

type _From struct {
	Plugin _Plugin           `@@ `
	Where  *_CommaExpression `[ "WHERE" @@ ]`
}

type _Plugin struct {
	Name string  `@Ident `
	Args []*_Args `"(" [ @@  { "," @@ } ] ")" `
}

type _Args struct {
	Left  string          `@Ident "=" `
	Right _CommaExpression `( "(" @@ ")" | @@ )`
}

type _SelectExpression struct {
	All         bool                 `  @"*"`
	Expressions []*_AliasedExpression `| @@ { "," @@ }`
}

type _AliasedExpression struct {
	Expression *_AndExpression `@@`
	As         string         `[ "AS " @Ident ]`
}

// Expressions separated by addition or subtraction.
type _AdditionExpression struct {
	Left  *_MultiplicationExpression `@@`
	Right []*_OpAddTerm              `{ @@ }`
}

type _OpAddTerm struct {
	Operator string                    `@("+" | "-")`
	Term     *_MultiplicationExpression `@@`
}

// Expressions separated by multiplication or division.
type _MultiplicationExpression struct {
	Left  *_MemberExpression `@@`
	Right []*_OpFactor       `{ @@ }`
}

type _OpFactor struct {
	Operator string `@("*" | "/")`
	Factor   *_Value `@@`
}

// Expression for membership access (dot operator).
// e.g. x.y.z
type _MemberExpression struct {
	Left  *_Value              `@@`
	Right []*_OpMembershipTerm `{ @@ }`
}

type _OpMembershipTerm struct {
	Operator string `@"."`
	Term     string `@Ident`
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
	Operator string         `@","`
	Term     *_AndExpression `@@`
}

// Expressions separated by AND.
type _AndExpression struct {
	Left  *_OrExpression `@@`
	Right []*_OpAndTerm  `{ @@ }`
}

type _OpAndTerm struct {
	Operator string        `@"AND "`
	Term     *_OrExpression `@@`
}

// Expressions separated by OR
type _OrExpression struct {
	Left  *_ConditionOperand `@@`
	Right []*_OpOrTerm       `{ @@ }`
}

type _OpOrTerm struct {
	Operator string            `@"OR "`
	Term     *_ConditionOperand `@@`
}

// Conditional expressions imply comparison.
type _ConditionOperand struct {
	Left  *_AdditionExpression `@@`
	Right *_OpComparison       `{ @@ }`
}

type _OpComparison struct {
	Operator string              `@( "<>" | "<=" | ">=" | "=" | "<" | ">" | "!=" | "IN " | "=~")`
	Right    *_AdditionExpression `@@`
}

type _Term struct {
	Select        *_Select          `| @@`
	SymbolRef     *_SymbolRef       `| @@`
	Value         *_Value           `| @@`
	SubExpression *_CommaExpression `| "(" @@ ")"`
}

type _SymbolRef struct {
	//	Symbol     []string `@Ident { "." @Ident }`
	Symbol     string  `@Ident`
	Parameters []*_Args `[ "(" [ @@ { "," @@ } ] ")" ]`
}

type _Value struct {
	Negated       bool             `[ @"-" | "+" ]`
	SymbolRef     *_SymbolRef       `( @@ `
	Subexpression *_CommaExpression `| "(" @@ ")"`
	Number        *float64         ` | @Number`
	String        *string          ` | @String`
	Boolean       *string          ` | @("TRUE" | "FALSE")`
	Null          bool             ` | @"NULL")`
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
type Row interface {}

// A concerete implementation of a row - similar to Python's dict.
type Dict map[string]interface{}


// Filter the row that we receive from the rest of the clause
// according to the select expression.
func (self _SelectExpression) Filter(
	ctx context.Context, scope *Scope, row Row) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		new_row := Dict{}

		// The select uses a * to relay all the row's columns
		if self.All {
			for _, column := range scope.GetMembers(row) {
				if cell, pres := scope.Associative(row, column); pres {
					new_row[column] = cell
				}
			}

			output_chan <- new_row
		} else {
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
			new_scope := *scope
			new_scope.AppendVars(row)

			for _, expr := range self.Expressions {
				expression_chan := expr.Expression.Reduce(ctx, &new_scope)
				expression, ok := <-expression_chan

				if !ok {
					expression = nil
				}

				var column_name string
				if expr.As != "" {
					column_name = expr.As
				} else {
					column_name = expr.Expression.ToString(scope)
				}
				new_row[column_name] = expression
			}

			output_chan <- new_row
		}
	}()

	return output_chan
}

func (self *_SelectExpression) Columns(scope *Scope) *[]string {
	var result []string

	for _, expr := range self.Expressions {
		if expr.As != "" {
			result = append(result, expr.As)
		} else {
			result = append(result, expr.Expression.ToString(scope))
		}
	}

	return &result
}


func (self _SelectExpression) ToString(scope *Scope) string {
	if self.All {
		return "*"
	}
	var substrings []string
	return strings.Join(substrings, ", ")
}

// The From expression runs the Plugin and then filters each row
// according to the Where clause.
func (self _From) Eval(ctx context.Context, scope *Scope) <-chan Row {
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

					if self.Where != nil {
						// If there is a filter clause, we
						// need to filter the row using a new
						// scope.
						new_scope := *scope
						new_scope.AppendVars(row)
						expression_chan := self.Where.Reduce(ctx, &new_scope)
						expression, ok := <-expression_chan

						// If the filtered expression returns
						// a bool true, then pass the row to
						// the output.
						if ok && scope.Bool(expression) {
							output_chan <- row
						}
					} else {
						output_chan <- row
					}
				}
			}
		}
	}()

	return output_chan
}

func (self _From) ToString(scope *Scope) string {
	return self.Plugin.ToString(scope) + " WHERE " + self.Where.ToString(scope)
}

func (self _Plugin) Eval(ctx context.Context, scope *Scope) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		// Build up the args to pass to the function.
		args := Dict{}
		for _, arg := range self.Args {
			value, ok := <-arg.Right.Reduce(ctx, scope)
			if !ok {
				return
			}

			args[arg.Left] = value
		}

		if plugin, pres := scope.plugins[self.Name]; pres {
			plugin_chan := plugin.Call(ctx, scope, args)
			for {
				row, ok := <-plugin_chan
				if !ok {
					return
				}

				output_chan <- row
			}
		} else {
			Debug("plugin not found")
			Debug(self.Name)
		}
	}()

	return output_chan
}

func (self *_Plugin) Columns(scope *Scope) *[]string {
	var result []string

	return &result
}

func (self _Plugin) ToString(scope *Scope) string {
	var substrings []string
	for _, arg := range self.Args {
		substrings = append(substrings, arg.ToString(scope))
	}

	return self.Name + "(" + strings.Join(substrings, ", ") + ")"
}

func (self _Args) ToString(scope *Scope) string {
	return self.Left + "=" + self.Right.ToString(scope)
}

func (self _MemberExpression) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	if self.Right == nil {
		return self.Left.Reduce(ctx, scope)
	}

	output_chan := make(chan Any)
	go func() {
		defer close(output_chan)
		select {
		case <-ctx.Done():
			return

		case lhs, ok := <-self.Left.Reduce(ctx, scope):
			if !ok {
				output_chan <- false
				return
			}

			for _, term := range self.Right {
				var pres bool
				lhs, pres = scope.Associative(lhs, term.Term)
				if !pres {
					output_chan <- false
					return
				}
			}

			output_chan <- lhs
		}
	}()

	return output_chan
}

func (self _MemberExpression) ToString(scope *Scope) string {
	result := []string{self.Left.ToString(scope)}
	for _, right := range self.Right {
		result = append(result, right.Term)
	}
	return strings.Join(result, ".")
}

func (self _CommaExpression) Reduce(
	ctx context.Context, scope *Scope) <-chan Any {

	if self.Right == nil {
		return self.Left.Reduce(ctx, scope)
	}

	output_chan := make(chan Any)
	go func() {
		defer close(output_chan)

		var result []Any

		select {
		case <-ctx.Done():
			return

		case lhs, ok := <-self.Left.Reduce(ctx, scope):
			if !ok {
				output_chan <- false
				return
			}

			result = append(result, lhs)
			for _, term := range self.Right {
				rhs, ok := <-term.Term.Reduce(ctx, scope)
				if ok {
					result = append(result, rhs)
				}
			}

			output_chan <- result
		}
	}()

	return output_chan
}

func (self _CommaExpression) ToString(scope *Scope) string {
	result := []string{self.Left.ToString(scope)}

	for _, right := range self.Right {
		result = append(result, right.Term.ToString(scope))
	}
	return strings.Join(result, ", ")
}

func (self _AndExpression) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	if self.Right == nil {
		return self.Left.Reduce(ctx, scope)
	}

	output_chan := make(chan Any)
	go func() {
		defer close(output_chan)
		var result Any = false

		inputs := []<-chan Any{self.Left.Reduce(ctx, scope)}
		for _, term := range self.Right {
			inputs = append(inputs, term.Term.Reduce(ctx, scope))
		}

		merged_channel := merge_channels(inputs)
		for {
			select {
			case <-ctx.Done():
				return

				// If any of the channels returns a
				// false value, we return false and
				// quit.
			case item, ok := <-merged_channel:
				if !ok {
					output_chan <- result
					return
				}

				if scope.Bool(item) == false {
					output_chan <- false
					return
				}

				result = true
			}
		}
	}()

	return output_chan
}

func (self _AndExpression) ToString(scope *Scope) string {
	result := []string{self.Left.ToString(scope)}

	for _, right := range self.Right {
		result = append(result, right.Term.ToString(scope))
	}
	return strings.Join(result, " AND ")
}

func (self _OrExpression) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	if self.Right == nil {
		return self.Left.Reduce(ctx, scope)
	}

	output_chan := make(chan Any)

	go func() {
		defer close(output_chan)
		inputs := []<-chan Any{self.Left.Reduce(ctx, scope)}
		for _, term := range self.Right {
			inputs = append(inputs, term.Term.Reduce(ctx, scope))
		}

		merged_channel := merge_channels(inputs)
		for {
			select {
			case <-ctx.Done():
				return

				// If any of the channels returns a
				// true value, we return the value and
				// quit.
			case result, ok := <-merged_channel:
				if !ok {
					// If we get here we exhausted
					// the merged channels without
					// a true result, so we return
					// false
					output_chan <- false
					return
				}

				if scope.Bool(result) == true {
					output_chan <- true
					return
				}
			}
		}
	}()

	return output_chan
}

func (self _OrExpression) ToString(scope *Scope) string {
	result := []string{self.Left.ToString(scope)}

	for _, right := range self.Right {
		result = append(result, right.Term.ToString(scope))
	}
	return strings.Join(result, " OR ")
}

func (self _AdditionExpression) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	if self.Right == nil {
		return self.Left.Reduce(ctx, scope)
	}

	output_chan := make(chan Any)

	go func() {
		defer close(output_chan)

		var term_chans []<-chan Any
		var operators []string
		for _, term := range self.Right {
			term_chans = append(term_chans, term.Term.Reduce(ctx, scope))
			operators = append(operators, term.Operator)
		}

		select {
		case <-ctx.Done():
			return

		case lhs := <-self.Left.Reduce(ctx, scope):
			for idx, term_chan := range term_chans {
				rhs := <-term_chan
				op := operators[idx]
				if op == "+" {
					lhs = scope.Add(lhs, rhs)
				} else if op == "-" {
					lhs = scope.Sub(lhs, rhs)
				}
			}

			output_chan <- lhs
		}
	}()

	return output_chan
}

func (self _AdditionExpression) ToString(scope *Scope) string {
	result := self.Left.ToString(scope)

	for _, right := range self.Right {
		result += " " + right.Operator + " " + right.Term.ToString(scope)
	}
	return result
}

func (self _ConditionOperand) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	if self.Right == nil {
		return self.Left.Reduce(ctx, scope)
	}

	output_chan := make(chan Any)

	comparator := func(lhs Any, rhs Any) bool {
		op := self.Right.Operator

		if op == "IN " {
			return scope.membership.Membership(scope, lhs, rhs)
		} else if op == "<" {
			return scope.Lt(lhs, rhs)
		}

		is_eq := scope.Eq(lhs, rhs)

		if op == "=" {
			return is_eq
		} else if op == "!=" {
			return !is_eq
		} else if op == "<=" {
			return scope.Lt(lhs, rhs) || is_eq
		} else if op == ">" {
			// This only works if there is a matching lt
			// operation.
			if scope.lt.Applicable(lhs, rhs) && !is_eq {
				return !scope.Lt(lhs, rhs)
			}
		} else if op == ">=" {
			if scope.lt.Applicable(lhs, rhs) {
				return !scope.Lt(lhs, rhs) || is_eq
			}
		} else if op == "=~" {
			return scope.Match(rhs, lhs)
		}

		return false
	}

	go func() {
		defer close(output_chan)

		var lhs Any
		var rhs Any
		var ok bool

		select {
		case <-ctx.Done():
			return

		// Run the Left and Right channels and wait for both.
		case lhs, ok = <-self.Left.Reduce(ctx, scope):
			if !ok {
				output_chan <- false
				return
			}
		}

		select {
		case <-ctx.Done():
			return

		case rhs, ok = <-self.Right.Right.Reduce(ctx, scope):
			if !ok {
				output_chan <- false
				return
			}
		}

		output_chan <- comparator(lhs, rhs)
	}()

	return output_chan
}

func (self _ConditionOperand) ToString(scope *Scope) string {
	result := self.Left.ToString(scope)

	if self.Right != nil {
		result += " " + self.Right.Operator + " " +
			self.Right.Right.ToString(scope)
	}

	return result
}

func (self _MultiplicationExpression) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	if self.Right == nil {
		return self.Left.Reduce(ctx, scope)
	}

	output_chan := make(chan Any)

	go func() {
		defer close(output_chan)

		var term_chans []<-chan Any
		var operators []string
		for _, term := range self.Right {
			term_chans = append(term_chans, term.Factor.Reduce(ctx, scope))
			operators = append(operators, term.Operator)
		}

		lhs := <-self.Left.Reduce(ctx, scope)
		for idx, term_chan := range term_chans {
			rhs := <-term_chan
			op := operators[idx]
			if op == "*" {
				lhs = scope.Mul(lhs, rhs)
			} else if op == "/" {
				lhs = scope.Div(lhs, rhs)
			}
		}

		output_chan <- lhs
	}()

	return output_chan
}

func (self _MultiplicationExpression) ToString(scope *Scope) string {
	result := self.Left.ToString(scope)

	for _, right := range self.Right {
		result += " " + right.Operator + " " + right.Factor.ToString(scope)
	}
	return result
}

func (self _Value) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	if self.Subexpression != nil {
		return self.Subexpression.Reduce(ctx, scope)
	} else if self.SymbolRef != nil {
		return self.SymbolRef.Reduce(ctx, scope)
	}

	output_chan := make(chan Any)

	go func() {
		defer close(output_chan)

		if self.String != nil {
			output_chan <- *self.String
		} else if self.Number != nil {
			output_chan <- *self.Number
		} else if self.Boolean != nil {
			output_chan <- *self.Boolean == "TRUE"
		} else if self.Null {
			output_chan <- nil
		} else {
			output_chan <- false
		}
	}()

	return output_chan
}

func (self _Value) ToString(scope *Scope) string {
	factor := 1.0
	if self.Negated {
		factor = -1.0
	}

	if self.SymbolRef != nil {
		return self.SymbolRef.ToString(scope)
	} else if self.Subexpression != nil {
		return "(" + self.Subexpression.ToString(scope) + ")"
	} else if self.String != nil {
		return "'" + *self.String + "'"
	} else if self.Number != nil {
		return strconv.FormatFloat(factor**self.Number, 'f', -1, 32)
	} else if self.Boolean != nil {
		return *self.Boolean
	} else if self.Null {
		return "NULL"
	} else {
		return "FALSE"
	}
}

func (self _SymbolRef) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	output_chan := make(chan Any)

	go func() {
		defer close(output_chan)

		// Build up the args to pass to the function.
		row := Dict{}
		for _, arg := range self.Parameters {
			value, ok := <-arg.Right.Reduce(ctx, scope)
			if !ok {
				output_chan <- false
				return
			}

			row[arg.Left] = value
		}

		// The symbol is just a constant in the scope.
		if value, pres := scope.Resolve(self.Symbol); pres {
			output_chan <- value

			// The symbol is a function.
		} else if value, pres := scope.functions[self.Symbol]; pres {
			output_chan <- value.Call(ctx, scope, row)

		} else {
			Debug(self.Symbol)
			Debug("Symbol not found.")
			// Todo: proper error handling.
			output_chan <- false
		}
	}()

	return output_chan
}

func (self _SymbolRef) ToString(scope *Scope) string {
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
