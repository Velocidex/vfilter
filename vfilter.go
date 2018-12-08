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

	"github.com/alecthomas/participle"
	"github.com/alecthomas/participle/lexer"
	errors "github.com/pkg/errors"
)

var (
	sqlLexer = lexer.Unquote(lexer.Upper(lexer.Must(lexer.Regexp(
		`(?ms)`+
			`(\s+)`+
			`|(^/[*].*?[*]/$)`+ // C Style comment.
			`|(^--.*?$)`+ // SQL style one line comment.
			`|(^//.*?$)`+ // C++ style one line comment.
			`|(?i)(?P<Keyword>LET |SELECT |FROM|TOP|DISTINCT|ALL|WHERE|GROUP +BY|HAVING|UNION|MINUS|EXCEPT|INTERSECT|ORDER +BY|LIMIT|TRUE|FALSE|NULL|IS |NOT |ANY|SOME|BETWEEN|AND |OR |LIKE |AS |IN |\\bDESC\\b)`+
			`|(?P<Ident>[a-zA-Z_][a-zA-Z0-9_]*)`+
			`|(?P<Number>[-+]?\d*\.?\d+([eE][-+]?\d+)?)`+
			`|(?P<String>'([^'\\]*(\\.[^'\\]*)*)'|"([^"\\]*(\\.[^"\\]*)*)")`+
			`|(?P<Operators><>|!=|<=|>=|=~|[-+*/%,.()=<>{}\[\]])`,
	)), "Keyword"), "String")
	sqlParser = participle.MustBuild(&VQL{}, sqlLexer)
)

// Parse the VQL expression. Returns a VQL object which may be
// evaluated.
func Parse(expression string) (*VQL, error) {
	sql := &VQL{}
	err := sqlParser.ParseString(expression, sql)
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

		return sql, errors.Wrap(
			err,
			expression[start:pos]+"|"+expression[pos:end])
	default:
		return sql, err
	}
}

// An opaque object representing the VQL expression.
type VQL struct {
	Let         string   `{ "LET " @Ident `
	LetOperator string   ` ( @"=" | @"<=" ) }`
	Query       *_Select ` @@ `
}

// Evaluate the expression. Returns a channel which emits a series of
// rows.
func (self VQL) Eval(ctx context.Context, scope *Scope) <-chan Row {
	// If this is a Let expression we need to create a stored
	// query and assign to the scope.
	if len(self.Let) > 0 {
		output_chan := make(chan Row)
		switch self.LetOperator {
		case "=":
			stored_query := NewStoredQuery(self.Query)
			scope.AppendVars(NewDict().Set(self.Let, stored_query))
		case "<=":
			scope.AppendVars(NewDict().Set(self.Let, Materialize(
				scope, self.Query)))
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
	result += "SELECT " + self.Query.SelectExpression.ToString(scope) +
		" FROM " + self.Query.From.ToString(scope)
	if self.Query.Where != nil {
		result += " WHERE " + self.Query.Where.ToString(scope)
	}

	if self.Query.OrderBy != nil {
		result += " ORDER BY " + *self.Query.OrderBy

		if self.Query.OrderByDesc != nil && *self.Query.OrderByDesc {
			result += " DESC "
		}
	}

	if self.Query.Limit != nil {
		result += fmt.Sprintf(
			" LIMIT %d ", int(*self.Query.Limit))
	}

	return result
}

// Provides a list of column names from this query. These columns will
// serve as Row keys for rows that are published on the output channel
// by Eval().
func (self *VQL) Columns(scope *Scope) *[]string {
	return self.Query.Columns(scope)
}

type _Select struct {
	SelectExpression *_SelectExpression `"SELECT " @@`
	From             *_From             `"FROM" @@`
	Where            *_CommaExpression  `[ "WHERE" @@ ]`
	OrderBy          *string            `[ "ORDER BY" @Ident `
	OrderByDesc      *bool              `  [@"DESC" | @"desc"] ]`
	Limit            *float64           `[ "LIMIT" @Number ]`
	GroupBy          *_CommaExpression  `[ "GROUPBY" @@ ]`
}

// Provides a list of column names from this query. These columns will
// serve as Row keys for rows that are published on the output channel
// by Eval().
func (self _Select) Columns(scope *Scope) *[]string {
	if self.SelectExpression.All {
		return self.From.Plugin.Columns(scope)
	}

	return self.SelectExpression.Columns(scope)
}

func (self _Select) ToString(scope *Scope) string {
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

	if self.OrderBy != nil {
		result += " ORDER BY " + *self.OrderBy

		if self.OrderByDesc != nil && *self.OrderByDesc {
			result += " DESC "
		}
	}

	return result
}

func (self _Select) Eval(ctx context.Context, scope *Scope) <-chan Row {
	output_chan := make(chan Row)
	from_chan := self.From.Eval(ctx, scope)

	if self.Limit != nil {
		go func() {
			defer close(output_chan)

			limit := int(*self.Limit)
			count := 1
			self.Limit = nil

			// Cancel the query when we hit the limit.
			sub_ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			for row := range self.Eval(sub_ctx, scope) {
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

		self.OrderBy = nil

		for row := range self.Eval(ctx, scope) {
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

				transformed_row := <-self.SelectExpression.Filter(
					ctx, scope, row)

				if self.Where == nil {
					output_chan <- transformed_row
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

					expression_chan := self.Where.Reduce(ctx, new_scope)
					expression, ok := <-expression_chan

					// If the filtered expression returns
					// a bool true, then pass the row to
					// the output.
					if ok && scope.Bool(expression) {
						output_chan <- transformed_row
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
	Right     *_AndExpression   ` "("@@ ")" | @@ )`
}

type _SelectExpression struct {
	All         bool                  `  @"*"`
	Expressions []*_AliasedExpression `| @@ { "," @@ }`
}

type _AliasedExpression struct {
	SubSelect  *_Select        `( "{" @@ "}" |`
	Expression *_AndExpression ` @@ )`

	As string `[ "AS " @Ident ]`
}

func (self _AliasedExpression) Reduce(ctx context.Context, scope *Scope) <-chan Any {
	if self.Expression != nil {
		return self.Expression.Reduce(ctx, scope)
	}

	if self.SubSelect != nil {
		output_chan := make(chan Any)

		go func() {
			defer close(output_chan)
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
				output_chan <- rows[0]
			} else {
				output_chan <- rows
			}
		}()
		return output_chan
	}

	return nil
}

func (self *_AliasedExpression) ToString(scope *Scope) string {
	if self.Expression != nil {
		result := self.Expression.ToString(scope)
		if self.As != "" {
			result += " AS " + self.As
		}
		return result

	} else if self.SubSelect != nil {
		result := self.SubSelect.ToString(scope)
		result = "{ " + result + " }"
		if self.As != "" {
			result += " AS " + self.As
		}
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
	Operator string          `@","`
	Term     *_AndExpression `@@`
}

// Expressions separated by AND.
type _AndExpression struct {
	Not   *_OrExpression `("NOT " @@ | `
	Left  *_OrExpression `@@`
	Right []*_OpAndTerm  `{ @@ })`
}

type _OpAndTerm struct {
	Operator string         `@"AND "`
	Term     *_OrExpression `@@`
}

// Expressions separated by OR
type _OrExpression struct {
	Left  *_ConditionOperand `@@`
	Right []*_OpOrTerm       `{ @@ }`
}

type _OpOrTerm struct {
	Operator string             `@"OR "`
	Term     *_ConditionOperand `@@`
}

// Conditional expressions imply comparison.
type _ConditionOperand struct {
	Left  *_AdditionExpression `@@`
	Right *_OpComparison       `{ @@ }`
}

type _OpComparison struct {
	Operator string               `@( "<>" | "<=" | ">=" | "=" | "<" | ">" | "!=" | "IN " | "=~")`
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
	Symbol     string   `@Ident`
	Parameters []*_Args `[ "(" [ @@ { "," @@ } ] ")" ]`
}

type _Value struct {
	Negated       bool              `[ @"-" | "+" ]`
	SymbolRef     *_SymbolRef       `( @@ `
	Subexpression *_CommaExpression `| "(" @@ ")"`
	Number        *float64          ` | @Number`
	String        *string           ` | @String`
	Boolean       *string           ` | @("TRUE" | "FALSE")`
	Null          bool              ` | @"NULL")`
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
func (self _SelectExpression) Filter(
	ctx context.Context, scope *Scope, row Row) <-chan Row {
	output_chan := make(chan Row)

	go func() {
		defer close(output_chan)

		// The select uses a * to relay all the rows without
		// filtering
		if self.All {
			output_chan <- row

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
			new_row := NewDict()
			new_scope := scope.Copy()
			new_scope.AppendVars(row)

			for _, expr := range self.Expressions {
				expression_chan := expr.Reduce(ctx, new_scope)
				expression, ok := <-expression_chan

				// If we fail to read we still need to
				// set something for that column.
				if !ok {
					expression = Null{}
				}

				var column_name string
				if expr.As != "" {
					column_name = expr.As
				} else {
					column_name = expr.ToString(scope)
				}
				new_row.Set(column_name, expression)
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
			result = append(result, expr.ToString(scope))
		}
	}
	return &result
}

func (self _SelectExpression) ToString(scope *Scope) string {
	if self.All {
		return "*"
	}
	var substrings []string
	for _, item := range self.Expressions {
		substrings = append(substrings, item.ToString(scope))
	}

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
					output_chan <- row
				}
			}
		}
	}()

	return output_chan
}

func (self _From) ToString(scope *Scope) string {
	result := self.Plugin.ToString(scope)
	return result
}

func (self _Plugin) getPlugin(scope *Scope, plugin_name string) (
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

func (self _Plugin) Eval(ctx context.Context, scope *Scope) <-chan Row {
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
					for {
						row, ok := <-from_chan
						if !ok {
							return
						}
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
				output_chan <- Null{}
			}
			return
		}

		// Build up the args to pass to the function.
		args := NewDict()
		for _, arg := range self.Args {
			if arg.Right != nil {
				value, ok := <-arg.Right.Reduce(ctx, scope)
				if !ok {
					return
				}
				args.Set(arg.Left, value)

			} else if arg.Array != nil {
				value, ok := <-arg.Array.Reduce(ctx, scope)
				if !ok {
					output_chan <- Null{}
					return
				}
				args.Set(arg.Left, value)

			} else if arg.SubSelect != nil {
				args.Set(arg.Left, arg.SubSelect)
			}
		}

		if plugin, pres := self.getPlugin(scope, self.Name); pres {
			plugin_chan := plugin.Call(ctx, scope, args)
			for {
				row, ok := <-plugin_chan
				if !ok {
					return
				}

				output_chan <- row
			}
		} else {
			scope.Log("Plugin %v not found", self.Name)
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
				for k, _ := range type_ref.Fields {
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

			fmt.Printf("Members of %v are %v.", value, scope.GetMembers(value))
			for _, item := range scope.GetMembers(value) {
				result = append(result, item)
			}
		}
	}

	return &result
}

func (self _Plugin) ToString(scope *Scope) string {
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

func (self _Args) ToString(scope *Scope) string {
	if self.Right != nil {
		return self.Left + "=" + self.Right.ToString(scope)
	} else if self.SubSelect != nil {
		return self.Left + "= { " + self.SubSelect.ToString(scope) + "}"
	} else if self.Array != nil {
		return self.Left + "= [" + self.Array.ToString(scope) + "]"
	}
	return ""
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
				output_chan <- Null{}
				return
			}

			for _, term := range self.Right {
				var pres bool
				lhs, pres = scope.Associative(lhs, term.Term)
				if !pres {
					output_chan <- Null{}
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
				output_chan <- Null{}
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
	if self.Not != nil {
		output_chan := make(chan Any)
		go func() {
			defer close(output_chan)

			select {
			case <-ctx.Done():
				return

			case value, ok := <-self.Not.Reduce(ctx, scope):
				if !ok {
					output_chan <- Null{}
					return
				}

				output_chan <- !scope.Bool(value)
			}
		}()
		return output_chan
	}

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
	if self.Not != nil {
		return " NOT " + self.Not.ToString(scope)
	}
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
				output_chan <- Null{}
				return
			}
		}

		select {
		case <-ctx.Done():
			return

		case rhs, ok = <-self.Right.Right.Reduce(ctx, scope):
			if !ok {
				output_chan <- Null{}
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
			output_chan <- Null{}
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
		args := NewDict()
		for _, arg := range self.Parameters {
			if arg.Right != nil {
				value, ok := <-arg.Right.Reduce(ctx, scope)
				if !ok {
					output_chan <- Null{}
					return
				}
				args.Set(arg.Left, value)

			} else if arg.Array != nil {
				value, ok := <-arg.Array.Reduce(ctx, scope)
				if !ok {
					output_chan <- Null{}
					return
				}
				args.Set(arg.Left, value)

			} else if arg.SubSelect != nil {
				args.Set(arg.Left, arg.SubSelect)
			}
		}

		// Lookup the symbol in the scope. Functions take
		// precedence over symbols.

		// The symbol is a function.
		if value, pres := scope.functions[self.Symbol]; pres {
			output_chan <- value.Call(ctx, scope, args)

			// The symbol is just a constant in the scope.
		} else if value, pres := scope.Resolve(self.Symbol); pres {
			output_chan <- value

		} else {
			scope.Log("Symbol %v not found. %s", self.Symbol,
				scope.PrintVars())
			output_chan <- Null{}
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
