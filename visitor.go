// A Visitor that visits the AST and serializes to string.

package vfilter

import (
	"fmt"
	"strconv"
	"strings"

	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/materializer"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

var (
	ToStringOptions = FormatOptions{
		BreakLines:        false,
		MaxWidthThreshold: 1000000,
		CollectCallSites:  false,
	}

	DefaultFormatOptions = FormatOptions{
		IndentWidthThreshold: 50,
		MaxWidthThreshold:    80,

		// If false we dont break lines at all - all tokens are
		// written on the same line.
		BreakLines: true,
	}

	CollectCallSites = FormatOptions{
		CollectCallSites: true,
	}
)

type FormatOptions struct {
	// Threshold above which we indent more aggresively on new
	// lines. Below the threshold we try to keep lines together.
	IndentWidthThreshold int
	MaxWidthThreshold    int

	// Parameters are layed one on each line and indent at the first (
	BreakLines       bool
	CollectCallSites bool

	// Set when we do a test reformat to try to lookahead.
	test bool
}

type CallSite struct {
	Type string
	Name string
	Args []string
}

type Visitor struct {
	CallSites []CallSite

	// Tokens added to the visitor as we encounter each token during
	// parsing. Combining all the Fragments yields a reformatted
	// query.
	Fragments []string
	scope     types.Scope

	// A list of indent points at each line break. Acts as a stack
	// when an indent pushes a new indent point and an unindent pops
	// it.
	indents []int

	// Fragment offsets for the line feeds. Each represent the offset
	// in the Fragment array where a line feed is inserted. Total
	// length of this array represents the total number of lines.
	line_breaks []int

	// Current position along the line.
	pos int

	opts FormatOptions

	// The max width of any of the lines in this visitor
	max_width int
	max_line  string

	// Flag set when a comment is encountered.
	has_comments bool
}

func NewVisitor(scope types.Scope, options FormatOptions) *Visitor {
	return &Visitor{
		scope:       scope,
		line_breaks: []int{0},
		opts:        options,
		indents:     []int{0},
	}
}

// Merge results from the in visitor to this visitor.
func (self *Visitor) merge(in *Visitor) {
	self.Fragments = append([]string{}, in.Fragments...)
	self.line_breaks = in.line_breaks
	self.indents = append([]int{}, in.indents...)
	self.pos = in.pos
	self.max_width = in.max_width
	self.max_line = in.max_line
	self.has_comments = in.has_comments
}

func (self *Visitor) copy() *Visitor {
	opts_copy := self.opts

	return &Visitor{
		Fragments:   append([]string{}, self.Fragments...),
		scope:       self.scope,
		indents:     append([]int{}, self.indents...),
		line_breaks: append([]int{}, self.line_breaks...),
		pos:         self.pos,
		max_width:   self.max_width,
		max_line:    self.max_line,
		opts:        opts_copy,
	}
}

// Set current position as an indent point
func (self *Visitor) push_indent() {
	self.indents = append(self.indents, self.pos)
}

// Add a line break and reset pos and indent to start of the line.
func (self *Visitor) new_line(pos int) {
	self.indents = append(self.indents, pos)
	self.pos = pos

	if self.opts.BreakLines {
		self.Fragments = append(self.Fragments, "\n")
		self.line_breaks = append(self.line_breaks, len(self.Fragments))
		self.Fragments = append(self.Fragments, strings.Repeat(" ", pos))

		self.checkInvariant()
	}
}

// Indent in from the last indent point.
func (self *Visitor) indent_in() {
	last_indent := 0
	if len(self.indents) > 0 {
		last_indent = self.indents[len(self.indents)-1]
	}

	self.indents = append(self.indents, last_indent+2)
}

func (self *Visitor) pop_indent() {
	if len(self.indents) > 0 {
		last_indent := self.indents[len(self.indents)-1]
		self.indents = self.indents[:len(self.indents)-1]
		_ = last_indent
	}
}

// Add a line break at this point with an indent up to the last indent
// point.
func (self *Visitor) line_break() {
	last_fragment := ""
	if len(self.Fragments) > 0 {
		last_fragment = self.Fragments[len(self.Fragments)-1]
	}

	// Format all on the same line. Strictly we dont actually need
	// spaces but we add them for readability.
	if !self.opts.BreakLines {
		switch last_fragment {
		// Do not follow these with a space.
		case " ", "(", "{", "=", "[", "=[":
		default:
			if !strings.HasSuffix(last_fragment, " ") {
				self.push(" ")
			}
		}
		return
	}

	// Ensure no trailing spaces
	if strings.TrimSpace(last_fragment) == "" {
		self.pop()
	}

	last_indent := 0
	if len(self.indents) > 0 {
		last_indent = self.indents[len(self.indents)-1]
	}

	// Go back to start of the line
	self.pos = 0
	self.Fragments = append(self.Fragments, "\n")
	self.line_breaks = append(self.line_breaks, len(self.Fragments))
	self.push(strings.Repeat(" ", last_indent))
}

func (self *Visitor) current_line() string {
	if len(self.line_breaks) == 0 {
		return ""
	}

	line_fragment_idx := self.line_breaks[len(self.line_breaks)-1]
	if len(self.Fragments) < line_fragment_idx {
		return ""
	}
	return strings.Join(self.Fragments[line_fragment_idx:], "")
}

func (self *Visitor) ToString() string {
	return strings.Join(self.Fragments, "")
}

func (self *Visitor) Visit(node interface{}) {
	switch t := node.(type) {
	case []*VQL:
		for _, vql := range t {
			self.visitVQL(vql)

			// Leave an empty line between each VQL statement.
			self.line_break()
		}

	case *types.FrozenStoredQuery:
		self.Visit(t.Query())

	case *_StoredQuery:
		self.Visit(t.query)

	case *string:
		if t != nil {
			self.push(*t)
		}

	case *_Comment:
		self.visitComment(t)

	case []*_Comment:
		for _, c := range t {
			self.visitComment(c)
		}

	case *VQL:
		self.visitVQL(t)

	case *_Select:
		self.visitSelect(t)

	case *_SelectExpression:
		self.visitSelectExpression(t)

	case *_From:
		self.visitPlugin(&t.Plugin)

	case *Plugin:
		self.visitPlugin(t)

	case *_Args:
		self.visitArgs(t)

	case *_MemberExpression:
		self.visitMemberExpression(t)

	case *_CommaExpression:
		self.visitCommaExpression(t)

	case *_MultiplicationExpression:
		self.visitMultiplicationExpression(t)

	case *_AdditionExpression:
		self.visitAdditionExpression(t)

	case *_ConditionOperand:
		self.visitConditionOperand(t)

	case *_OrExpression:
		self.visitOrExpression(t)

	case *_AndExpression:
		self.visitAndExpression(t)

	case *_Value:
		self.visitValue(t)

	case *_SymbolRef:
		self.visitSymbolRef(t)

	case *_ParameterList:
		self.visitParameterList(t)

	case *_AliasedExpression:
		self.visitAliasedExpression(t)

	case *Lambda:
		self.visitLambda(t)

	case *LazyExprImpl:
		self.Visit(t.Expr)

	case *StoredExpression:
		self.visitStoredExpression(t)

	case types.StringProtocol:
		self.push(t.ToString(self.scope))

	case *arg_parser.StoredQueryWrapperLazyExpression:
		self.Visit(t.Delegate())

	case *arg_parser.LazyExpressionWrapper:
		self.Visit(t.Delegate())

	case *materializer.InMemoryMatrializer:
		return

	default:
		self.scope.Log("FormatToString: Unable to visit %T", node)
	}
}

func (self *Visitor) visitStoredExpression(node *StoredExpression) {
	self.Visit(node.Expr)
}

// Comments must be on their own lines
func (self *Visitor) visitComment(node *_Comment) {
	// C Style comments start with // and follow the arg.
	if node.Comment != nil {
		self.push(*node.Comment)
		self.line_break()
	}

	// VQL Comments start with -- and generally follow the thing they
	// are commenting. We must break line after wards to tell the
	// parser to switch back to VQL mode.
	if node.VQLComment != nil {
		self.push(*node.VQLComment)
		self.line_break()
	}

	// Multi line comments always start at column 0 and follow by a
	// line break.
	if node.MultiLine != nil {
		self.new_line(0)
		defer self.pop_indent()

		self.push(*node.MultiLine)
		self.line_break()
	}
}

func (self *Visitor) visitLambda(node *Lambda) {
	self.Visit(node.Parameters)
	self.push(" => ")
	self.Visit(node.Expression)
}

func (self *Visitor) visitParameterList(node *_ParameterList) {
	self.push(node.Left)

	if node.Right != nil {
		self.push(", ")
		self.Visit(node.Right.Term)
	}
}

func (self *Visitor) isCurrentLineSpace() bool {
	return strings.TrimSpace(self.current_line()) == ""
}

// Aliased expressions are elements after the SELECT separated by ,
func (self *Visitor) visitAliasedExpression(node *_AliasedExpression) {
	node.mu.Lock()
	defer node.mu.Unlock()

	self.Visit(node.Comments)

	if node.Star != nil {
		self.push("*")
		return
	}

	pos := node.pos

	if node.Expression != nil {
		// We have two choices here - fit the expression on the
		// current line, or break lines
		if node.As != "" {
			node_as := node.As
			self.abTest("visitAliasedExpression with AS", node.Expression,
				// Fit expression on one line.
				func(self *Visitor) {
					self.opts.BreakLines = false
					self.Visit(node.Expression)

					self.push(" AS ", node_as)
				},
				func(self *Visitor) {
					// Break lines but not for the first arg after
					// select. This ensures that something follows
					// SELECT.
					if pos > 0 && !self.isCurrentLineSpace() {
						self.line_break()
					}

					self.Visit(node.Expression)
					self.push(" AS ", node_as)
				})
			return
		}

		// No AS clause
		self.abTest("visitAliasedExpression no AS", node.Expression,
			// Fit expression on one line.
			func(self *Visitor) {
				self.opts.BreakLines = false
				self.Visit(node.Expression)
			},
			func(self *Visitor) {
				// Break lines
				if !self.isCurrentLineSpace() {
					self.line_break()
				}
				self.Visit(node.Expression)
			})
		return

	} else if node.SubSelect != nil {
		self.push("{", " ")

		// We prefer the subquery to start at the begining of the line
		// with a little indent.
		if self.opts.BreakLines {
			self.new_line(2)
			defer self.pop_indent()

			self.indent_in()
			defer self.pop_indent()

			self.push("  ")
			self.Visit(node.SubSelect)

			// Align closing } to previous block
			self.line_break()
			self.push("}")

		} else {
			self.Visit(node.SubSelect)
			self.push(" }")
		}

		if node.As != "" {
			self.push(" AS ", node.As)
		}
	}
}

func (self *Visitor) visitSymbolRef(node *_SymbolRef) {
	node.mu.Lock()
	defer node.mu.Unlock()

	self.Visit(node.Comments)

	if self.pos > self.opts.IndentWidthThreshold {
		self.indent_in()
		defer self.pop_indent()

		self.line_break()
	}

	self.push(node.Symbol)
	if !node.Called && node.Parameters == nil {
		return
	}

	if node.Called && self.opts.CollectCallSites {
		callsite := CallSite{
			Type: "function",
			Name: node.Symbol,
		}

		for _, p := range node.Parameters {
			callsite.Args = append(callsite.Args, p.Left)
		}
		self.CallSites = append(self.CallSites, callsite)
	}

	// No parameters anyway.
	if len(node.Parameters) == 0 {
		self.push("()")
		return
	}

	// Here have two choices:
	// 1. Try to fit the entire arg list on the same line.
	// 2. Break each arg on its own line.
	self.abTest("visitSymbolRef", node,
		// Fit everything on the one line.
		func(self *Visitor) {
			self.opts.BreakLines = false

			// Write all the args on the one line
			self.push("(")
			for idx, arg := range node.Parameters {
				self.Visit(arg)
				// Do not add , after the last parameter
				if idx < len(node.Parameters)-1 {
					self.push(",", " ")
				}
			}
			self.push(")")
		},

		// Here we have a couple of choices:
		// Write the args starting immediately after the ( - for example
		//    plugin(Arg1=Foo
		//           Arg2=Bar)
		//
		func(self *Visitor) {
			self.push("(")
			self.push_indent()
			defer self.pop_indent()

			for idx, arg := range node.Parameters {
				if idx > 0 {
					self.line_break()
				}

				self.Visit(arg)
				if idx < len(node.Parameters)-1 {
					self.push(",", " ")
				}
			}

			self.push(")")
		},

		// Add another line break and indent a bit after the name of the plugin:
		//    plugin(
		//      Arg1=Foo,
		//      Arg2=Bar)
		func(self *Visitor) {
			self.push("(")

			self.indent_in()
			defer self.pop_indent()

			self.line_break()

			for idx, arg := range node.Parameters {
				if idx > 0 {
					self.line_break()
				}

				self.Visit(arg)
				if idx < len(node.Parameters)-1 {
					self.push(",", " ")
				}
			}

			self.push(")")
		})
}

func (self *Visitor) visitAndExpression(node *_AndExpression) {
	self.Visit(node.Left)

	for _, right := range node.Right {
		self.line_break()
		self.push(" ", right.Operator, " ")
		self.push_indent()
		defer self.pop_indent()

		self.Visit(right.Term)
	}
}

func (self *Visitor) visitOrExpression(node *_OrExpression) {
	self.Visit(node.Comments)
	self.Visit(node.Left)

	for _, right := range node.Right {
		self.push(" ", right.Operator, " ")
		self.Visit(right.Term.Comments)
		self.Visit(right.Term)
	}
}

func (self *Visitor) visitConditionOperand(node *_ConditionOperand) {
	if node.Not != nil {
		self.push("NOT ")
		self.Visit(node.Not)
		return
	}

	self.Visit(node.Left)
	if node.Right != nil {
		self.push(" ", node.Right.Operator, " ")
		self.Visit(node.Right.Right)
	}
}

func (self *Visitor) visitAdditionExpression(node *_AdditionExpression) {
	self.Visit(node.Comments)
	self.Visit(node.Left)
	for _, right := range node.Right {
		self.push(" ", right.Operator, " ")
		self.Visit(right.Term)
	}
}

func (self *Visitor) visitMultiplicationExpression(node *_MultiplicationExpression) {
	self.Visit(node.Comments)
	self.Visit(node.Left)
	if len(node.Right) == 0 {
		return
	}

	for _, right := range node.Right {
		self.push(" ", right.Operator, " ")
		self.Visit(right.Factor)
	}
}

func (self *Visitor) visitValue(node *_Value) {
	node.mu.Lock()

	self.Visit(node.Comments)
	node.maybeParseStrNumber(self.scope)

	factor := 1.0
	if node.Negated {
		factor = -1.0
	}

	symbolref := node.SymbolRef
	if symbolref != nil {
		node.mu.Unlock()
		self.Visit(symbolref)
		return
	}

	subexpression := node.Subexpression
	if subexpression != nil {
		node.mu.Unlock()
		self.push("(")
		self.indent_in()
		defer self.pop_indent()

		self.Visit(subexpression)
		self.push(")")
		return
	}

	if node.String != nil {
		self.push(*node.String)
		node.mu.Unlock()
		return
	}

	if node.Int != nil {
		factor := int64(1)
		if node.Negated {
			factor = -1
		}
		self.push(strconv.FormatInt(factor**node.Int, 10))
		node.mu.Unlock()
		return

	}

	if node.Float != nil {
		result := strconv.FormatFloat(factor**node.Float, 'f', -1, 64)
		if !strings.Contains(result, ".") {
			result = result + ".0"
		}
		self.push(result)
		node.mu.Unlock()
		return
	}

	if node.Boolean != nil {
		self.push(*node.Boolean)
		node.mu.Unlock()
		return

	}

	if node.Null {
		node.mu.Unlock()
		self.push("NULL")
		return
	}

	node.mu.Unlock()
	self.push("FALSE")
}

func (self *Visitor) visitCommaExpression(node *_CommaExpression) {
	self.Visit(node.Comments)
	self.Visit(node.Left)
	for _, right := range node.Right {
		self.push(",", " ")

		self.Visit(right.Comments)
		self.Visit(right.Comment2)
		if right.Term != nil {
			self.Visit(right.Term)
		}
	}
}

func (self *Visitor) visitMemberExpression(node *_MemberExpression) {
	self.Visit(node.Comments)
	self.Visit(node.Left)

	for _, right := range node.Right {
		if right.Range != nil {
			self.push("[")
			if right.Index != nil {
				self.Visit(right.Index)
			}

			self.push(":")

			if right.RangeEnd != nil {
				self.Visit(right.RangeEnd)
			}
			self.push("]")

		} else if right.Index != nil {
			self.push("[")
			self.Visit(right.Index)
			self.push("]")

		} else {
			self.push(".")
			self.Visit(right.Term)
		}
	}
}

func (self *Visitor) visitArgs(node *_Args) {
	if self.pos > self.opts.IndentWidthThreshold {
		self.line_break()
	}

	if node.Comments != nil {
		self.has_comments = true

		// We encountered a comment - we have to break lines from now
		// on so we can preserve the comments.
		self.opts.BreakLines = true
		self.Visit(node.Comments)
	}

	if node.Right != nil {
		self.push(node.Left, "=")
		self.Visit(node.Right)

	} else if node.SubSelect != nil {
		self.push(node.Left, "={")

		// We prefer subquery to start at the begining of the line
		// with a small indent.
		if self.opts.BreakLines {
			self.new_line(2)
			self.indent_in()
			defer self.pop_indent()

			self.push("  ")
			self.Visit(node.SubSelect)

			// Align closing } to previous block
			self.pop_indent()
			self.line_break()
			self.push("}")

		} else {
			self.push(" ")
			self.Visit(node.SubSelect)
			self.push(" }")
		}

	} else if node.Array != nil {
		self.push(node.Left, "=[")
		self.Visit(node.Array)
		self.push("]")

	} else if node.ArrayOpenBrace != "" {
		self.push(node.Left, "=[]")
	}
}

func (self *Visitor) visitPlugin(node *Plugin) {
	self.push(node.Name)
	if node.Call {

		// Collect callsites if needed.
		if self.opts.CollectCallSites {
			callsite := CallSite{
				Type: "plugin",
				Name: node.Name,
			}
			for _, arg := range node.Args {
				callsite.Args = append(callsite.Args, arg.Left)
			}
			self.CallSites = append(self.CallSites, callsite)
		}

		// No parameters anyway.
		if len(node.Args) == 0 {
			self.push("()")
			return
		}

		// Here have two choices:
		// 1. Try to fit the entire arg list on the same line.
		// 2. Break each arg on its own line.
		self.abTest("visitPlugin", node,

			// Fit everything on the one line.
			func(self *Visitor) {
				self.opts.BreakLines = false

				// Write all the args on the one line
				self.push("(")
				for idx, arg := range node.Args {
					self.Visit(arg)
					// Do not add , after the last parameter
					if idx < len(node.Args)-1 {
						self.push(",", " ")
					}
				}
				self.push(")")

			},

			// Here we have a couple of choices:
			// 1. Write the args starting immediately after the ( - for example
			//    plugin(Arg1=Foo
			//           Arg2=Bar)
			//
			func(self *Visitor) {
				self.push("(")

				self.push_indent()
				defer self.pop_indent()

				for idx, arg := range node.Args {
					if idx > 0 {
						self.line_break()
					}

					self.Visit(arg)
					if idx < len(node.Args)-1 {
						self.push(",", " ")
					}
				}

				self.push(")")
			},

			// Add another line break and indent a bit after the name of the plugin:
			//    plugin(
			//      Arg1=Foo,
			//      Arg2=Bar)
			func(self *Visitor) {
				self.push("(")

				self.indent_in()
				defer self.pop_indent()

				self.line_break()

				for idx, arg := range node.Args {
					if idx > 0 {
						self.line_break()
					}

					self.Visit(arg)
					if idx < len(node.Args)-1 {
						self.push(",", " ")
					}
				}

				self.push(")")
			})
	}
}

func (self *Visitor) visitSelectExpression(node *_SelectExpression) {
	if node.All {
		self.push("*")
		if len(node.Expressions) > 0 {
			self.push(",", " ")
		}
	}

	for idx, item := range node.Expressions {
		item.pos = idx

		self.Visit(item)
		// No trailing , in the last element.
		if idx < len(node.Expressions)-1 {
			self.push(",", " ")

			// We want each expression on its own line.
			self.line_break()
		}
	}
}

func (self *Visitor) visitSelect(node *_Select) {
	self.Visit(node.Comments)

	if node.Explain != nil {
		self.push("EXPLAIN ")
	}

	self.push("SELECT ")

	if node.SelectExpression != nil {
		// We need to make a choice here:
		// 1. Put expression after SELECT
		// 2. Break line and put expression near the start of line.
		self.abTest("SELECT expression",
			node.SelectExpression,

			// Render after the SELECT
			func(self *Visitor) {
				self.push_indent()
				defer self.pop_indent()

				self.Visit(node.SelectExpression)
			},

			// Start on a new line.
			func(self *Visitor) {
				self.new_line(2)
				defer self.pop_indent()

				self.indent_in()
				defer self.pop_indent()

				self.push("  ")
				self.Visit(node.SelectExpression)
			})
	}

	if node.From != nil {
		self.line_break()
		self.push("FROM ")
		self.Visit(node.From)
	}

	if node.Where != nil {
		self.line_break()
		self.push("WHERE ")
		self.Visit(node.Where)
	}

	if node.GroupBy != nil {
		self.line_break()
		self.push("GROUP BY ")
		self.push_indent()
		defer self.pop_indent()

		self.Visit(node.GroupBy)
	}

	if node.OrderBy != nil {
		self.line_break()
		self.push("ORDER BY ", *node.OrderBy)

		if node.OrderByDesc != nil && *node.OrderByDesc {
			self.push(" DESC ")
		}
	}

	if node.Limit != nil {
		self.line_break()
		self.push(fmt.Sprintf("LIMIT %d ", int(*node.Limit)))
	}
}

// At any point the following invariants hold:

//  1. self.pos represents the current cursor position. It must be at
//     the end of the current line.
//  2. self.max_width must be the equal to the length of self.max_line
func (self *Visitor) checkInvariant() {
	// self.pos must line up with the current line offset.
	current_line := self.current_line()
	if len(current_line) != self.pos ||
		len(self.max_line) != self.max_width {
		utils.DebugPrint("ERROR: Invariant failed!\n")
	}
}

// Push fragments into the fragment queue.
func (self *Visitor) push(fragments ...string) {
	for _, i := range fragments {
		self.Fragments = append(self.Fragments, i)
		self.pos += len(i)

		// Line has overrun the max width, break the line.
		if self.max_width < self.pos {
			self.max_width = self.pos
			self.max_line = self.current_line()
		}
		self.checkInvariant()
	}
}

func (self *Visitor) pop() {
	if len(self.Fragments) > 0 {
		last_fragment := self.Fragments[len(self.Fragments)-1]
		self.Fragments = self.Fragments[:len(self.Fragments)-1]
		self.pos -= len(last_fragment)
	}
}

// Represents a VQL statement.
func (self *Visitor) visitVQL(node *VQL) {
	self.Visit(node.Comments)

	// When we finish here we reset the state of the renderer to the
	// start as each VQL statement is rendered independently.
	defer func() {
		self.line_break()
		self.pos = 0
		self.max_line = ""
		self.max_width = 0
	}()

	if node.Let != "" {
		operator := " = "
		if node.LetOperator != "" {
			operator = node.LetOperator
		}

		if node.Expression != nil || node.StoredQuery != nil {
			self.push("LET ", node.Let)
			if node.Parameters != nil {
				self.push("(")
				parameters := node.getParameters()
				for idx, p := range parameters {
					self.push(p)
					if idx < len(parameters)-1 {
						self.push(",", " ")
					}
				}
				self.push(")")
			}
		}
		self.push(" ", operator, " ")
		self.indent_in()
		defer self.pop_indent()

		if node.Expression != nil {
			self.Visit(node.Expression)
			return
		}

		if node.StoredQuery != nil {
			self.Visit(node.StoredQuery)
			return
		}
	}

	if node.Query != nil {
		self.Visit(node.Query)
	}
}

// Try to format the node both ways and select the better one (based on is_better())
func (self *Visitor) abTest(
	name string,
	node interface{},
	cbs ...func(self *Visitor),
) {
	var res *Visitor

	if utils.IsDebug() {
		fmt.Printf("%v: Performing abTest on %v\n", name, self.ToString())
	}

	for idx, cb := range cbs {
		test_visitor := self.copy()
		test_visitor.opts.test = true

		cb(test_visitor)

		if utils.IsDebug() {
			fmt.Printf("%v: Test %d:\n%v\n", name, idx, test_visitor.ToString())

			if len(test_visitor.indents) != len(self.indents) {
				utils.DebugPrint("Unbalanced indents")
			}
		}

		// When not in reformat mode we really dont care which option
		// we choose.
		if !self.opts.BreakLines {
			self.merge(test_visitor)
			return
		}

		if res == nil {
			res = test_visitor

		} else if res.is_better(test_visitor) {
			res.merge(test_visitor)
		}
	}

	if res != nil {
		self.merge(res)
	}

	if utils.IsDebug() {
		fmt.Printf("%v: Selected:\n%v\n", name, self.ToString())
	}
}

// Is the other visitor better than this one?
func (self *Visitor) is_better(other *Visitor) bool {
	// Most important priority is to ensure we dont exceed the max
	// width by much.
	/*
		fmt.Printf("self %v (%v lines)\n%v\n - other %v (%v lines)\n%v\n",
			self.max_width, len(self.line_breaks), self.max_line,
			other.max_width, len(other.line_breaks), other.max_line)
	*/
	max_width := self.opts.MaxWidthThreshold

	// If the other formatting exceeds the max line number but we dont
	// then reject it.
	if other.max_width > max_width && self.max_width < max_width {
		return false
	}

	// If we are too wide but the other is not then the other is
	// better.
	if self.max_width > max_width && other.max_width < max_width {
		return true
	}

	// If both widths are ok, the best one is the one with less lines.
	if self.max_width < max_width &&
		other.max_width < max_width &&
		len(other.line_breaks) > len(self.line_breaks) {
		return false
	}

	// If the other is wider then it is not better
	if other.max_width > self.max_width {
		return false
	}

	return true
}

func FormatToString(scope types.Scope, node interface{}) string {
	visitor := NewVisitor(scope, ToStringOptions)
	visitor.Visit(node)
	return strings.TrimSpace(visitor.ToString())
}
