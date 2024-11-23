// A Visitor that visits the AST and serializes to string.

package vfilter

import (
	"fmt"
	"strconv"
	"strings"

	"www.velocidex.com/golang/vfilter/arg_parser"
	"www.velocidex.com/golang/vfilter/materializer"
	"www.velocidex.com/golang/vfilter/types"
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
		ArgsOnNewLine:        true,
		BreakLines:           true,
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
	ArgsOnNewLine    bool
	BreakLines       bool
	CollectCallSites bool
}

type CallSite struct {
	Type string
	Name string
	Args []string
}

type Visitor struct {
	CallSites []CallSite
	Fragments []string
	scope     types.Scope
	indents   []int

	// Fragment offsets for the line feeds
	line_breaks []int

	// Current position along the line.
	pos int

	opts FormatOptions

	// The max width of any of the lines in this visitor
	max_width int
	max_line  string

	has_comments bool
}

func NewVisitor(scope types.Scope, options FormatOptions) *Visitor {
	return &Visitor{
		scope:       scope,
		line_breaks: []int{0},
		opts:        options,
	}
}

// Merge results from the in visitor to this visitor.
func (self *Visitor) merge(in *Visitor) {
	self.Fragments = in.Fragments
	self.line_breaks = in.line_breaks
	self.indents = in.indents
	self.pos = in.pos
	self.max_width = in.max_width
}

func (self *Visitor) copy() *Visitor {
	return &Visitor{
		Fragments:   append([]string{}, self.Fragments...),
		scope:       self.scope,
		indents:     append([]int{}, self.indents...),
		line_breaks: append([]int{}, self.line_breaks...),
		pos:         self.pos,
		max_width:   self.max_width,
		opts:        self.opts,
	}
}

// Set current position as an indent point
func (self *Visitor) push_indent() {
	self.indents = append(self.indents, self.pos)
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
		case " ", "(", "{":
		default:
			self.push(" ")
		}
		return
	}

	// Ensure no trailing spaces
	if last_fragment == " " {
		self.pop()
	}

	last_indent := 0
	if len(self.indents) > 0 {
		last_indent = self.indents[len(self.indents)-1]
	}
	// Go back to start of the line
	self.push("\n")
	self.pos = 0
	self.line_breaks = append(self.line_breaks, len(self.Fragments)-1)
	self.push(strings.Repeat(" ", last_indent))
}

func (self *Visitor) current_line() string {
	if len(self.line_breaks) == 0 {
		return ""
	}

	line_fragment_idx := self.line_breaks[len(self.line_breaks)-1] + 1
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
			self.line_break()
		}

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
			self.Visit(c)
			self.line_break()
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

func (self *Visitor) visitComment(node *_Comment) {
	if node.Comment != nil {
		self.push(*node.Comment)
	}
	if node.VQLComment != nil {
		self.push(*node.VQLComment)
	}
	if node.MultiLine != nil {
		self.push(*node.MultiLine)
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

func (self *Visitor) visitAliasedExpression(node *_AliasedExpression) {
	node.mu.Lock()
	defer node.mu.Unlock()

	self.Visit(node.Comments)

	if node.Star != nil {
		self.push("*")
		return
	}

	if node.Expression != nil {
		visitor, longest_line, does_it_fit := doesNodeFitInOneLine(self, node.Expression)

		if node.As != "" {
			// Make sure we have enough room for the AS clause
			if does_it_fit && longest_line+3+len(node.As) < self.opts.MaxWidthThreshold {
				self.merge(visitor)
				self.push(" AS ", node.As)
				return
			}
			self.line_break()

			self.Visit(node.Expression)
			self.push(" AS ", node.As)
			return
		}

		// No AS Clause
		if does_it_fit {
			self.merge(visitor)
			return
		}
		self.line_break()

		self.Visit(node.Expression)
		return

	} else if node.SubSelect != nil {
		self.push("{", " ")
		self.indent_in()

		self.line_break()
		self.Visit(node.SubSelect)

		// Align closing } to previous block
		self.pop_indent()
		self.line_break()
		self.push("}")
		if node.As != "" {
			self.push(" AS ", node.As)
		}
	}
}

func (self *Visitor) visitSymbolRef(node *_SymbolRef) {
	node.mu.Lock()
	defer node.mu.Unlock()

	self.Visit(node.Comments)
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

	longest_arg := 0

	// See if we can fit the arg list in one line
	if !self.pluginUsesLineMode(node.Symbol) {
		visitor, longest_arg_, ok := doesArgListFitInOneLine(
			self, node.Parameters)
		if ok {
			self.merge(visitor)
			return
		}
		longest_arg = longest_arg_
	}

	// Nope we break into lines.
	self.push("(")

	// The width will be quite wide so we try to fit it a bit better
	// on a new line by indenting 2 spots from the start of the block.
	if self.opts.ArgsOnNewLine ||
		self.pluginUsesLineMode(node.Symbol) {
		self.indent_in()
		self.line_break()

	} else if self.pos+longest_arg > self.opts.MaxWidthThreshold {
		self.indent_in()
		self.line_break()

	} else {
		// Otherwise try to line up on the ( because it looks neater.
		self.push_indent()
	}

	defer self.pop_indent()

	for idx, arg := range node.Parameters {
		if idx > 0 && self.opts.ArgsOnNewLine {
			self.line_break()
		}

		self.Visit(arg)
		if idx < len(node.Parameters)-1 {
			self.push(",", " ")
		}
	}

	self.push(")")
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

		// If we are not breaking lines we dont adds the comment at
		// all.
		if self.opts.BreakLines {
			self.Visit(node.Comments)
		}
	}

	if node.Right != nil {
		self.push(node.Left, "=")
		self.Visit(node.Right)

	} else if node.SubSelect != nil {
		self.push(node.Left, "={")
		self.indent_in()

		self.line_break()
		self.Visit(node.SubSelect)

		// Align closing } to previous block
		self.pop_indent()
		self.line_break()
		self.push("}")

	} else if node.Array != nil {
		self.push(node.Left, "=[")
		self.Visit(node.Array)
		self.push("]")

	} else if node.ArrayOpenBrace != "" {
		self.push(node.Left, "=[]")
	}
}

// Some special plugins will always use line mode
func (self *Visitor) pluginUsesLineMode(name string) bool {
	switch name {
	case "foreach", "if":
		return true
	}
	return false
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

		longest_arg := 0

		if !self.pluginUsesLineMode(node.Name) {
			// Check if the arg list is going to fit on the current
			// line.
			//
			// We need to format the args in the plugin args. There
			// are 3 formatting styles:
			//
			// 1. All args fit on the same line.
			//    SELECT * FROM plugin(A="A", B="B")
			//
			// 2. There are many args but they are generally short. We
			//    format them one on each line lining up with the
			//    opening brace.
			//    SELECT * FROM plugin(A="A",
			//                         B="B",
			//                         C="C")
			//
			// 3. One of the args is so long that the line will
			//    overflow, in that case we move the indent point
			//    relative to the block start.
			//    SELECT * FROM plugin(
			//      A="A",
			//      B="Very long arg",
			//      C="C")
			//
			// The following code figures out which style is
			// appropriate by calculating:
			// * How long would the arg list be if formatted on the same line?
			// * What is the length of each arg if formatted on its own?
			//
			// We do this by trying to format the args into a single
			// line with a new visitor. This is effectively a
			// lookahead/backtracking algorithm.
			visitor, longest_arg_, ok := doesArgListFitInOneLine(
				self, node.Args)
			if ok {
				// The args all fit in the same line, just merge the
				// visitor
				self.merge(visitor)
				return
			}

			longest_arg = longest_arg_
		}

		self.push("(")

		// The block will be very wide so we break it into a smaller
		// block
		if self.pluginUsesLineMode(node.Name) ||
			self.pos+longest_arg > self.opts.MaxWidthThreshold {
			self.indent_in()
			self.line_break()

		} else {
			// Otherwise try to line up on the (
			self.push_indent()
		}
		defer self.pop_indent()

		// Write args one per line
		for idx, arg := range node.Args {
			if idx > 0 {
				// First arg inline
				self.line_break()
			}
			self.Visit(arg)
			if idx < len(node.Args)-1 {
				self.push(",", " ")
			}
		}
		self.push(")")
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
		self.Visit(item)
		// No trailing , in the last element.
		if idx < len(node.Expressions)-1 {
			self.push(",", " ")
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
	self.push_indent()

	if node.SelectExpression != nil {
		self.Visit(node.SelectExpression)
	}
	self.pop_indent()

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
		self.Visit(node.GroupBy)
		self.pop_indent()
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

func (self *Visitor) push(fragments ...string) {
	for _, i := range fragments {
		self.Fragments = append(self.Fragments, i)
		self.pos += len(i)
		if self.max_width < self.pos {
			self.max_width = self.pos
			self.max_line = self.current_line()
		}
	}
}

func (self *Visitor) pop() {
	if len(self.Fragments) > 0 {
		self.Fragments = self.Fragments[:len(self.Fragments)-1]
	}
}

func (self *Visitor) visitVQL(node *VQL) {
	self.Visit(node.Comments)

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

func FormatToString(scope types.Scope, node interface{}) string {
	visitor := NewVisitor(scope, ToStringOptions)
	visitor.Visit(node)
	return visitor.ToString()
}

func doesArgListFitInOneLine(self *Visitor, args []*_Args) (
	result *Visitor, longest_arg int, does_it_fit bool) {

	// It is not going to fit on the line at all
	if self.pos > self.opts.MaxWidthThreshold {
		return self, self.pos, false
	}

	// make a copy of the visitor and try to write all the args on it.
	result = self.copy()
	result.opts.BreakLines = false

	// Write all the args on the one line
	result.push("(")
	for idx, arg := range args {
		start := result.pos
		result.Visit(arg)
		if idx < len(args)-1 {
			result.push(",", " ")
		}
		arg_len := result.pos - start
		if arg_len > longest_arg {
			longest_arg = arg_len
		}
	}
	result.push(")")

	// Check if the width exceeds the recommended size
	does_it_fit = !result.has_comments &&
		result.max_width < self.opts.MaxWidthThreshold &&
		len(result.line_breaks) == len(self.line_breaks)

	// Comments need to take the entire line.
	if result.has_comments {
		longest_arg = self.opts.MaxWidthThreshold
	}

	return result, longest_arg, does_it_fit
}

func doesNodeFitInOneLine(self *Visitor, node interface{}) (
	result *Visitor, longest_line int, does_it_fit bool) {

	// We already overflow it can not fit.
	if self.pos > self.opts.MaxWidthThreshold {
		return self, self.pos, false
	}

	// make a copy of the visitor and try to write all the args on it.
	result = self.copy()
	result.opts.BreakLines = false
	result.opts.ArgsOnNewLine = false
	result.Visit(node)

	does_it_fit = !result.has_comments &&
		result.max_width < self.opts.MaxWidthThreshold &&
		len(result.line_breaks) == len(self.line_breaks)

	// Comments need to take the entire line.
	if result.has_comments {
		result.max_width = self.opts.MaxWidthThreshold
	}

	return result, result.max_width, does_it_fit
}
