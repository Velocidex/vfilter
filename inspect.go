/*
   Velociraptor - Dig Deeper
   Copyright (C) 2019-2025 Rapid7 Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Affero General Public License as published
   by the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU Affero General Public License for more details.

   You should have received a copy of the GNU Affero General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

package vfilter

import (
	"github.com/alecthomas/participle/v2/lexer"
)

// This file provides an exported, position-aware walker over the VQL AST.
//
// The grammar node types (like _Select, _AndExpression and _SymbolRef) are
// unexported, which makes it impossible for external packages to traverse
// the AST directly. The Inspect() function below is the sanctioned way for
// tooling (for example the VQL language server) to walk a parsed query and
// learn about plugin calls, function calls and argument positions.

// ArgInfo describes a single keyword argument to a plugin or function call.
type ArgInfo struct {
	// Name is the argument name (or "" for positional/array args).
	Name string
	// Pos is the position of the argument name.
	Pos lexer.Position
	// EndPos is the end of the argument expression.
	EndPos lexer.Position
}

// CallInfo describes a plugin or function invocation.
//
// Note: this shadows nothing in the package; the reformat-oriented
// CallInfo lives in visitor.go and is unrelated.
type CallInfo struct {
	// Name is the full dotted name (e.g. "Artifact.Windows.Sys.Users").
	Name string
	// IsPlugin is true for plugins in a FROM clause, false for function
	// calls in expressions.
	IsPlugin bool
	// Pos is the position of the name.
	Pos lexer.Position
	// EndPos is the end of the call (including the closing paren if present).
	EndPos lexer.Position
	// Args holds the keyword arguments.
	Args []ArgInfo
	// FreeForm is set if the callable accepts arbitrary keyword args.
	FreeForm bool
}

// SymbolInfo describes a reference to a bare symbol (a LET variable, a
// column name or a function name that was not resolved as a call).
type SymbolInfo struct {
	// Name is the symbol name.
	Name string
	// Pos is the position of the symbol.
	Pos lexer.Position
	// EndPos is the end of the symbol.
	EndPos lexer.Position
}

// LetInfo describes a LET statement.
type LetInfo struct {
	// Name is the LET variable name.
	Name string
	// Pos is the position of the variable name.
	Pos lexer.Position
}

// Inspection is the result of walking a VQL AST.
type Inspection struct {
	Calls   []CallInfo
	Symbols []SymbolInfo
	Lets    []LetInfo
}

// Inspect walks the AST of a parsed VQL statement and returns all plugin
// calls, function calls, symbol references and LET definitions with their
// source positions.
//
// The vql parameter may be nil.
func Inspect(vql *VQL) *Inspection {
	result := &Inspection{}
	if vql == nil {
		return result
	}

	if vql.Let != "" {
		result.Lets = append(result.Lets, LetInfo{
			Name: vql.Let,
			Pos:  vql.Pos,
		})
	}

	// LET X = SELECT ... or LET X = <expr>
	if vql.StoredQuery != nil {
		result.inspectSelect(vql.StoredQuery)
	}
	if vql.Expression != nil {
		result.inspectAndExpression(vql.Expression)
	}
	// Plain SELECT statement.
	if vql.Query != nil {
		result.inspectSelect(vql.Query)
	}

	return result
}

func (self *Inspection) inspectSelect(select_ *_Select) {
	if select_ == nil {
		return
	}

	// Column expressions.
	if select_.SelectExpression != nil {
		for _, column := range select_.SelectExpression.Expressions {
			self.inspectAliasedExpression(column)
		}
	}

	// FROM clause plugin.
	if select_.From != nil {
		plugin := &select_.From.Plugin
		if plugin.Name != "" {
			call := CallInfo{
				Name:     plugin.Name,
				IsPlugin: true,
				Pos:      plugin.Pos,
				EndPos:   plugin.EndPos,
			}
			for _, arg := range plugin.Args {
				call.Args = append(call.Args, self.argToSite(arg))
			}
			self.Calls = append(self.Calls, call)
		}
	}

	// WHERE clause.
	if select_.Where != nil {
		self.inspectCommaExpression(select_.Where)
	}

	// GROUP BY clause.
	if select_.GroupBy != nil {
		self.inspectCommaExpression(select_.GroupBy)
	}
}

func (self *Inspection) inspectAliasedExpression(expr *_AliasedExpression) {
	if expr == nil {
		return
	}
	if expr.SubSelect != nil {
		self.inspectSelect(expr.SubSelect)
	}
	if expr.Expression != nil {
		self.inspectAndExpression(expr.Expression)
	}
}

func (self *Inspection) argToSite(arg *_Args) ArgInfo {
	site := ArgInfo{
		Name:   arg.Left,
		Pos:    arg.Pos,
		EndPos: arg.EndPos,
	}
	if arg.SubSelect != nil {
		self.inspectSelect(arg.SubSelect)
	}
	if arg.Right != nil {
		self.inspectAndExpression(arg.Right)
	}
	if arg.Array != nil {
		self.inspectCommaExpression(arg.Array)
	}
	return site
}

func (self *Inspection) inspectCommaExpression(expr *_CommaExpression) {
	if expr == nil {
		return
	}
	self.inspectAndExpression(expr.Left)
	for _, term := range expr.Right {
		self.inspectAndExpression(term.Term)
	}
}

func (self *Inspection) inspectAndExpression(expr *_AndExpression) {
	if expr == nil {
		return
	}
	self.inspectOrExpression(expr.Left)
	for _, term := range expr.Right {
		self.inspectOrExpression(term.Term)
	}
}

func (self *Inspection) inspectOrExpression(expr *_OrExpression) {
	if expr == nil {
		return
	}
	self.inspectConditionOperand(expr.Left)
	for _, term := range expr.Right {
		self.inspectConditionOperand(term.Term)
	}
}

func (self *Inspection) inspectConditionOperand(expr *_ConditionOperand) {
	if expr == nil {
		return
	}
	if expr.Not != nil {
		self.inspectConditionOperand(expr.Not)
	}
	self.inspectAdditionExpression(expr.Left)
	if expr.Right != nil {
		self.inspectAdditionExpression(expr.Right.Right)
	}
}

func (self *Inspection) inspectAdditionExpression(expr *_AdditionExpression) {
	if expr == nil {
		return
	}
	self.inspectMultiplicationExpression(expr.Left)
	for _, term := range expr.Right {
		self.inspectMultiplicationExpression(term.Term)
	}
}

func (self *Inspection) inspectMultiplicationExpression(expr *_MultiplicationExpression) {
	if expr == nil {
		return
	}
	self.inspectMemberExpression(expr.Left)
	for _, term := range expr.Right {
		self.inspectValue(term.Factor)
	}
}

func (self *Inspection) inspectMemberExpression(expr *_MemberExpression) {
	if expr == nil {
		return
	}
	self.inspectValue(expr.Left)
	for _, term := range expr.Right {
		if term.Index != nil {
			self.inspectValue(term.Index)
		}
		if term.RangeEnd != nil {
			self.inspectValue(term.RangeEnd)
		}
	}
}

func (self *Inspection) inspectValue(value *_Value) {
	if value == nil {
		return
	}
	if value.SymbolRef != nil {
		self.inspectSymbolRef(value.SymbolRef)
	}
	if value.Subexpression != nil {
		self.inspectCommaExpression(value.Subexpression)
	}
}

func (self *Inspection) inspectSymbolRef(ref *_SymbolRef) {
	if ref == nil {
		return
	}
	if ref.Called {
		// A function call.
		call := CallInfo{
			Name:   ref.Symbol,
			Pos:    ref.Pos,
			EndPos: ref.EndPos,
		}
		for _, arg := range ref.Parameters {
			call.Args = append(call.Args, self.argToSite(arg))
		}
		self.Calls = append(self.Calls, call)
	} else {
		// A bare symbol reference (column, LET var, or unknown).
		self.Symbols = append(self.Symbols, SymbolInfo{
			Name:   ref.Symbol,
			Pos:    ref.Pos,
			EndPos: ref.EndPos,
		})
	}
}
