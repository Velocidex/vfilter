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

// This file provides a hierarchical outline of a parsed VQL statement,
// suitable for building LSP document symbols (the outline shown in an
// editor's symbol navigator).
//
// Like Inspect() it is exported because the grammar node types are
// unexported; Outline() is the sanctioned way for tooling to learn about
// the structure of a query.

// OutlineKind classifies an outline entry.
type OutlineKind string

const (
	// OutlineKindLet is a LET variable definition.
	OutlineKindLet OutlineKind = "let"
	// OutlineKindQuery is a SELECT statement (or subquery).
	OutlineKindQuery OutlineKind = "query"
	// OutlineKindColumn is a column expression in a SELECT list.
	OutlineKindColumn OutlineKind = "column"
	// OutlineKindFunction is a function call inside an expression.
	OutlineKindFunction OutlineKind = "function"
)

// OutlineInfo is one node in a document outline.
type OutlineInfo struct {
	// Name is a human readable name for the symbol. For a LET this is the
	// variable name; for a query it is the plugin name; for a column it is
	// the alias if one was given, otherwise "" (the caller can extract the
	// source text from the document via Pos/EndPos).
	Name string
	// Kind classifies the entry.
	Kind OutlineKind
	// Pos is the start of the symbol.
	Pos lexer.Position
	// EndPos is the end of the symbol.
	EndPos lexer.Position
	// Children are nested symbols.
	Children []*OutlineInfo
}

// Outline walks a parsed VQL statement and returns the root outline node.
//
// The root is:
//   - a LET entry (name = variable) with the value outline as children, or
//   - a query entry (name = FROM plugin) when the statement is a SELECT.
//
// The vql parameter may be nil, in which case nil is returned.
func Outline(vql *VQL) *OutlineInfo {
	if vql == nil {
		return nil
	}

	if vql.Let != "" {
		root := &OutlineInfo{
			Name:   vql.Let,
			Kind:   OutlineKindLet,
			Pos:    vql.Pos,
			EndPos: vql.EndPos,
		}
		if vql.StoredQuery != nil {
			root.Children = append(root.Children, outlineSelect(vql.StoredQuery))
		}
		if vql.Expression != nil {
			root.Children = append(root.Children, outlineExpression(vql.Expression))
		}
		return root
	}

	if vql.Query != nil {
		return outlineSelect(vql.Query)
	}
	return nil
}

func outlineSelect(select_ *_Select) *OutlineInfo {
	if select_ == nil {
		return nil
	}

	root := &OutlineInfo{
		Kind:   OutlineKindQuery,
		Pos:    select_.Pos,
		EndPos: select_.EndPos,
	}
	if select_.From != nil && select_.From.Plugin.Name != "" {
		root.Name = select_.From.Plugin.Name
	}

	if select_.SelectExpression != nil {
		for _, column := range select_.SelectExpression.Expressions {
			child := outlineColumn(column)
			if child != nil {
				root.Children = append(root.Children, child)
			}
		}
	}
	return root
}

func outlineColumn(expr *_AliasedExpression) *OutlineInfo {
	if expr == nil {
		return nil
	}

	// A subquery used directly as a column (SELECT (SELECT ...)).
	if expr.SubSelect != nil {
		return outlineSelect(expr.SubSelect)
	}

	entry := &OutlineInfo{
		Kind:   OutlineKindColumn,
		Pos:    expr.Pos,
		EndPos: expr.EndPos,
	}
	if expr.As != "" {
		entry.Name = expr.As
	}
	if expr.Expression != nil {
		entry.Children = outlineExpressionChildren(expr.Expression)
	}
	return entry
}

// outlineExpression builds an entry for a bare expression (used when a
// LET is assigned a non-query value). It carries any nested function calls
// as children.
func outlineExpression(expr *_AndExpression) *OutlineInfo {
	if expr == nil {
		return nil
	}
	entry := &OutlineInfo{
		Kind:   OutlineKindQuery,
		Pos:    expr.Pos,
		EndPos: expr.EndPos,
	}
	entry.Children = outlineExpressionChildren(expr)
	return entry
}

// outlineExpressionChildren returns the function calls nested inside an
// expression tree.
func outlineExpressionChildren(expr *_AndExpression) []*OutlineInfo {
	children := []*OutlineInfo{}
	collectExpressionFunctions(expr, &children)
	return children
}

func collectExpressionFunctions(expr *_AndExpression, children *[]*OutlineInfo) {
	if expr == nil {
		return
	}
	collectOrFunctions(expr.Left, children)
	for _, term := range expr.Right {
		collectOrFunctions(term.Term, children)
	}
}

func collectOrFunctions(expr *_OrExpression, children *[]*OutlineInfo) {
	if expr == nil {
		return
	}
	collectConditionFunctions(expr.Left, children)
	for _, term := range expr.Right {
		collectConditionFunctions(term.Term, children)
	}
}

func collectConditionFunctions(expr *_ConditionOperand, children *[]*OutlineInfo) {
	if expr == nil {
		return
	}
	if expr.Not != nil {
		collectConditionFunctions(expr.Not, children)
	}
	collectAdditionFunctions(expr.Left, children)
	if expr.Right != nil {
		collectAdditionFunctions(expr.Right.Right, children)
	}
}

func collectAdditionFunctions(expr *_AdditionExpression, children *[]*OutlineInfo) {
	if expr == nil {
		return
	}
	collectMultiplicationFunctions(expr.Left, children)
	for _, term := range expr.Right {
		collectMultiplicationFunctions(term.Term, children)
	}
}

func collectMultiplicationFunctions(expr *_MultiplicationExpression, children *[]*OutlineInfo) {
	if expr == nil {
		return
	}
	collectMemberFunctions(expr.Left, children)
	for _, term := range expr.Right {
		collectValueFunctions(term.Factor, children)
	}
}

func collectMemberFunctions(expr *_MemberExpression, children *[]*OutlineInfo) {
	if expr == nil {
		return
	}
	collectValueFunctions(expr.Left, children)
	for _, term := range expr.Right {
		if term.Index != nil {
			collectValueFunctions(term.Index, children)
		}
		if term.RangeEnd != nil {
			collectValueFunctions(term.RangeEnd, children)
		}
	}
}

func collectValueFunctions(value *_Value, children *[]*OutlineInfo) {
	if value == nil {
		return
	}
	if value.SymbolRef != nil && value.SymbolRef.Called {
		*children = append(*children, &OutlineInfo{
			Name:   value.SymbolRef.Symbol,
			Kind:   OutlineKindFunction,
			Pos:    value.SymbolRef.Pos,
			EndPos: value.SymbolRef.EndPos,
		})
	}
	if value.Subexpression != nil {
		collectCommaFunctions(value.Subexpression, children)
	}
}

func collectCommaFunctions(expr *_CommaExpression, children *[]*OutlineInfo) {
	if expr == nil {
		return
	}
	collectAndFunctions(expr.Left, children)
	for _, term := range expr.Right {
		collectAndFunctions(term.Term, children)
	}
}

func collectAndFunctions(expr *_AndExpression, children *[]*OutlineInfo) {
	collectExpressionFunctions(expr, children)
}
