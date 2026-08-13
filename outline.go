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
// unexported; Outline() is the sanctioned way for tooling to learn
// about the structure of a query.
//
// Outline() does not walk the grammar itself. It is a thin adapter
// over the package's single traversal mechanism (the Visitor): when
// constructed with CollectOutline, the visitor builds the OutlineInfo
// tree incrementally as it descends the AST - LET statements, queries
// and columns push nodes, and function calls inside column
// expressions become children - and Outline() returns the root it
// produced. Keeping one descent of the grammar - instead of one per
// analysis - is what allows Outline() and Inspect() to stay in sync
// with the parser without duplicating its tree walk.

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

	// isExpression marks the synthetic node the visitor creates for
	// a LET statement whose value is a plain expression rather than
	// a query. Function calls are outlined beneath it just like they
	// are beneath a column. This is an internal marker; callers never
	// need to set it.
	isExpression bool
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

	visitor := NewVisitor(NewScope(), FormatOptions{
		CollectOutline: true,

		// We only care about the outline, not the formatting.
		// AnalysisOnly skips the formatter's look-ahead copies so
		// the outline stack is maintained on the visitor itself.
		AnalysisOnly: true,
	})
	visitor.Visit(vql)

	return visitor.outlineRoot
}
