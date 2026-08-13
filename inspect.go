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

// This file provides an exported, position-aware analysis of the VQL
// AST: the plugin calls, function calls, symbol references and LET
// definitions in a parsed query.
//
// The grammar node types (like _Select, _AndExpression and _SymbolRef)
// are unexported, which makes it impossible for external packages to
// traverse the AST directly. The Inspect() function below is the
// sanctioned way for tooling (for example the VQL language server) to
// learn about plugin calls, function calls and argument positions.
//
// Inspect() does not walk the grammar itself. It is a thin adapter
// over the package's single traversal mechanism (the Visitor): the
// visitor collects call sites, definition sites and symbol references
// as it descends the tree, and Inspect() translates its (now
// position-aware) collections into the exported analysis types below.
// Keeping one descent of the grammar - instead of one per analysis -
// is what allows Inspect() and Outline() to stay in sync with the
// parser without duplicating its tree walk.

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
	// FreeForm is reserved; it is always false for parsed queries (the
	// registry decides whether a callable accepts arbitrary args).
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

	visitor := NewVisitor(NewScope(), FormatOptions{
		CollectCallSites:       true,
		CollectDefinitionSites: true,

		// We only care about the analysis results, not the
		// formatting. AnalysisOnly skips the look-ahead copies
		// the formatter would make, so each node is visited once.
		AnalysisOnly: true,
	})
	visitor.Visit(vql)

	// The visitor records every callable it meets in a single
	// list, tagged with its Type ("symbol", "function" or
	// "plugin"). Split it into the two exported collections.
	for _, cs := range visitor.CallSites {
		switch cs.Type {
		case "symbol":
			result.Symbols = append(result.Symbols, SymbolInfo{
				Name:   cs.Name,
				Pos:    cs.Pos,
				EndPos: cs.EndPos,
			})

		case "function", "plugin":
			call := CallInfo{
				Name:     cs.Name,
				IsPlugin: cs.Type == "plugin",
				Pos:      cs.Pos,
				EndPos:   cs.EndPos,
			}
			for i, arg := range cs.Args {
				info := ArgInfo{Name: arg}
				if i < len(cs.ArgPositions) {
					info.Pos = cs.ArgPositions[i].Pos
					info.EndPos = cs.ArgPositions[i].EndPos
				}
				call.Args = append(call.Args, info)
			}
			result.Calls = append(result.Calls, call)
		}
	}

	for _, def := range visitor.Definitions {
		result.Lets = append(result.Lets, LetInfo{
			Name: def.Name,
			Pos:  def.Pos,
		})
	}

	return result
}
