package vfilter

import (
	"strings"

	"github.com/alecthomas/participle/v2/lexer"
)

// Token represents a single lexical token of a VQL query.
type Token struct {
	// Type is the symbolic name of the token as used by the VQL
	// lexer, e.g. "Ident", "SELECT", "String", "Number", "Comment".
	Type string
	// Value is the raw source text of the token.
	Value string
	// Pos is the start of the token.
	Pos lexer.Position
	// EndPos is the position just past the end of the token.
	EndPos lexer.Position
}

// Tokenize lexes a VQL query and returns all non-whitespace tokens
// in source order, including comments. Whitespace is elided because
// the VQL lexer only elides rules whose names start with a lowercase
// letter. The token positions are 0-based byte offsets suitable for
// LSP tooling; clients that require UTF-16 positions must convert.
//
// This is the public entry point for tooling that needs lexical
// information about a query, such as semantic highlighting. The
// returned tokens include comment tokens which the AST-based
// Inspect()/Outline() APIs deliberately hide.
func Tokenize(expression string) ([]Token, error) {
	lex, err := vqlLexer.Lex("", strings.NewReader(expression))
	if err != nil {
		return nil, err
	}

	symbols := vqlLexer.Symbols()
	reverse := make(map[lexer.TokenType]string, len(symbols))
	for name, ttype := range symbols {
		reverse[ttype] = name
	}

	result := []Token{}
	for {
		t, err := lex.Next()
		if err != nil {
			return nil, err
		}
		if t.EOF() {
			return result, nil
		}
		name, ok := reverse[t.Type]
		if !ok {
			// EOF is the only type not in Symbols(); we handle
			// it above, so this should never happen.
			continue
		}
		result = append(result, Token{
			Type:   name,
			Value:  t.Value,
			Pos:    t.Pos,
			EndPos: t.Pos.Add(lexer.Position{Offset: len(t.Value)}),
		})
	}
}
