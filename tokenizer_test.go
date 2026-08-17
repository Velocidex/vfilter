package vfilter

import (
	"testing"

	"github.com/alecthomas/participle/v2/lexer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenize(t *testing.T) {
	// A query with comments, keywords, strings, numbers and operators.
	tokens, err := Tokenize(`
// A comment
LET X = "hello" + 42

SELECT A.B, lower(X) AS L FROM pslist(pid=1) WHERE Foo =~ Bar
`)
	require.NoError(t, err)

	names := []string{}
	values := []string{}
	for _, t := range tokens {
		names = append(names, t.Type)
		values = append(values, t.Value)
	}
	assert.Contains(t, names, "Comment")
	assert.Contains(t, names, "LET")
	assert.Contains(t, names, "SELECT")
	assert.Contains(t, names, "FROM")
	assert.Contains(t, names, "WHERE")
	assert.Contains(t, names, "AS")
	assert.Contains(t, names, "Ident")
	assert.Contains(t, names, "String")
	assert.Contains(t, names, "Number")
	assert.Contains(t, names, "Operators")

	// No whitespace tokens.
	for _, n := range names {
		assert.NotEqual(t, "whitespace", n)
	}
}

func TestTokenizePositions(t *testing.T) {
	// Positions must be monotonic, in source order.
	tokens, err := Tokenize(`LET X = 1
SELECT * FROM pslist()`)
	require.NoError(t, err)

	last := lexer.Position{Offset: -1}
	for _, tok := range tokens {
		assert.Greater(t, tok.Pos.Offset, last.Offset)
		last = tok.Pos
	}
}

func TestTokenizeCommentsElidedByParseButNotTokenizer(t *testing.T) {
	query := `// hidden comment
LET X = 1`
	// MultiParse strips comments.
	statements, err := MultiParse(query)
	require.NoError(t, err)
	require.Len(t, statements, 1)

	// Tokenize keeps them.
	tokens, err := Tokenize(query)
	require.NoError(t, err)
	found := false
	for _, t := range tokens {
		if t.Type == "Comment" {
			found = true
		}
	}
	assert.True(t, found, "tokenizer should expose comment tokens")
}
