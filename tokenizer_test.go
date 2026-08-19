package vfilter

import (
	"encoding/json"
	"testing"

	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
)

func TestTokenize(t *testing.T) {
	// A query with comments, keywords, strings, numbers and operators.
	tokens, err := Tokenize(`
// A comment
LET X = "hello" + 42

LET Y = '''
Multiline
String
'''

/* Multiline
comment
*/
SELECT A.B, lower(X) AS L FROM pslist(pid=1) WHERE Foo =~ Bar
`)
	assert.NoError(t, err)

	serialized, err := json.MarshalIndent(tokens, "", " ")
	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.Assert(t, "TestTokenize", serialized)
}
