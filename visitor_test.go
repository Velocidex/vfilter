package vfilter

import (
	"fmt"
	"testing"

	"github.com/sebdah/goldie/v2"
	"github.com/stretchr/testify/assert"
	"www.velocidex.com/golang/vfilter/utils"
)

var (
	visitorTestCases = []struct {
		Query string
	}{{
		Query: "LET X = 5 SELECT * FROM info(Foo=1, Bar='Hello')",
	}, {
		Query: "LET Func(X=1) = 5",
	}, {
		Query: "LET X <= 1 SELECT 1 + X FROM scope()",
	}}
)

func TestVisitor(t *testing.T) {
	scope := makeScope()
	golden := ""
	for idx, tc := range visitorTestCases {
		golden += fmt.Sprintf("\n%v: %v\n", idx, tc.Query)
		vql, err := MultiParseWithComments(tc.Query)
		assert.NoError(t, err)

		visitor := NewVisitor(scope, CollectAllInfo)
		visitor.Visit(vql)

		golden += "\nCallSites: " + utils.MarshalStringIndent(visitor.CallSites)
		golden += "\nDefinitions: " + utils.MarshalStringIndent(visitor.Definitions)
		golden += "\nComments: " + utils.MarshalStringIndent(visitor.Comments)
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.Assert(t, "TestVisitor", []byte(golden))
}
