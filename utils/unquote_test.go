package utils

import (
	"fmt"
	"testing"

	"github.com/sebdah/goldie/v2"
)

var testcases = []string{
	`"Hello world"`,
	`"Multi\nLine\n"`,
	`"Multi\r\nLine"`,
	`"With\ttab"`,
	`"With Hex \x01\x02\xf0\xf1"`,
	`"With escaped \"\' quotes"`,

	`'''Raw \r\n'''`,

	// Invalid sequences are silently ignored

	// Truncated sequences are dropped.
	`"Trailing \"`,
	`"Trailing \x1"`,

	// Invalid hex are copied verbatim.
	`"Invalid hex \xag"`,
}

func TestUnquote(t *testing.T) {
	res := make([]string, 0)
	for _, testcase := range testcases {
		decoded := Unquote(testcase)
		decoded += fmt.Sprintf(" Hex: % x", decoded)
		res = append(res, decoded)
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "TestUnquote", res)
}

var split_ident_cases = []string{
	// Escaped component
	"X.`Hello world`",

	// Embedded .
	"`X.Hello world`",
}

func TestSplitIdent(t *testing.T) {
	res := make([][]string, 0)
	for _, testcase := range split_ident_cases {
		decoded := SplitIdent(testcase)
		res = append(res, decoded)
	}

	g := goldie.New(
		t,
		goldie.WithFixtureDir("fixtures"),
		goldie.WithNameSuffix(".golden"),
		goldie.WithDiffEngine(goldie.ColoredDiff),
	)
	g.AssertJson(t, "TestSplitIdent", res)
}
