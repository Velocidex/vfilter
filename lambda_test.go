package vfilter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type lambdaTest struct {
	name, clause string
	result       Any
	input        Any
}

var lambdaTestCases = []lambdaTest{
	{
		name:   "Simple Lambda",
		clause: "x=>1+1",
		input:  1,
		result: 2,
	},
	{
		name:   "Complex Lambda",
		clause: "x=> 1 + 0x0aB",
		input:  1,
		result: 172,
	},
}

func TestLambda(t *testing.T) {
	scope := makeScope()
	for _, test := range lambdaTestCases {
		lambda, err := ParseLambda(test.clause)
		assert.NoError(t, err)

		res := lambda.Reduce(
			context.Background(), scope, []Any{test.input})
		if !scope.Eq(test.result, res) {
			t.Fatalf("%v: %v != %v", test.name, test.result, res)
		}
	}
}
