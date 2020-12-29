package vfilter

import (
	"context"
	"fmt"

	"github.com/Velocidex/ordereddict"
	"github.com/alecthomas/participle"
)

var (
	lambdaParser = participle.MustBuild(
		&Lambda{},
		participle.Lexer(vqlLexer),
		participle.Elide("Comment", "MLineComment", "VQLComment"))
)

type Lambda struct {
	Parameters  *_ParameterList ` @@ `
	LetOperator string          ` @"=>" `
	Expression  *_AndExpression ` @@ `
}

func (self *Lambda) GetParameters() []string {
	result := []string{}

	if self.Parameters != nil {
		visitor(self.Parameters, &result)
	}

	return result
}

func (self *Lambda) ToString(scope *Scope) string {
	return fmt.Sprintf("%v => %v", self.Parameters.ToString(scope),
		self.Expression.ToString(scope))
}

func (self *Lambda) Reduce(ctx context.Context, scope *Scope, parameters []Any) Any {
	my_parameters := self.GetParameters()
	if len(my_parameters) != len(parameters) {
		scope.Log("Incorrect number of parameters is Lambda call")
		return Null{}
	}

	vars := ordereddict.NewDict()
	for idx, name := range my_parameters {
		vars.Set(name, parameters[idx])
	}
	subscope := scope.Copy().AppendVars(vars)
	return self.Expression.Reduce(ctx, subscope)
}

func ParseLambda(expression string) (*Lambda, error) {
	lambda := &Lambda{}
	err := lambdaParser.ParseString(expression, lambda)
	return lambda, err
}
