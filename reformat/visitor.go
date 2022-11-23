package reformat

import (
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/types"
)

func ReFormatVQL(scope types.Scope, query string,
	options vfilter.FormatOptions) (string, error) {
	vql, err := vfilter.MultiParse(query)
	if err != nil {
		return "", err
	}

	visitor := vfilter.NewVisitor(scope, options)
	visitor.Visit(vql)

	return visitor.ToString(), nil
}
