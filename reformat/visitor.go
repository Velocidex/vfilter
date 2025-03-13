package reformat

import (
	"strings"

	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/types"
)

func ReFormatVQL(scope types.Scope, query string,
	options vfilter.FormatOptions) (string, error) {
	vql, err := vfilter.MultiParseWithComments(query)
	if err != nil {
		return "", err
	}

	visitor := vfilter.NewVisitor(scope, options)
	visitor.Visit(vql)

	lines := strings.Split(visitor.ToString(), "\n")
	result := make([]string, 0, len(lines))
	for _, l := range lines {
		result = append(result, strings.TrimRight(l, " "))
	}

	return strings.Join(result, "\n"), nil
}
