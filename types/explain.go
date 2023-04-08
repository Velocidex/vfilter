package types

import "github.com/Velocidex/ordereddict"

type TypeDescriber interface {
	DescribeType() string
}

// An explainer can be installed into the scope and will be used to
// provide tracing information about the query as it executes.
type Explainer interface {
	// Emitted when beginning to explain new query
	StartQuery(select_ast_node interface{})

	// Report each row extracted from a plugin
	PluginOutput(plugin_ast_node interface{}, row Row)

	// The final row emitted from SELECT (after filtering)
	SelectOutput(row Row)

	// Report when args are parsed into a plugin/function
	ParseArgs(args *ordereddict.Dict, result interface{}, err error)

	RejectRow(where_ast_node interface{})

	// A general purpose log
	Log(message string)
}
