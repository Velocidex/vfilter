package explain

import (
	"fmt"

	"github.com/Velocidex/ordereddict"
	"github.com/alecthomas/repr"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/types"
)

type descriptor struct {
	Type  string
	Key   string
	Value string
}

type LoggingExplainer struct {
	scope types.Scope
}

func NewLoggingExplainer(scope types.Scope) *LoggingExplainer {
	return &LoggingExplainer{scope: scope}
}

func (self *LoggingExplainer) StartQuery(select_ast_node interface{}) {
	self.scope.Log("DEBUG:Explain start query: %v",
		vfilter.FormatToString(self.scope, select_ast_node))
}

func (self *LoggingExplainer) ParseArgs(args *ordereddict.Dict, result interface{}, err error) {
	if err == nil {
		self.scope.Log("DEBUG:  arg parsing: %v",
			repr.String(result, repr.NoIndent(),
				repr.OmitEmpty(true), repr.IgnorePrivate()))
	} else {
		self.scope.Log("DEBUG:  arg parsing: error %v while parsing %v",
			err, repr.String(args, repr.NoIndent(),
				repr.OmitEmpty(true), repr.IgnorePrivate()))
	}
}

func (self *LoggingExplainer) PluginOutput(
	node interface{}, row types.Row) {

	name := ""
	plugin, ok := node.(vfilter.Plugin)
	if ok {
		name = plugin.Name + "()"
	}

	var fields []descriptor
	for _, key := range self.scope.GetMembers(row) {
		value, pres := self.scope.Associative(row, key)
		if !pres {
			continue
		}

		var type_desc string
		describer, ok := value.(types.TypeDescriber)
		if ok {
			type_desc = describer.DescribeType()
		} else {
			type_desc = fmt.Sprintf("%T", value)
		}

		fields = append(fields, descriptor{
			Type:  type_desc,
			Key:   key,
			Value: fmt.Sprintf("%v", value),
		})
	}

	self.scope.Log("DEBUG: plugin %v sent row: %v",
		name, fields)
}

func (self *LoggingExplainer) SelectOutput(row types.Row) {
	var fields []descriptor
	for _, key := range self.scope.GetMembers(row) {
		value, pres := self.scope.Associative(row, key)
		if !pres {
			continue
		}

		fields = append(fields, descriptor{
			Type:  fmt.Sprintf("%T", value),
			Key:   key,
			Value: fmt.Sprintf("%v", value),
		})
	}

	self.scope.Log("DEBUG: SELECT: emitting row: %v",
		fields)
}

func (self *LoggingExplainer) Log(message string) {
	self.scope.Log("DEBUG:" + message)
}

func (self *LoggingExplainer) RejectRow(where_ast_node interface{}) {
	self.scope.Log("DEBUG: REJECTED by " +
		vfilter.FormatToString(self.scope, where_ast_node))
}
