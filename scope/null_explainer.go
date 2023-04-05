package scope

import (
	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

var (
	NULL_EXPLAINER = &NullExplainer{}
)

type NullExplainer struct{}

func (self *NullExplainer) StartQuery(select_ast_node interface{}) {}

func (self *NullExplainer) RejectRow(where_ast_node interface{}) {}

func (self *NullExplainer) ParseArgs(args *ordereddict.Dict, result interface{}, err error) {}

func (self *NullExplainer) PluginOutput(
	ast_node interface{}, row types.Row) {
}

func (self *NullExplainer) SelectOutput(row types.Row) {}

func (self *NullExplainer) Log(message string) {}
