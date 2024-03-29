package vfilter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/marshal"
	"www.velocidex.com/golang/vfilter/types"
)

type StoredQueryItem struct {
	Query      string   `json:"query,omitempty"`
	Name       string   `json:"name,omitempty"`
	Parameters []string `json:"parameters,omitempty"`
}

func (self *_StoredQuery) Marshal(
	scope types.Scope) (*types.MarshalItem, error) {

	var query string
	if self.parameters == nil {
		query = fmt.Sprintf("LET `%v` = %s", self.name,
			FormatToString(scope, self.query))
	} else {
		query = fmt.Sprintf("LET `%v`(%s) = %s", self.name,
			strings.Join(self.parameters, ", "),
			FormatToString(scope, self.query))
	}

	query_str, err := json.Marshal(query)
	return &types.MarshalItem{
		Type: "Replay",
		Data: query_str,
	}, err
}

func (self *StoredExpression) Marshal(
	scope types.Scope) (*types.MarshalItem, error) {

	var query string
	if self.parameters == nil {
		query = fmt.Sprintf("LET `%v` = %s", self.name,
			FormatToString(scope, self.Expr))
	} else {
		query = fmt.Sprintf("LET `%v`(%s) = %s", self.name,
			strings.Join(self.parameters, ", "),
			FormatToString(scope, self.Expr))
	}

	query_str, err := json.Marshal(query)
	return &types.MarshalItem{
		Type: "Replay",
		Data: query_str,
	}, err
}

type ReplayUnmarshaller struct{}

func (self ReplayUnmarshaller) Unmarshal(
	unmarshaller types.Unmarshaller,
	scope types.Scope, item *types.MarshalItem) (interface{}, error) {
	var query string
	err := json.Unmarshal(item.Data, &query)
	if err != nil {
		return nil, err
	}

	vql, err := Parse(query)
	if err != nil {
		return nil, err
	}

	for _ = range vql.Eval(context.Background(), scope) {
	}

	return nil, nil
}

type OrdereddictUnmarshaller struct{}

func (self OrdereddictUnmarshaller) Unmarshal(
	unmarshaller types.Unmarshaller,
	scope types.Scope, item *types.MarshalItem) (interface{}, error) {
	dict := ordereddict.NewDict()
	err := json.Unmarshal(item.Data, dict)
	if err != nil {
		return nil, err
	}

	return dict, nil
}

func NewUnmarshaller(ignore_vars []string) *marshal.Unmarshaller {
	unmarshaller := marshal.NewUnmarshaller()
	unmarshaller.Handlers["Scope"] = ScopeUnmarshaller{ignore_vars}
	unmarshaller.Handlers["Replay"] = ReplayUnmarshaller{}
	unmarshaller.Handlers["OrderedDict"] = OrdereddictUnmarshaller{}

	return unmarshaller
}
