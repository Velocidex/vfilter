package scope

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/marshal"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Marshal a scope so it can be restored.
type ScopeItems struct {
	Vars []map[string]*types.MarshalItem `json:"vars,omitempty"`
}

func (self *Scope) Marshal(scope types.Scope) (*types.MarshalItem, error) {
	result := &ScopeItems{}

	for _, var_item := range self.vars {
		data := make(map[string]*types.MarshalItem)
		for _, k := range self.GetMembers(var_item) {
			// Skip these vars
			if strings.HasPrefix(k, "$") ||
				k == "NULL" ||
				k == "Artifact" {
				continue
			}

			value, pres := self.Resolve(k)
			if !pres {
				continue
			}

			// Just marshal each element
			serialized, err := marshal.Marshal(scope, value)
			if err != nil {
				return nil, err
			}
			data[k] = serialized
		}

		if len(data) > 0 {
			result.Vars = append(result.Vars, data)
		}
	}

	serialized, err := json.Marshal(result)
	return &types.MarshalItem{
		Type: "Scope",
		Data: serialized,
	}, err
}

type ScopeUnmarshaller struct{}

func (self ScopeUnmarshaller) Unmarshal(
	unmarshaller types.Unmarshaller,
	scope types.Scope, item *types.MarshalItem) (interface{}, error) {

	new_scope := scope.Copy()

	scope_items := &ScopeItems{}
	err := json.Unmarshal(item.Data, &scope_items)
	if err != nil {
		return nil, err
	}

	env := ordereddict.NewDict()
	for _, var_item := range scope_items.Vars {
		for k, v := range var_item {
			unmarshalled, err := unmarshaller.Unmarshal(unmarshaller,
				new_scope, v)
			if err == nil {
				if !utils.IsNil(unmarshaller) {
					env.Set(k, unmarshalled)
				}
			} else {
				fmt.Printf("Cant decode %v: %v\n", k, err)
			}
		}
	}

	return new_scope.AppendVars(env), nil
}
