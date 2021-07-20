package marshal

import (
	"encoding/json"
	"fmt"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

func Marshal(scope types.Scope, item interface{}) (*types.MarshalItem, error) {
	switch t := item.(type) {
	case types.Marshaler:
		return t.Marshal(scope)

		// Handle ordered dicts especially so they retain
		// their order.
	case *ordereddict.Dict:
		serialized, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		return &types.MarshalItem{
			Type: "OrderedDict",
			Data: serialized,
		}, nil

		// Normal types are just serialized with JSON
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		string, float32, float64:
		serialized, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		return &types.MarshalItem{
			Type: "JSON",
			Data: serialized,
		}, nil

		// The default marshaller is to just convert it into
		// JSON and hope for the best - but we let the user
		// know we dont know how to handle it.
	default:
		serialized, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		return &types.MarshalItem{
			Type:    "JSON",
			Data:    serialized,
			Comment: fmt.Sprintf("Default encoding from %T", item),
		}, nil
	}
}
