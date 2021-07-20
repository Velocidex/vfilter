package marshal

import (
	"encoding/json"
	"errors"

	"www.velocidex.com/golang/vfilter/types"
)

type Unmarshaller struct {
	Handlers map[string]types.Unmarshaller
}

func (self *Unmarshaller) Unmarshal(
	unmarshaller types.Unmarshaller,
	scope types.Scope, item *types.MarshalItem) (interface{}, error) {
	switch item.Type {
	case "JSON":
		var value interface{}
		err := json.Unmarshal(item.Data, &value)
		return value, err

	default:
		handler, pres := self.Handlers[item.Type]
		if !pres {
			return nil, errors.New("No parser for MarshalItem " + item.Type)
		}

		return handler.Unmarshal(self, scope, item)
	}
}

func NewUnmarshaller() *Unmarshaller {
	return &Unmarshaller{
		Handlers: make(map[string]types.Unmarshaller),
	}
}
