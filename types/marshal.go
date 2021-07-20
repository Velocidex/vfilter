package types

import "encoding/json"

type MarshalItem struct {
	Type    string          `json:"type"`
	Comment string          `json:"comment,omitempty"`
	Data    json.RawMessage `json:"data"`
}

// A type that implements the marshaller interface is able to convert
// itself into a MarshalItem
type Marshaler interface {
	Marshal(scope Scope) (*MarshalItem, error)
}

type Unmarshaller interface {
	Unmarshal(unmarshaller Unmarshaller,
		scope Scope, item *MarshalItem) (interface{}, error)
}
