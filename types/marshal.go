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

// An unmarshaller for a custom type. Note: You must register this
// unmarshaller with marshal/Unmarshaller like this:
//
// unmarshaller := marshal.NewUnmarshaller()
// unmarshaller.Handlers["Scope"] = vfilter.ScopeUnmarshaller{ignoreVars}
// unmarshaller.Handlers["Replay"] = vfilter.ReplayUnmarshaller{}
//
type Unmarshaller interface {
	Unmarshal(unmarshaller Unmarshaller,
		scope Scope, item *MarshalItem) (interface{}, error)
}
