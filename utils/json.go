package utils

import "encoding/json"

func MarshalStringIndent(in interface{}) string {
	serialized, err := json.MarshalIndent(in, "", " ")
	if err != nil {
		return ""
	}
	return string(serialized)
}
