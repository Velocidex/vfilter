package vfilter

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Velocidex/ordereddict"
	"github.com/sebdah/goldie"
	"github.com/stretchr/testify/assert"
)

func marshal_indent(rows []Row) ([]byte, error) {
	return json.MarshalIndent(rows, "", " ")
}

func TestAPIGetResponseChannel(t *testing.T) {
	ctx := context.Background()
	scope := makeTestScope()

	golden := ordereddict.NewDict()

	// test() returns 3 rows
	vql, err := Parse("SELECT * FROM test()")
	assert.NoError(t, err)

	// GetResponseChannel streams result sets over a channel.
	test_GetResponseChannel := func(name string, vql *VQL, max_rows int) {
		payloads := []*VFilterJsonResult{}
		serialized := []string{}
		for result := range GetResponseChannel(
			vql, ctx, scope, marshal_indent,
			max_rows, 1000) {
			serialized = append(serialized, string(result.Payload))
			payloads = append(payloads, result)
		}
		golden.Set(name, payloads)
		golden.Set(name+".Payloads", serialized)
	}

	// Send all rows in one packet.
	test_GetResponseChannel("GetResponseChannel", vql, 1000)

	// Send packets of max 1 row (i.e. will send 3 packets)
	test_GetResponseChannel("GetResponseChannel_Small", vql, 1)

	{
		// OutputJSON dumps everything in one big json blob.
		serialized, err := OutputJSON(vql, ctx, scope, marshal_indent)
		assert.NoError(t, err)
		golden.Set("OutputJSON", string(serialized))
	}

	result_json, _ := json.MarshalIndent(golden, "", " ")
	goldie.Assert(t, "api", result_json)
}
