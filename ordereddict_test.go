package vfilter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Velocidex/ordereddict"
	"github.com/alecthomas/repr"
	"github.com/stretchr/testify/assert"
)

type dictSerializationTest struct {
	dict       *ordereddict.Dict
	serialized string
}

var dictSerializationTests = []dictSerializationTest{
	{ordereddict.NewDict().Set("Foo", "Bar"), `{"Foo":"Bar"}`},

	// Test an unserilizable member - This should not prevent the
	// entire dict from serializing - only that member should be
	// ignored.
	{ordereddict.NewDict().Set("Foo", "Bar").
		Set("Time", time.Unix(3000000000000000, 0)),
		`{"Foo":"Bar","Time":null}`},

	// Recursive dict
	{ordereddict.NewDict().Set("Foo",
		ordereddict.NewDict().Set("Bar", 2).
			Set("Time", time.Unix(3000000000000000, 0))),
		`{"Foo":{"Bar":2,"Time":null}}`},
}

func TestDictSerialization(t *testing.T) {
	for _, test := range dictSerializationTests {
		serialized, err := json.Marshal(test.dict)
		if err != nil {
			t.Fatalf("Failed to serialize %v: %v", repr.String(test.dict), err)
		}

		assert.Equal(t, test.serialized, string(serialized))
	}
}

func TestOrder(t *testing.T) {
	scope := NewScope()
	test := ordereddict.NewDict().
		Set("A", 1).
		Set("B", 2)

	assert.Equal(t, []string{"A", "B"}, scope.GetMembers(test))

	test = ordereddict.NewDict().
		Set("B", 1).
		Set("A", 2)

	assert.Equal(t, []string{"B", "A"}, scope.GetMembers(test))
}

func TestCaseInsensitive(t *testing.T) {
	test := ordereddict.NewDict().SetCaseInsensitive()

	test.Set("FOO", 1)

	value, pres := test.Get("foo")
	assert.True(t, pres)
	assert.Equal(t, 1, value)

	test = ordereddict.NewDict().Set("FOO", 1)
	value, pres = test.Get("foo")
	assert.False(t, pres)
}
