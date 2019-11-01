package vfilter

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alecthomas/repr"
	"github.com/stretchr/testify/assert"
)

type dictSerializationTest struct {
	dict       *Dict
	serialized string
}

var dictSerializationTests = []dictSerializationTest{
	{NewDict().Set("Foo", "Bar"), `{"Foo":"Bar"}`},

	// Test an unserilizable member - This should not prevent the
	// entire dict from serializing - only that member should be
	// ignored.
	{NewDict().Set("Foo", "Bar").
		Set("Time", time.Unix(3000000000000000, 0)),
		`{"Foo":"Bar","Time":null}`},

	// Recursive dict
	{NewDict().Set("Foo",
		NewDict().Set("Bar", 2).
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
	test := NewDict().
		Set("A", 1).
		Set("B", 2)

	assert.Equal(t, []string{"A", "B"}, scope.GetMembers(test))

	test = NewDict().
		Set("B", 1).
		Set("A", 2)

	assert.Equal(t, []string{"B", "A"}, scope.GetMembers(test))
}

func TestCaseInsensitive(t *testing.T) {
	test := NewDict().SetCaseInsensitive()

	test.Set("FOO", 1)

	value, pres := test.Get("foo")
	assert.True(t, pres)
	assert.Equal(t, 1, value)

	test = NewDict().Set("FOO", 1)
	value, pres = test.Get("foo")
	assert.False(t, pres)
}
