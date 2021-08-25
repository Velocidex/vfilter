package arg_parser

import (
	"reflect"
	"sync"
)

var (
	// A Global cache of parsers and types. These are not expected
	// to ever change since they are tied to types within the
	// actual code.
	mu          sync.Mutex
	parserCache = make(map[reflect.Type]*Parser)
)

func GetParser(target reflect.Value) (*Parser, error) {
	mu.Lock()
	defer mu.Unlock()

	t := target.Type()

	parser, pres := parserCache[t]
	if pres {
		// fmt.Printf("Cache hit for %v\n", t)
		return parser, nil
	}

	parser, err := BuildParser(target)
	if err != nil {
		return nil, err
	}
	parserCache[t] = parser
	return parser, nil
}
