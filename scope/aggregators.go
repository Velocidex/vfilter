package scope

import (
	"sync"

	"www.velocidex.com/golang/vfilter/types"
)

type AggregatorCtx struct {
	mu   sync.Mutex
	data map[string]types.Any
}

func (self *AggregatorCtx) Modify(name string,
	modifier func(old_value types.Any, pres bool) types.Any) types.Any {
	self.mu.Lock()
	defer self.mu.Unlock()

	old_value, pres := self.data[name]
	new_value := modifier(old_value, pres)
	self.data[name] = new_value
	return new_value
}

func NewAggregatorCtx() *AggregatorCtx {
	return &AggregatorCtx{
		data: make(map[string]types.Any),
	}
}
