package protocols

import (
	"context"
	"reflect"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/types"
)

// The Iterator protocol allows types to be iterated over.
type IterateDispatcher struct {
	impl []IterateProtocol
}

func (self IterateDispatcher) Copy() IterateDispatcher {
	return IterateDispatcher{
		append([]IterateProtocol{}, self.impl...)}
}

func (self IterateDispatcher) Iterate(
	ctx context.Context, scope types.Scope, a types.Any) <-chan types.Row {

	switch t := a.(type) {

	// A LazyExpr is a placeholder for a real value.
	case types.LazyExpr:
		return scope.Iterate(ctx, t.Reduce(ctx))

		// A StoredQuery is a source of rows and so returns a channel.
	case types.StoredQuery:
		return t.Eval(ctx, scope)

	case *ordereddict.Dict:
		output_chan := make(chan types.Row)

		go func() {
			defer close(output_chan)

			select {
			case <-ctx.Done():
				return
			case output_chan <- t:
			}
		}()
		return output_chan
	}

	if is_array(a) {
		return _SliceIterator(ctx, scope, a)
	}

	for i, impl := range self.impl {
		if impl.Applicable(a) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Iterate(ctx, scope, a)
		}
	}

	scope.Trace("Protocol Iterate not found for %v (%T)", a, a)

	// By default if no other iterator is available, prepare a row
	// with the value as the _value column.
	output_chan := make(chan types.Row)
	go func() {
		defer close(output_chan)

		if !types.IsNullObject(a) {
			output_chan <- ordereddict.NewDict().Set("_value", a)
		}
	}()

	return output_chan
}

func (self *IterateDispatcher) AddImpl(elements ...IterateProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

// This protocol implements the truth value.
type IterateProtocol interface {
	Applicable(a types.Any) bool
	Iterate(ctx context.Context, scope types.Scope, a types.Any) <-chan types.Row
}

func _SliceIterator(ctx context.Context, scope types.Scope, a types.Any) <-chan types.Row {
	output_chan := make(chan types.Row)

	go func() {
		defer close(output_chan)

		a_value := reflect.Indirect(reflect.ValueOf(a))
		if a_value.Type().Kind() == reflect.Slice {
			for i := 0; i < a_value.Len(); i++ {
				value := a_value.Index(i).Interface()
				if types.IsNullObject(value) {
					continue
				}

				_, ok := value.(*ordereddict.Dict)
				if ok {
					output_chan <- value
				} else {
					output_chan <- ordereddict.NewDict().
						Set("_value", value)
				}
			}
		}

	}()

	return output_chan
}
