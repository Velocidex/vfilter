package types

type AggregatorCtx interface {
	// Modify the context under lock. If there is no existing value,
	// old_value will be nil and pres will be false. You can use this
	// to read the old value as well by just returning it.
	Modify(name string, modifier func(old_value Any, pres bool) Any) Any
}
