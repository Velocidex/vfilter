package types

type Throttler interface {
	// This is called frequently through the code, it may sleep in
	// order to delay query execution.
	ChargeOp()
	Close()
}
