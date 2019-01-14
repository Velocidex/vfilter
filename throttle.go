package vfilter

import (
	"time"
)

type Throttler interface {
	ChargeOp()
	Close()
}

type TimeThrottler struct {
	ticker *time.Ticker
	done   chan bool
}

func (self TimeThrottler) ChargeOp() {
	select {
	case <-self.ticker.C:
	case <-self.done:
	}
}

func (self TimeThrottler) Close() {
	self.ticker.Stop()
	close(self.done)
}

func NewTimeThrottler(rate float64) Throttler {
	if rate > 100 {
		return TimeThrottler{
			done: make(chan bool, 1),
		}
	}

	return TimeThrottler{
		ticker: time.NewTicker(time.Nanosecond *
			time.Duration((float64(1000000000) / float64(rate)))),
		done: make(chan bool, 1),
	}
}

func InstallThrottler(scope *Scope, throttler Throttler) {
	// Ignore throttles faster than 100 ops per second.
	scope.AppendVars(NewDict().Set("$throttle", throttler))
	scope.AddDesctructor(func() {
		throttler.Close()
	})
}

func ChargeOp(scope *Scope) {
	any_throttle, _ := scope.Resolve("$throttle")
	throttle, ok := any_throttle.(Throttler)
	if ok {
		throttle.ChargeOp()
	}
}
