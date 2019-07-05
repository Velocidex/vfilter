package vfilter

import (
	"time"
)

type Throttler interface {
	ChargeOp()
	Close()
}

type TimeThrottler struct {
	ticker  *time.Ticker
	done    chan bool
	running bool
}

func (self *TimeThrottler) ChargeOp() {
	select {
	case <-self.ticker.C:
	case <-self.done:
	}
}

func (self *TimeThrottler) Close() {
	if self.running {
		self.ticker.Stop()
		self.running = false
		close(self.done)
	}
}

func NewTimeThrottler(rate float64) Throttler {
	result := &TimeThrottler{
		ticker: time.NewTicker(time.Nanosecond *
			time.Duration((float64(1000000000) / float64(rate)))),
		done:    make(chan bool, 1),
		running: true,
	}

	// Just ignore rates which are too fast - do not throttle at
	// all.
	if rate > 100 {
		result.Close()
	}

	return result
}

func InstallThrottler(scope *Scope, throttler Throttler) {
	// Ignore throttles faster than 100 ops per second.
	scope.AppendVars(NewDict().Set("$throttle", throttler))
	scope.AddDestructor(func() {
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
