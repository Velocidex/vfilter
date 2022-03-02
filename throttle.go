package vfilter

import (
	"time"

	"www.velocidex.com/golang/vfilter/types"
)

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

func NewTimeThrottler(rate float64) types.Throttler {
	// rate of 0 means no throttling.
	if rate == 0 || rate > 100 {
		rate = 100
	}

	result := &TimeThrottler{
		ticker: time.NewTicker(time.Nanosecond *
			time.Duration((float64(1000000000) / float64(rate)))),
		done:    make(chan bool, 1),
		running: true,
	}

	// Just ignore rates which are too fast - do not throttle at
	// all.
	if rate >= 100 {
		result.Close()
	}

	return result
}
