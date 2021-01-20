package types

import (
	"sync/atomic"

	"github.com/Velocidex/ordereddict"
)

// A lightweight struct for accumulating general stats.
type Stats struct {
	// All rows emitted from all plugins (this includes filtered rows).
	_RowsScanned uint64

	// Total number of plugin calls.
	_PluginsCalled uint64

	// Total number of function calls.
	_FunctionsCalled uint64

	// Total search for operator protocols.
	_ProtocolSearch uint64

	// Number of subscopes created.
	_ScopeCopy uint64
}

func (self *Stats) IncRowsScanned() {
	atomic.AddUint64(&self._RowsScanned, uint64(1))
}

func (self *Stats) IncPluginsCalled() {
	atomic.AddUint64(&self._PluginsCalled, uint64(1))
}

func (self *Stats) IncFunctionsCalled() {
	atomic.AddUint64(&self._FunctionsCalled, uint64(1))
}

func (self *Stats) IncProtocolSearch(i int) {
	atomic.AddUint64(&self._ProtocolSearch, uint64(i))
}

func (self *Stats) IncScopeCopy() {
	atomic.AddUint64(&self._ScopeCopy, uint64(1))
}

func (self *Stats) Snapshot() *ordereddict.Dict {
	return ordereddict.NewDict().
		Set("RowsScanned", atomic.LoadUint64(&self._RowsScanned)).
		Set("PluginsCalled", atomic.LoadUint64(&self._PluginsCalled)).
		Set("FunctionsCalled", atomic.LoadUint64(&self._FunctionsCalled)).
		Set("ProtocolSearch", atomic.LoadUint64(&self._ProtocolSearch)).
		Set("ScopeCopy", atomic.LoadUint64(&self._ScopeCopy))
}
