package types

import (
	"context"
	"log"
	"runtime"
	"sync/atomic"

	"github.com/Velocidex/ordereddict"
)

// A scope is passed inside the evaluation context.
type Scope interface {

	// Duplicate the scope to a completely new scope - this is a
	// deep copy not a subscope!  Very rarely used.
	NewScope() Scope

	// Copy the scope and create a subscope child.
	Copy() Scope

	GetStats() *Stats

	// The scope context is a global k/v store
	GetContext(name string) (Any, bool)
	SetContext(name string, value Any)
	ClearContext()

	// Extract debug string about the current scope state.
	PrintVars() string

	// Scope manages the protocols
	Bool(a Any) bool
	Eq(a Any, b Any) bool
	Lt(a Any, b Any) bool
	Gt(a Any, b Any) bool
	Add(a Any, b Any) Any
	Sub(a Any, b Any) Any
	Mul(a Any, b Any) Any
	Div(a Any, b Any) Any
	Membership(a Any, b Any) bool
	Associative(a Any, b Any) (Any, bool)
	GetMembers(a Any) []string
	Match(a Any, b Any) bool
	Iterate(ctx context.Context, a Any) <-chan Row

	// We can program the scope's protocols
	AddProtocolImpl(implementations ...Any) Scope
	AppendVars(row Row) Scope
	AppendFunctions(functions ...FunctionInterface) Scope
	AppendPlugins(plugins ...PluginGeneratorInterface) Scope

	GetFunction(name string) (FunctionInterface, bool)
	GetPlugin(name string) (PluginGeneratorInterface, bool)

	// Logging
	SetLogger(logger *log.Logger)
	SetTracer(logger *log.Logger)
	GetLogger() *log.Logger

	Log(format string, a ...interface{})
	Trace(format string, a ...interface{})

	// Introspection
	GetSimilarPlugins(name string) []string
	Describe(type_map *TypeMap) *ScopeInformation

	// Destructors are called when the scope is Close()
	AddDestructor(fn func())
	Close()

	// Resolve a variable
	Resolve(field string) (interface{}, bool)

	StackDepth() int
}

// Utilities to do with scope.
func RecoverVQL(scope Scope) {
	r := recover()
	if r != nil {
		scope.Log("PANIC: %v\n", r)
		buffer := make([]byte, 4096)
		n := runtime.Stack(buffer, false /* all */)
		scope.Log("%s", buffer[:n])
	}
}

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
