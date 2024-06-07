package scope

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/aggregators"
	"www.velocidex.com/golang/vfilter/functions"
	"www.velocidex.com/golang/vfilter/materializer"
	"www.velocidex.com/golang/vfilter/plugins"
	"www.velocidex.com/golang/vfilter/protocols"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

var (
	idx uint64
)

// Destructors are attached to each scope in the stack - they are
// called when scope.Close() is called.
type _destructors struct {
	mu           sync.Mutex
	fn           []func()
	is_destroyed bool
	wg           sync.WaitGroup
}

func (self *_destructors) SetDestroyed() {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.is_destroyed = true
}

func (self *_destructors) IsDestroyed() bool {
	self.mu.Lock()
	defer self.mu.Unlock()

	return self.is_destroyed
}

func (self *_destructors) AddDestructor(fn func()) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.fn = append(self.fn, fn)
}

func (self *_destructors) RemoveDestructors() []func() {
	self.mu.Lock()
	defer self.mu.Unlock()

	result := append(self.fn[:0:0], self.fn...)
	self.fn = nil
	return result
}

/*
The scope is a common environment passed to all plugins, functions and
operators.

The scope contains all the client specific code which velocifilter
will use to actually execute the query. For example, clients may add
new plugins (See PluginGeneratorInterface{}), functions (see
FunctionInterface{}) or various protocol implementations to the scope
prior to evaluating any queries. This is the main mechanism where
clients may extend and specialize the VQL language.

The scope also contains convenience functions allowing clients to
execute available protocols.

The scope may be populated with free variables that can be referenced
by the query.
*/
type Scope struct {
	sync.Mutex

	vars []types.Row

	// The dispatcher contains all items that are constant for the
	// entire query evaluation. Pulling it into its own object
	// make scope copy very cheap.
	dispatcher *protocolDispatcher

	ag_context types.AggregatorCtx

	stack_depth int

	// All children of this scope and a link to our parent.
	children               []*Scope
	children_grabage_count int
	parent                 *Scope

	// If enabled we explain this scope and its children
	enable_explainer bool

	// types.Any destructors attached to this scope.
	destructors _destructors

	throttler types.Throttler

	id uint64
}

func (self *Scope) SetLogger(logger *log.Logger) {
	self.dispatcher.Logger = logger
}

func (self *Scope) SetAggregatorCtx(ctx types.AggregatorCtx) {
	self.Lock()
	defer self.Unlock()

	if ctx == nil {
		ctx = aggregators.NewAggregatorCtx()
	}

	self.ag_context = ctx
}

// Get the aggregator context from the scope or one of its parents.
func (self *Scope) GetAggregatorCtx() types.AggregatorCtx {
	self.Lock()
	defer self.Unlock()

	if self.ag_context == nil {
		if self.parent != nil {
			return self.parent.GetAggregatorCtx()
		}
		self.ag_context = aggregators.NewAggregatorCtx()
	}

	return self.ag_context
}

func (self *Scope) SetTracer(logger *log.Logger) {
	self.dispatcher.Tracer = logger
}

func (self *Scope) GetLogger() *log.Logger {
	return self.dispatcher.GetLogger()
}

// Create a new scope from this scope.
func (self *Scope) NewScope() types.Scope {
	self.Lock()
	defer self.Unlock()

	// Make a copy of self
	result := &Scope{
		vars: []types.Row{
			ordereddict.NewDict().
				Set("NULL", types.Null{}),
		},
		dispatcher: self.dispatcher.Copy(),
		throttler:  self.throttler,
		ag_context: NewAggregatorCtx(),
		id:         NextId(),
	}

	return result
}

func (self *Scope) GetStats() *types.Stats {
	return self.dispatcher.GetStats()
}

func (self *Scope) GetContext(name string) (types.Any, bool) {
	return self.dispatcher.GetContext(name)
}

func (self *Scope) ClearContext() {
	self.Lock()
	defer self.Unlock()

	// The dispatcher is normally shared between all scopes and their
	// children, however when setting a new context, we need to create
	// a new dispatcher object to hold the new context.
	self.dispatcher = self.dispatcher.WithNewContext()
	self.dispatcher.SetContext(ordereddict.NewDict())
}

func (self *Scope) SetContext(name string, value types.Any) {
	self.Lock()
	defer self.Unlock()

	self.dispatcher.SetContextValue(name, value)
}

func (self *Scope) PrintVars() string {
	self.Lock()
	defer self.Unlock()

	my_vars := []string{}
	for _, vars := range self.vars {
		keys := []string{}
		for _, k := range self.GetMembers(vars) {
			keys = append(keys, k)
		}
		my_vars = append(my_vars, "["+strings.Join(keys, ", ")+"]")
	}
	return strings.Join(my_vars, ", ")
}

/*
func (self *Scope) Keys() []string {
	self.Lock()
	defer self.Unlock()

	result := []string{}

	for _, vars := range self.vars {
		for _, k := range self.GetMembers(vars) {
			if !InString(&result, k) {
				result = append(result, k)
			}
		}
	}

	return result
}

*/

func (self *Scope) Describe(type_map *types.TypeMap) *types.ScopeInformation {
	return self.dispatcher.Describe(self, type_map)
}

func (self *Scope) SetThrottler(t types.Throttler) {
	self.Lock()
	self.throttler = t
	self.Unlock()
	self.AddDestructor(t.Close)
}

func (self *Scope) ChargeOp() {
	if self.throttler != nil {
		self.throttler.ChargeOp()
	}
}

func (self *Scope) CheckForOverflow() bool {
	self.Lock()
	vars := self.vars[:]
	self.Unlock()

	if self.stack_depth < 1000 {
		return false
	}

	// Log the query for overflow
	query, _ := self._ResolveVars("$Query", vars)
	self.Log("Stack Overflow: %v", query)

	return true
}

// Tests two values for equality.
func (self *Scope) Eq(a types.Any, b types.Any) bool {
	return self.dispatcher.eq.Eq(self, a, b)
}

// Evaluate the truth value of a value.
func (self *Scope) Bool(a types.Any) bool {
	ctx := context.Background()
	return self.dispatcher.bool.Bool(ctx, self, a)
}

// Is a less than b?
func (self *Scope) Lt(a types.Any, b types.Any) bool {
	return self.dispatcher.lt.Lt(self, a, b)
}

func (self *Scope) Gt(a types.Any, b types.Any) bool {
	return self.dispatcher.gt.Gt(self, a, b)
}

// Add a and b together.
func (self *Scope) Add(a types.Any, b types.Any) types.Any {
	return self.dispatcher.add.Add(self, a, b)
}

// Subtract b from a.
func (self *Scope) Sub(a types.Any, b types.Any) types.Any {
	return self.dispatcher.sub.Sub(self, a, b)
}

// Multiply a and b.
func (self *Scope) Mul(a types.Any, b types.Any) types.Any {
	return self.dispatcher.mul.Mul(self, a, b)
}

// Divide b into a.
func (self *Scope) Div(a types.Any, b types.Any) types.Any {
	return self.dispatcher.div.Div(self, a, b)
}

// Is a a member in b?
func (self *Scope) Membership(a types.Any, b types.Any) bool {
	return self.dispatcher.membership.Membership(self, a, b)
}

// Get the field member b from a (i.e. a.b).
func (self *Scope) Associative(a types.Any, b types.Any) (types.Any, bool) {
	res, pres := self.dispatcher.associative.Associative(self, a, b)
	return res, pres
}

func (self *Scope) GetMembers(a types.Any) []string {
	return self.dispatcher.associative.GetMembers(self, a)
}

// Does the regex a match object b.
func (self *Scope) Match(a types.Any, b types.Any) bool {
	return self.dispatcher.regex.Match(self, a, b)
}

func (self *Scope) Iterate(ctx context.Context, a types.Any) <-chan types.Row {
	return self.dispatcher.iterator.Iterate(ctx, self, a)
}

func (self *Scope) Materialize(ctx context.Context,
	name string, query types.StoredQuery) types.StoredQuery {
	return self.dispatcher.Materializer.Materialize(ctx, self, name, query)
}

func (self *Scope) StackDepth() int {
	self.Lock()
	defer self.Unlock()

	return self.stack_depth
}

func (self *Scope) Copy() types.Scope {
	self.Lock()
	defer self.Unlock()

	self.GetStats().IncScopeCopy()

	// Fast make copy
	var_copy := make([]types.Row, len(self.vars))
	copy(var_copy, self.vars)

	child_scope := &Scope{
		dispatcher:       self.dispatcher,
		vars:             var_copy,
		stack_depth:      self.stack_depth + 1,
		parent:           self,
		enable_explainer: self.enable_explainer,
		throttler:        self.throttler,
		ag_context:       nil, //  Search for context in our parent.
		id:               NextId(),
	}

	// Compact the children list lazily
	if self.children_grabage_count > 10 {
		new_children := make([]*Scope, 0, len(self.children))
		for _, c := range self.children {
			if c != nil {
				new_children = append(new_children, c)
			}
		}
		self.children = new_children
		self.children_grabage_count = 0
	}

	// Remember our children.
	if len(self.children) > 1000 {
		fmt.Printf("Copying scope of %v children - this is probably a bug!!!\n%v\n",
			len(self.children), string(debug.Stack()))
	}
	self.children = append(self.children, child_scope)

	return child_scope
}

// Add various protocol implementations into this
// scope. Implementations must be one of the supported protocols or
// this function will panic.
func (self *Scope) AddProtocolImpl(implementations ...types.Any) types.Scope {
	self.dispatcher.AddProtocolImpl(implementations...)
	return self
}

// Append the variables in types.Row to the scope.
func (self *Scope) AppendVars(row types.Row) types.Scope {
	self.Lock()
	defer self.Unlock()

	result := self

	result.vars = append(result.vars, row)

	return result
}

// Add client function implementations to the scope. Queries using
// this scope can call these functions from within VQL queries.
func (self *Scope) AppendFunctions(functions ...types.FunctionInterface) types.Scope {
	self.dispatcher.AppendFunctions(self, functions...)
	return self
}

// Add plugins (data sources) to the scope. VQL queries may select
// from these newly added plugins.
func (self *Scope) AppendPlugins(plugins ...types.PluginGeneratorInterface) types.Scope {
	self.dispatcher.AppendPlugins(self, plugins...)
	return self
}

func (self *Scope) GetFunction(name string) (types.FunctionInterface, bool) {
	return self.dispatcher.GetFunction(name)
}

func (self *Scope) GetPlugin(name string) (types.PluginGeneratorInterface, bool) {
	return self.dispatcher.GetPlugin(name)
}

func (self *Scope) Info(type_map *types.TypeMap, name string) (*types.PluginInfo, bool) {
	return self.dispatcher.Info(self, type_map, name)
}

func (self *Scope) Log(format string, a ...interface{}) {
	self.dispatcher.Log(format, a...)
}

func (self *Scope) Error(format string, a ...interface{}) {
	self.dispatcher.Log("ERROR:"+format, a...)
}

func (self *Scope) Debug(format string, a ...interface{}) {
	self.dispatcher.Log("DEBUG:"+format, a...)
}

func (self *Scope) Warn(format string, a ...interface{}) {
	self.dispatcher.Log("WARN:"+format, a...)
}

func (self *Scope) Trace(format string, a ...interface{}) {
	self.dispatcher.Trace("TRACE:"+format, a...)
}

func (self *Scope) Sort(
	ctx context.Context, scope types.Scope, input <-chan types.Row,
	key string, desc bool) <-chan types.Row {
	return self.dispatcher.Sorter.Sort(ctx, scope, input, key, desc)
}

func (self *Scope) Group(
	ctx context.Context, scope types.Scope, actor types.GroupbyActor) <-chan types.Row {
	return self.dispatcher.Grouper.Group(ctx, scope, actor)
}

// Adding a destructor to the current scope will call it when any
// parent scopes are closed.
func (self *Scope) AddDestructor(fn func()) error {
	self.Lock()
	defer self.Unlock()

	// Scope is already destroyed - call the destructor now.
	if self.destructors.IsDestroyed() {
		return errors.New("Scope already closed")
	} else {
		self.destructors.AddDestructor(fn)
		return nil
	}
}

func (self *Scope) IsClosed() bool {
	self.Lock()
	defer self.Unlock()

	return self.destructors.IsDestroyed()
}

// Closing a scope will also close all its children. Note that
// destructors may use the scope so we can not lock it for the
// duration.
func (self *Scope) Close() {
	self.Lock()

	// We need to call child.Close() without a lock since
	// child.Close() will attempt to remove themselves from our
	// own child list and will grab the lock.
	children := append([]*Scope{}, self.children...)

	parent := self.parent

	// Stop new destructors from appearing.
	self.destructors.SetDestroyed()

	// Remove destructors from list so they are not run again.
	ds := self.destructors.RemoveDestructors()

	// Unlock the scope and start running the
	// destructors. Destructors may actually add new destructors
	// to this scope but hopefully the parent scope will be
	// deleted later.
	self.Unlock()

	// This has to be done without a lock since the child needs to
	// access us.
	for _, child := range children {
		if child != nil {
			child.Close()
		}
	}

	// Remove ourselves from our parent.
	if parent != nil && parent != self {
		parent.Lock()

		// Clear the child in the parent list and increment its garbage count
		for idx, c := range parent.children {
			if c != nil && self.id == c.id {
				parent.children[idx] = nil
				parent.children_grabage_count++
			}
		}
		parent.Unlock()
	}

	// Destructors are called in reverse order to their
	// declerations.
	for i := len(ds) - 1; i >= 0; i-- {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
		go func() {
			ds[i]()
			cancel()
		}()

		select {
		// Wait a maximum 60 seconds for the
		// destructor before moving on.
		case <-ctx.Done():
		}
	}
}

// A factory for the default scope. This will add all built in
// protocols for commonly used code. Clients are expected to add their
// own specialized protocols, functions and plugins to specialize
// their scope objects.
func NewScope() *Scope {
	dispatcher := newprotocolDispatcher()

	result := &Scope{
		dispatcher: dispatcher,
		ag_context: NewAggregatorCtx(),
		id:         NextId(),
	}

	// Add Builtin protocols, functions, and plugins
	dispatcher.AddProtocolImpl(protocols.GetBuiltinTypes()...)
	dispatcher.AppendFunctions(result, functions.GetBuiltinFunctions()...)
	dispatcher.AppendPlugins(result, plugins.GetBuiltinPlugins()...)
	dispatcher.AppendFunctions(result, _GetVersion{})

	result.AppendVars(
		ordereddict.NewDict().
			Set("NULL", types.Null{}))

	dispatcher.AddProtocolImpl(materializer.InMemoryMatrializer{})

	return result
}

func (self *Scope) String() string {
	return "<Scope>"
}

func (self *Scope) SetSorter(sorter types.Sorter) {
	self.dispatcher.SetSorter(sorter)
}

func (self *Scope) SetGrouper(grouper types.Grouper) {
	self.dispatcher.SetGrouper(grouper)
}

func (self *Scope) SetMaterializer(materializer types.ScopeMaterializer) {
	self.dispatcher.SetMaterializer(materializer)
}

func (self *Scope) SetExplainer(explainer types.Explainer) {
	self.dispatcher.SetExplainer(explainer)
}

func (self *Scope) EnableExplain() {
	self.Lock()
	defer self.Unlock()

	self.enable_explainer = true
}

func (self *Scope) Explainer() types.Explainer {
	self.Lock()
	defer self.Unlock()
	if self.enable_explainer {
		return self.dispatcher.Explainer()
	}

	return NULL_EXPLAINER
}

// Fetch the field from the scope variables.
func (self *Scope) Resolve(field string) (interface{}, bool) {
	if self.CheckForOverflow() {
		return types.Null{}, false
	}

	// Snapshot the vars to remove the need to lock the scope for so
	// long.
	self.Lock()
	vars := self.vars[:]
	self.Unlock()

	return self._ResolveVars(field, vars)
}

func (self *Scope) _ResolveVars(field string, vars []types.Row) (interface{}, bool) {
	var default_value types.Any

	// Walk the scope stack in reverse so more recent vars shadow
	// older ones.
	for i := len(vars) - 1; i >= 0; i-- {
		subscope := vars[i]

		// Allow each subscope to specify a default. In the
		// end if a default was found then return Resolve as
		// present.
		element, pres := self.Associative(subscope, field)
		if pres {
			// Do not allow go nil to be emitted into the
			// query - this leads to various panics and
			// does not interact well with the reflect
			// package. It is better to emit vfilter types.Null{}
			// objects which do the right thing when
			// participating in protocols.
			if element == nil {
				element = types.Null{}
			}
			return element, true
		}

		// Default value of inner most scope will prevail.
		if element != nil && default_value == nil {
			default_value = element
		}
	}

	return default_value, default_value != nil
}

// Scope Associative
type _ScopeAssociative struct{}

func (self _ScopeAssociative) Applicable(a types.Any, b types.Any) bool {
	_, a_ok := a.(*Scope)
	_, b_ok := utils.ToString(b)
	return a_ok && b_ok
}

func (self _ScopeAssociative) GetMembers(
	scope *Scope, a types.Any) []string {
	seen := make(map[string]bool)
	var result []string

	a_scope, ok := a.(Scope)
	if ok {
		for _, vars := range scope.vars {
			for _, member := range a_scope.GetMembers(vars) {
				seen[member] = true
			}
		}

		for k, _ := range seen {
			result = append(result, k)
		}
	}
	return result
}

func (self _ScopeAssociative) Associative(
	scope *Scope, a types.Any, b types.Any) (types.Any, bool) {
	b_str, ok := utils.ToString(b)
	if !ok {
		return nil, false
	}

	a_scope, ok := a.(*Scope)
	if !ok {
		return nil, false
	}
	return a_scope.Resolve(b_str)
}

func NextId() uint64 {
	return atomic.AddUint64(&idx, 1)
}
