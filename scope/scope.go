package scope

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/functions"
	"www.velocidex.com/golang/vfilter/plugins"
	"www.velocidex.com/golang/vfilter/protocols"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Destructors are attached to each scope in the stack - they are
// called when scope.Close() is called.
type _destructors struct {
	fn           []func()
	is_destroyed bool
	wg           sync.WaitGroup
}

/* The scope is a common environment passed to all plugins, functions
   and operators.

   The scope contains all the client specific code which velocifilter
   will use to actually execute the query. For example, clients may
   add new plugins (See PluginGeneratorInterface{}), functions (see
   FunctionInterface{}) or various protocol implementations to the
   scope prior to evaluating any queries. This is the main mechanism
   where clients may extend and specialize the VQL language.

   The scope also contains convenience functions allowing clients to
   execute available protocols.

   The scope may be populated with free variables that can be
   referenced by the query.
*/
type Scope struct {
	sync.Mutex

	Stats *types.Stats

	vars      []types.Row
	functions map[string]types.FunctionInterface
	plugins   map[string]types.PluginGeneratorInterface

	bool        protocols.BoolDispatcher
	eq          protocols.EqDispatcher
	lt          protocols.LtDispatcher
	gt          protocols.GtDispatcher
	add         protocols.AddDispatcher
	sub         protocols.SubDispatcher
	mul         protocols.MulDispatcher
	div         protocols.DivDispatcher
	membership  protocols.MembershipDispatcher
	associative protocols.AssociativeDispatcher
	regex       protocols.RegexDispatcher
	iterator    protocols.IterateDispatcher

	Logger *log.Logger

	// Very verbose debugging goes here - not generally useful
	// unless users try to debug VQL expressions.
	Tracer *log.Logger

	context *ordereddict.Dict

	stack_depth int

	// All children of this scope and a link to our parent.
	children map[*Scope]*Scope
	parent   *Scope

	// types.Any destructors attached to this scope.
	destructors _destructors
}

func (self *Scope) SetLogger(logger *log.Logger) {
	self.Logger = logger
}

func (self *Scope) SetTracer(logger *log.Logger) {
	self.Tracer = logger
}

func (self *Scope) GetLogger() *log.Logger {
	self.Lock()
	defer self.Unlock()

	return self.Logger
}

// Create a new scope from this scope.
func (self *Scope) NewScope() types.Scope {
	self.Lock()
	defer self.Unlock()

	// Make a copy of self
	result := &Scope{
		Stats:   &types.Stats{},
		context: ordereddict.NewDict(),
		vars: []types.Row{
			ordereddict.NewDict().
				Set("NULL", types.Null{}),
		},
		functions:   self.functions,
		plugins:     self.plugins,
		bool:        self.bool.Copy(),
		eq:          self.eq.Copy(),
		lt:          self.lt.Copy(),
		gt:          self.gt.Copy(),
		add:         self.add.Copy(),
		sub:         self.sub.Copy(),
		mul:         self.mul.Copy(),
		div:         self.div.Copy(),
		membership:  self.membership.Copy(),
		associative: self.associative.Copy(),
		regex:       self.regex.Copy(),
		iterator:    self.iterator.Copy(),
		Logger:      self.Logger,
		Tracer:      self.Tracer,
		children:    make(map[*Scope]*Scope),
	}

	return result
}

func (self *Scope) GetStats() *types.Stats {
	return self.Stats
}

func (self *Scope) GetContext(name string) (types.Any, bool) {
	self.Lock()
	defer self.Unlock()

	return self.context.Get(name)
}

func (self *Scope) ClearContext() {
	self.Lock()
	defer self.Unlock()

	self.context = ordereddict.NewDict()
	self.vars = append(self.vars, ordereddict.NewDict().
		Set("NULL", types.Null{}))
}

func (self *Scope) SetContext(name string, value types.Any) {
	self.Lock()
	defer self.Unlock()
	self.context.Set(name, value)
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
	return fmt.Sprintf("Current Scope is: %s", strings.Join(my_vars, ", "))
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
	self.Lock()
	defer self.Unlock()

	result := &types.ScopeInformation{}
	for _, item := range self.plugins {
		result.Plugins = append(result.Plugins, item.Info(self, type_map))
	}

	for _, func_item := range self.functions {
		result.Functions = append(result.Functions, func_item.Info(self, type_map))
	}

	return result
}

// Tests two values for equality.
func (self *Scope) Eq(a types.Any, b types.Any) bool {
	return self.eq.Eq(self, a, b)
}

// Evaluate the truth value of a value.
func (self *Scope) Bool(a types.Any) bool {
	return self.bool.Bool(self, a)
}

// Is a less than b?
func (self *Scope) Lt(a types.Any, b types.Any) bool {
	return self.lt.Lt(self, a, b)
}

func (self *Scope) Gt(a types.Any, b types.Any) bool {
	return self.gt.Gt(self, a, b)
}

// Add a and b together.
func (self *Scope) Add(a types.Any, b types.Any) types.Any {
	return self.add.Add(self, a, b)
}

// Subtract b from a.
func (self *Scope) Sub(a types.Any, b types.Any) types.Any {
	return self.sub.Sub(self, a, b)
}

// Multiply a and b.
func (self *Scope) Mul(a types.Any, b types.Any) types.Any {
	return self.mul.Mul(self, a, b)
}

// Divide b into a.
func (self *Scope) Div(a types.Any, b types.Any) types.Any {
	return self.div.Div(self, a, b)
}

// Is a a member in b?
func (self *Scope) Membership(a types.Any, b types.Any) bool {
	return self.membership.Membership(self, a, b)
}

// Get the field member b from a (i.e. a.b).
func (self *Scope) Associative(a types.Any, b types.Any) (types.Any, bool) {
	res, pres := self.associative.Associative(self, a, b)
	return res, pres
}

func (self *Scope) GetMembers(a types.Any) []string {
	return self.associative.GetMembers(self, a)
}

// Does the regex a match object b.
func (self *Scope) Match(a types.Any, b types.Any) bool {
	return self.regex.Match(self, a, b)
}

func (self *Scope) Iterate(ctx context.Context, a types.Any) <-chan types.Row {
	return self.iterator.Iterate(ctx, self, a)
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
	child_scope := &Scope{
		functions: self.functions,
		plugins:   self.plugins,
		Logger:    self.Logger,
		Tracer:    self.Tracer,
		vars:      append([]types.Row(nil), self.vars...),
		context:   self.context,
		Stats:     self.Stats,

		// Not sure if we have to make a full copy here? It is
		// faster not to.
		/*
			bool:        self.bool.Copy(),
			eq:          self.eq.Copy(),
			lt:          self.lt.Copy(),
			gt:          self.gt.Copy(),
			add:         self.add.Copy(),
			sub:         self.sub.Copy(),
			mul:         self.mul.Copy(),
			div:         self.div.Copy(),
			membership:  self.membership.Copy(),
			associative: self.associative.Copy(),
			regex:       self.regex.Copy(),
			iterator:    self.iterator.Copy(),
		*/

		bool:        self.bool,
		eq:          self.eq,
		lt:          self.lt,
		gt:          self.gt,
		add:         self.add,
		sub:         self.sub,
		mul:         self.mul,
		div:         self.div,
		membership:  self.membership,
		associative: self.associative,
		regex:       self.regex,
		iterator:    self.iterator,

		stack_depth: self.stack_depth + 1,
		children:    make(map[*Scope]*Scope),
		parent:      self,
	}

	// Remember our children.
	self.children[child_scope] = child_scope

	return child_scope
}

// Add various protocol implementations into this
// scope. Implementations must be one of the supported protocols or
// this function will panic.
func (self *Scope) AddProtocolImpl(implementations ...types.Any) types.Scope {
	self.Lock()
	defer self.Unlock()

	for _, imp := range implementations {
		switch t := imp.(type) {
		case protocols.BoolProtocol:
			self.bool.AddImpl(t)
		case protocols.EqProtocol:
			self.eq.AddImpl(t)
		case protocols.LtProtocol:
			self.lt.AddImpl(t)
		case protocols.GtProtocol:
			self.gt.AddImpl(t)
		case protocols.AddProtocol:
			self.add.AddImpl(t)
		case protocols.SubProtocol:
			self.sub.AddImpl(t)
		case protocols.MulProtocol:
			self.mul.AddImpl(t)
		case protocols.DivProtocol:
			self.div.AddImpl(t)
		case protocols.MembershipProtocol:
			self.membership.AddImpl(t)
		case protocols.AssociativeProtocol:
			self.associative.AddImpl(t)
		case protocols.RegexProtocol:
			self.regex.AddImpl(t)
		case protocols.IterateProtocol:
			self.iterator.AddImpl(t)
		default:
			utils.Debug(t)
			panic("Unsupported interface")
		}
	}

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
	self.Lock()
	defer self.Unlock()

	result := self
	for _, function := range functions {
		info := function.Info(self, nil)
		result.functions[info.Name] = function
	}

	return result
}

// Add plugins (data sources) to the scope. VQL queries may select
// from these newly added plugins.
func (self *Scope) AppendPlugins(plugins ...types.PluginGeneratorInterface) types.Scope {
	self.Lock()
	defer self.Unlock()

	result := self
	for _, plugin := range plugins {
		info := plugin.Info(self, nil)
		result.plugins[info.Name] = plugin
	}

	return result
}

func (self *Scope) GetFunction(name string) (types.FunctionInterface, bool) {
	res, pres := self.functions[name]
	return res, pres
}

func (self *Scope) GetPlugin(name string) (types.PluginGeneratorInterface, bool) {
	res, pres := self.plugins[name]
	return res, pres
}

func (self *Scope) Info(type_map *types.TypeMap, name string) (*types.PluginInfo, bool) {
	self.Lock()
	defer self.Unlock()

	if plugin, pres := self.plugins[name]; pres {
		return plugin.Info(self, type_map), true
	}

	return nil, false
}

func (self *Scope) Log(format string, a ...interface{}) {
	self.Lock()
	defer self.Unlock()

	if self.Logger != nil {
		msg := fmt.Sprintf(format, a...)
		self.Logger.Print(msg)
	}
}

func (self *Scope) Trace(format string, a ...interface{}) {
	self.Lock()
	defer self.Unlock()

	if self.Tracer != nil {
		msg := fmt.Sprintf(format, a...)
		self.Tracer.Print(msg)
	}
}

// Adding a destructor to the current scope will call it when any
// parent scopes are closed.
func (self *Scope) AddDestructor(fn func()) error {
	self.Lock()
	self.Unlock()

	// Scope is already destroyed - call the destructor now.
	if self.destructors.is_destroyed {
		return errors.New("Scope already closed")
	} else {
		self.destructors.fn = append(self.destructors.fn, fn)
		return nil
	}
}

// Closing a scope will also close all its children. Note that
// destructors may use the scope so we can not lock it for the
// duration.
func (self *Scope) Close() {
	self.Lock()

	// We need to call child.Close() without a lock since
	// child.Close() will attempt to remove themselves from our
	// own child list and will grab the lock.
	children := make([]*Scope, 0, len(self.children))
	for _, child := range self.children {
		children = append(children, child)
	}

	parent := self.parent

	// Stop new destructors from appearing.
	self.destructors.is_destroyed = true

	// Remove destructors from list so they are not run again.
	ds := append(self.destructors.fn[:0:0], self.destructors.fn...)
	self.destructors.fn = []func(){}

	// Unlock the scope and start running the
	// destructors. Destructors may actually add new destructors
	// to this scope but hopefully the parent scope will be
	// deleted later.
	self.Unlock()

	// This has to be done without a lock since the child needs to
	// access us.
	for _, child := range children {
		child.Close()
	}

	// Remove ourselves from our parent.
	if parent != nil {
		parent.Lock()
		delete(parent.children, self)
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
	result := Scope{
		children: make(map[*Scope]*Scope),
	}
	result.functions = make(map[string]types.FunctionInterface)
	result.plugins = make(map[string]types.PluginGeneratorInterface)
	result.context = ordereddict.NewDict()
	result.Stats = &types.Stats{}
	result.AppendVars(
		ordereddict.NewDict().
			Set("NULL", types.Null{}))

	// Get Builtin protocols, functions, and plugins
	result.AddProtocolImpl(protocols.GetBuiltinTypes()...)
	result.AppendFunctions(functions.GetBuiltinFunctions()...)
	result.AppendPlugins(plugins.GetBuiltinPlugins()...)

	result.AppendFunctions(_GetVersion{})

	return &result
}

// Fetch the field from the scope variables.
func (self *Scope) Resolve(field string) (interface{}, bool) {
	self.Lock()
	defer self.Unlock()

	var default_value types.Any

	// Walk the scope stack in reverse so more recent vars shadow
	// older ones.
	for i := len(self.vars) - 1; i >= 0; i-- {
		subscope := self.vars[i]

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
