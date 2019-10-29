package vfilter

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
)

// Destructors are stored in the root of the scope stack so they may
// be reached from any nested scope and only destroyed when the root
// scope is destroyed.
type _destructors struct {
	mu sync.Mutex

	fn []func()
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

	vars      []Row
	functions map[string]FunctionInterface
	plugins   map[string]PluginGeneratorInterface

	bool        _BoolDispatcher
	eq          _EqDispatcher
	lt          _LtDispatcher
	add         _AddDispatcher
	sub         _SubDispatcher
	mul         _MulDispatcher
	div         _DivDispatcher
	membership  _MembershipDispatcher
	associative _AssociativeDispatcher
	regex       _RegexDispatcher

	Logger *log.Logger

	regexp_cache map[string]*regexp.Regexp

	context *Dict
}

func (self *Scope) GetContext(name string) Any {
	self.Lock()
	defer self.Unlock()

	res, pres := self.context.Get(name)
	if !pres {
		return nil
	}

	return res
}

func (self *Scope) SetContext(name string, value Any) {
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

func (self *Scope) Describe(type_map *TypeMap) *ScopeInformation {
	self.Lock()
	defer self.Unlock()

	result := &ScopeInformation{}
	for _, item := range self.plugins {
		result.Plugins = append(result.Plugins, item.Info(self, type_map))
	}

	for _, func_item := range self.functions {
		result.Functions = append(result.Functions, func_item.Info(self, type_map))
	}

	return result
}

// Tests two values for equality.
func (self *Scope) Eq(a Any, b Any) bool {
	return self.eq.Eq(self, a, b)
}

// Evaluate the truth value of a value.
func (self *Scope) Bool(a Any) bool {
	return self.bool.Bool(self, a)
}

// Is a less than b?
func (self *Scope) Lt(a Any, b Any) bool {
	return self.lt.Lt(self, a, b)
}

// Add a and b together.
func (self *Scope) Add(a Any, b Any) Any {
	return self.add.Add(self, a, b)
}

// Subtract b from a.
func (self *Scope) Sub(a Any, b Any) Any {
	return self.sub.Sub(self, a, b)
}

// Multiply a and b.
func (self *Scope) Mul(a Any, b Any) Any {
	return self.mul.Mul(self, a, b)
}

// Divide b into a.
func (self *Scope) Div(a Any, b Any) Any {
	return self.div.Div(self, a, b)
}

// Is a a member in b?
func (self *Scope) Membership(a Any, b Any) bool {
	return self.membership.Membership(self, a, b)
}

// Get the field member b from a (i.e. a.b).
func (self *Scope) Associative(a Any, b Any) (Any, bool) {
	res, pres := self.associative.Associative(self, a, b)
	return res, pres
}

func (self *Scope) GetMembers(a Any) []string {
	return self.associative.GetMembers(self, a)
}

// Does the regex a match object b.
func (self *Scope) Match(a Any, b Any) bool {
	return self.regex.Match(self, a, b)
}

/*
func (self Scope) Copy() *Scope {
	copy_of_vars := append([]Row{}, self.vars...)
	self.vars = copy_of_vars
	return &self
}
*/

func (self *Scope) Copy() *Scope {
	self.Lock()
	defer self.Unlock()

	return &Scope{
		functions:    self.functions,
		plugins:      self.plugins,
		Logger:       self.Logger,
		regexp_cache: self.regexp_cache,
		vars:         append([]Row{}, self.vars...),
		context:      self.context,

		bool:        self.bool,
		eq:          self.eq,
		lt:          self.lt,
		add:         self.add,
		sub:         self.sub,
		mul:         self.mul,
		div:         self.div,
		membership:  self.membership,
		associative: self.associative,
		regex:       self.regex,
	}
}

// Add various protocol implementations into this
// scope. Implementations must be one of the supported protocols or
// this function will panic.
func (self *Scope) AddProtocolImpl(implementations ...Any) *Scope {
	self.Lock()
	defer self.Unlock()

	for _, imp := range implementations {
		switch t := imp.(type) {
		case BoolProtocol:
			self.bool.AddImpl(t)
		case EqProtocol:
			self.eq.AddImpl(t)
		case LtProtocol:
			self.lt.AddImpl(t)
		case AddProtocol:
			self.add.AddImpl(t)
		case SubProtocol:
			self.sub.AddImpl(t)
		case MulProtocol:
			self.mul.AddImpl(t)
		case DivProtocol:
			self.div.AddImpl(t)
		case MembershipProtocol:
			self.membership.AddImpl(t)
		case AssociativeProtocol:
			self.associative.AddImpl(t)
		case RegexProtocol:
			self.regex.AddImpl(t)
		default:
			Debug(t)
			panic("Unsupported interface")
		}
	}

	return self
}

// Append the variables in Row to the scope.
func (self *Scope) AppendVars(row Row) *Scope {
	self.Lock()
	defer self.Unlock()

	result := self

	result.vars = append(result.vars, row)

	return result
}

// Add client function implementations to the scope. Queries using
// this scope can call these functions from within VQL queries.
func (self *Scope) AppendFunctions(functions ...FunctionInterface) *Scope {
	self.Lock()
	defer self.Unlock()

	result := self
	type_map := NewTypeMap()
	for _, function := range functions {
		info := function.Info(self, type_map)
		result.functions[info.Name] = function
	}

	return result
}

// Add plugins (data sources) to the scope. VQL queries may select
// from these newly added plugins.
func (self *Scope) AppendPlugins(plugins ...PluginGeneratorInterface) *Scope {
	self.Lock()
	defer self.Unlock()

	result := self
	type_map := NewTypeMap()
	for _, plugin := range plugins {
		info := plugin.Info(self, type_map)
		result.plugins[info.Name] = plugin
	}

	return result
}

func (self *Scope) Info(type_map *TypeMap, name string) (*PluginInfo, bool) {
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

	msg := fmt.Sprintf(format, a...)
	if self.Logger != nil {
		self.Logger.Print(msg)
	}
}

func (self *Scope) AddDestructor(fn func()) {
	destructors_any, _ := self.Resolve("__destructors")
	destructors, ok := destructors_any.(*_destructors)
	if ok {
		destructors.fn = append(destructors.fn, fn)
	} else {
		panic("Can not get destructors")
	}
}

func (self *Scope) Close() {
	destructors_any, _ := self.Resolve("__destructors")
	destructors, ok := destructors_any.(*_destructors)
	if ok {
		destructors.mu.Lock()
		defer destructors.mu.Unlock()

		// Destructors are called in reverse order to their
		// declerations.
		for i := len(destructors.fn) - 1; i >= 0; i-- {
			destructors.fn[i]()
		}

		destructors.fn = []func(){}
	}
}

// A factory for the default scope. This will add all built in
// protocols for commonly used code. Clients are expected to add their
// own specialized protocols, functions and plugins to specialize
// their scope objects.
func NewScope() *Scope {
	result := Scope{
		regexp_cache: make(map[string]*regexp.Regexp),
	}
	result.functions = make(map[string]FunctionInterface)
	result.plugins = make(map[string]PluginGeneratorInterface)
	result.context = NewDict()
	result.AppendVars(
		NewDict().
			Set("NULL", Null{}).
			Set("__destructors", &_destructors{}))

	// Protocol handlers.
	result.AddProtocolImpl(
		_NullAssociative{}, _NullEqProtocol{}, _NullBoolProtocol{},
		_BoolImpl{}, _BoolInt{}, _BoolString{}, _BoolSlice{}, _BoolDict{},
		_NumericLt{}, _StringLt{},
		_StringEq{}, _IntEq{}, _NumericEq{}, _ArrayEq{}, _DictEq{},
		_AddNull{}, _AddStrings{}, _AddInts{}, _AddFloats{}, _AddSlices{}, _AddSliceAny{},
		_StoredQueryAdd{},
		_SubInts{}, _SubFloats{},
		_SubstringMembership{},
		_MulInt{}, _NumericMul{},
		_NumericDiv{},
		_DictAssociative{},
		_SubstringRegex{}, _ArrayRegex{},
		_StoredQueryAssociative{}, _StoredQueryBool{},
		_ScopeAssociative{}, _LazyRowAssociative{},
	)

	// Built in functions.
	result.AppendFunctions(
		_DictFunc{},
		_Timestamp{},
		_SubSelectFunction{},
		_SplitFunction{},
		_IfFunction{},
		_GetFunction{},
		_EncodeFunction{},
		_CountFunction{},
		_MinFunction{},
		_MaxFunction{},
		_EnumerateFunction{},
	)

	result.AppendPlugins(
		_IfPlugin{},
		_FlattenPluginImpl{},
		_ChainPlugin{},
		_ForeachPluginImpl{},
		&GenericListPlugin{
			PluginName: "scope",
			Function: func(scope *Scope, args *Dict) []Row {
				return []Row{scope}
			},
		},
	)

	return &result
}

// Fetch the field from the scope variables.
func (self *Scope) Resolve(field string) (interface{}, bool) {
	self.Lock()
	defer self.Unlock()

	var default_value Any

	// Walk the scope stack in reverse so more recent vars shadow
	// older ones.
	for i := len(self.vars) - 1; i >= 0; i-- {
		subscope := self.vars[i]

		// Allow each subscope to specify a default. In the
		// end if a default was found then return Resolve as
		// present.
		element, pres := self.Associative(subscope, field)
		if pres {
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

func (self _ScopeAssociative) Applicable(a Any, b Any) bool {
	_, a_ok := a.(*Scope)
	_, b_ok := to_string(b)
	return a_ok && b_ok
}

func (self _ScopeAssociative) GetMembers(
	scope *Scope, a Any) []string {
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
	scope *Scope, a Any, b Any) (Any, bool) {
	b_str, ok := to_string(b)
	if !ok {
		return nil, false
	}

	a_scope, ok := a.(*Scope)
	if !ok {
		return nil, false
	}
	return a_scope.Resolve(b_str)
}
