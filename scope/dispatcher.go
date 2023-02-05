package scope

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"

	"github.com/Velocidex/ordereddict"
	"www.velocidex.com/golang/vfilter/grouper"
	"www.velocidex.com/golang/vfilter/materializer"
	"www.velocidex.com/golang/vfilter/protocols"
	sorter "www.velocidex.com/golang/vfilter/sort"
	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Pull out the dispatcher into its own object to prevent needing to
// copy it when creating a subscope.
type protocolDispatcher struct {
	sync.Mutex

	functions map[string]types.FunctionInterface
	plugins   map[string]types.PluginGeneratorInterface

	Stats *types.Stats

	// Protocol dispatchers control operators.
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

	// Sorters allow VQL to sort result sets.
	Sorter       types.Sorter
	Grouper      types.Grouper
	Materializer types.ScopeMaterializer

	Logger *log.Logger

	// Very verbose debugging goes here - not generally useful
	// unless users try to debug VQL expressions.
	Tracer *log.Logger

	context *ordereddict.Dict
}

func (self *protocolDispatcher) SetContext(context *ordereddict.Dict) {
	self.Lock()
	self.context = context
	self.Unlock()
}

func (self *protocolDispatcher) SetSorter(sorter types.Sorter) {
	self.Lock()
	self.Sorter = sorter
	self.Unlock()
}

func (self *protocolDispatcher) SetGrouper(grouper types.Grouper) {
	self.Lock()
	self.Grouper = grouper
	self.Unlock()
}

func (self *protocolDispatcher) SetMaterializer(materializer types.ScopeMaterializer) {
	self.Lock()
	self.Materializer = materializer
	self.Unlock()
}

func (self *protocolDispatcher) SetContextValue(name string, value types.Any) {
	self.Lock()
	defer self.Unlock()
	self.context.Set(name, value)
}

func (self *protocolDispatcher) GetContext(name string) (types.Any, bool) {
	self.Lock()
	defer self.Unlock()

	return self.context.Get(name)
}

func (self *protocolDispatcher) GetLogger() *log.Logger {
	self.Lock()
	defer self.Unlock()

	return self.Logger
}

func (self *protocolDispatcher) GetStats() *types.Stats {
	self.Lock()
	defer self.Unlock()

	return self.Stats
}

func (self *protocolDispatcher) Describe(scope *Scope, type_map *types.TypeMap) *types.ScopeInformation {
	self.Lock()
	defer self.Unlock()

	result := &types.ScopeInformation{}
	for _, item := range self.plugins {
		result.Plugins = append(result.Plugins, item.Info(scope, type_map))
	}

	for _, func_item := range self.functions {
		result.Functions = append(result.Functions, func_item.Info(scope, type_map))
	}

	return result
}

func (self *protocolDispatcher) WithNewContext() *protocolDispatcher {
	return &protocolDispatcher{
		Stats:        &types.Stats{},
		context:      ordereddict.NewDict(),
		functions:    self.functions,
		plugins:      self.plugins,
		bool:         self.bool,
		eq:           self.eq,
		lt:           self.lt,
		gt:           self.gt,
		add:          self.add,
		sub:          self.sub,
		mul:          self.mul,
		div:          self.div,
		membership:   self.membership,
		associative:  self.associative,
		regex:        self.regex,
		iterator:     self.iterator,
		Sorter:       self.Sorter,
		Grouper:      self.Grouper,
		Materializer: self.Materializer,
		Logger:       self.Logger,
		Tracer:       self.Tracer,
	}
}

func (self *protocolDispatcher) Copy() *protocolDispatcher {
	function_copy := make(map[string]types.FunctionInterface)
	for k, v := range self.functions {
		function_copy[k] = v
	}

	plugins_copy := make(map[string]types.PluginGeneratorInterface)
	for k, v := range self.plugins {
		plugins_copy[k] = v
	}

	return &protocolDispatcher{
		Stats:        &types.Stats{},
		context:      ordereddict.NewDict(),
		functions:    function_copy,
		plugins:      plugins_copy,
		bool:         self.bool.Copy(),
		eq:           self.eq.Copy(),
		lt:           self.lt.Copy(),
		gt:           self.gt.Copy(),
		add:          self.add.Copy(),
		sub:          self.sub.Copy(),
		mul:          self.mul.Copy(),
		div:          self.div.Copy(),
		membership:   self.membership.Copy(),
		associative:  self.associative.Copy(),
		regex:        self.regex.Copy(),
		iterator:     self.iterator.Copy(),
		Sorter:       self.Sorter,
		Grouper:      self.Grouper,
		Materializer: self.Materializer,
		Logger:       self.Logger,
		Tracer:       self.Tracer,
	}
}

func (self *protocolDispatcher) AppendPlugins(
	scope *Scope, plugins ...types.PluginGeneratorInterface) {
	self.Lock()
	defer self.Unlock()

	result := self
	for _, plugin := range plugins {
		info := plugin.Info(scope, nil)
		result.plugins[info.Name] = plugin
	}
}

func (self *protocolDispatcher) AppendFunctions(
	scope *Scope, functions ...types.FunctionInterface) {
	self.Lock()
	defer self.Unlock()

	result := self
	for _, function := range functions {
		info := function.Info(scope, nil)
		result.functions[info.Name] = function
	}
}

func (self *protocolDispatcher) GetFunction(name string) (types.FunctionInterface, bool) {
	res, pres := self.functions[name]
	return res, pres
}

func (self *protocolDispatcher) GetPlugin(name string) (types.PluginGeneratorInterface, bool) {
	res, pres := self.plugins[name]
	return res, pres
}

func (self *protocolDispatcher) Info(scope *Scope,
	type_map *types.TypeMap, name string) (*types.PluginInfo, bool) {
	self.Lock()
	defer self.Unlock()

	if plugin, pres := self.plugins[name]; pres {
		return plugin.Info(scope, type_map), true
	}

	return nil, false
}

func (self *protocolDispatcher) Log(format string, a ...interface{}) {
	self.Lock()
	logger := self.Logger
	self.Unlock()

	if logger != nil {
		msg := fmt.Sprintf(format, a...)
		logger.Print(msg)
	}
}

func (self *protocolDispatcher) Trace(format string, a ...interface{}) {
	self.Lock()
	defer self.Unlock()

	if self.Tracer != nil {
		msg := fmt.Sprintf(format, a...)
		self.Tracer.Print(msg)
	}
}

// Add various protocol implementations into this
// scope. Implementations must be one of the supported protocols or
// this function will panic.
func (self *protocolDispatcher) AddProtocolImpl(implementations ...types.Any) {
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
			panic(fmt.Sprintf("Unsupported interface: %T", imp))
		}
	}
}

// Get a list of similar sounding plugins.
func (self *protocolDispatcher) GetSimilarPlugins(name string) []string {
	result := []string{}
	parts := strings.Split(name, "_")

	self.Lock()
	defer self.Unlock()

	for _, part := range parts {
		for k, _ := range self.plugins {
			if strings.Contains(k, part) && !utils.InString(&result, k) {
				result = append(result, k)
			}
		}
	}

	sort.Strings(result)

	return result
}

func newprotocolDispatcher() *protocolDispatcher {
	return &protocolDispatcher{
		Sorter:       &sorter.DefaultSorter{},
		Grouper:      &grouper.DefaultGrouper{},
		Materializer: &materializer.DefaultMaterializer{},
		functions:    make(map[string]types.FunctionInterface),
		plugins:      make(map[string]types.PluginGeneratorInterface),
		context:      ordereddict.NewDict(),
		Stats:        &types.Stats{},
	}
}
