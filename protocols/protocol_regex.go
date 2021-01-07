package protocols

import (
	"reflect"
	"regexp"

	"www.velocidex.com/golang/vfilter/types"
	"www.velocidex.com/golang/vfilter/utils"
)

// Regex Match protocol
type RegexProtocol interface {
	Applicable(pattern types.Any, target types.Any) bool
	Match(scope types.Scope, pattern types.Any, target types.Any) bool
}

type RegexDispatcher struct {
	impl []RegexProtocol
}

func (self RegexDispatcher) Copy() RegexDispatcher {
	return RegexDispatcher{
		append([]RegexProtocol{}, self.impl...)}
}

func (self RegexDispatcher) Match(scope types.Scope, pattern types.Any, target types.Any) bool {
	pattern_str, ok := pattern.(string)
	if ok {
		// Shortcut the match all operator.
		if pattern_str == "." {
			return true
		}

		switch target.(type) {
		case string:
			var x _SubstringRegex
			return x.Match(scope, pattern, target)

		}
	}

	for i, impl := range self.impl {
		if impl.Applicable(pattern, target) {
			scope.GetStats().IncProtocolSearch(i)
			return impl.Match(scope, pattern, target)
		}
	}

	scope.Trace("Protocol Regex not found for %v (%T) and %v (%T)",
		pattern, pattern, target, target)

	return false
}

func (self *RegexDispatcher) AddImpl(elements ...RegexProtocol) {
	for _, impl := range elements {
		self.impl = append(self.impl, impl)
	}
}

type _SubstringRegex struct{}

func (self _SubstringRegex) Applicable(pattern types.Any, target types.Any) bool {
	_, a_ok := utils.ToString(pattern)
	_, b_ok := utils.ToString(target)

	return a_ok && b_ok
}

func (self _SubstringRegex) Match(scope types.Scope, pattern types.Any, target types.Any) bool {
	pattern_string, _ := utils.ToString(pattern)
	target_string, _ := utils.ToString(target)

	var re *regexp.Regexp
	key := "__re" + pattern_string

	re_any, pres := scope.GetContext(key)
	if pres {
		re, _ = re_any.(*regexp.Regexp)

	} else {
		var err error
		re, err = regexp.Compile("(?i)" + pattern_string)
		if err != nil {
			scope.Log("Compile regexp: %v", err)
			return false
		}

		scope.SetContext(key, re)
	}

	return re.MatchString(target_string)
}

type _ArrayRegex struct{}

func (self _ArrayRegex) Applicable(pattern types.Any, target types.Any) bool {
	_, pattern_ok := utils.ToString(pattern)
	return pattern_ok && is_array(target)
}

func (self _ArrayRegex) Match(scope types.Scope, pattern types.Any, target types.Any) bool {
	a_slice := reflect.ValueOf(target)
	for i := 0; i < a_slice.Len(); i++ {
		if scope.Match(pattern, a_slice.Index(i).Interface()) {
			return true
		}
	}

	return false
}
