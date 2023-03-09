package protocols

import (
	"reflect"
	"regexp"

	"www.velocidex.com/golang/vfilter/types"
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
	target = maybeReduce(target)

	pattern_str, ok := pattern.(string)
	if ok {
		// Shortcut the match all operator - ignore LHS and just
		// return TRUE. This allows a default regex to be provided
		// which just skips all matches transparently.
		switch pattern_str {
		case ".", ".*", "":
			return true
		}

		switch t := target.(type) {
		case string:
			return Match(scope, pattern_str, t)
		}

		if is_array(target) {
			a_slice := reflect.ValueOf(target)
			for i := 0; i < a_slice.Len(); i++ {
				if scope.Match(pattern, a_slice.Index(i).Interface()) {
					return true
				}
			}
			return false
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
		self.impl = append([]RegexProtocol{impl}, self.impl...)
	}
}

func Match(scope types.Scope, pattern string, target string) bool {
	var re *regexp.Regexp
	key := "__re" + pattern

	re_any, pres := scope.GetContext(key)
	if pres {
		re, _ = re_any.(*regexp.Regexp)

	} else {
		var err error
		re, err = regexp.Compile("(?i)" + pattern)
		if err != nil {
			scope.Log("Compile regexp: %v", err)
			return false
		}

		scope.SetContext(key, re)
	}

	return re.MatchString(target)
}
