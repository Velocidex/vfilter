package vfilter

import (
	"github.com/alecthomas/repr"
	"reflect"
	"sync"
	"unicode"
)

func Debug(arg interface{}) {
	if arg != nil {
		repr.Println(arg)
	} else {
		repr.Println("nil")
	}
}

func merge_channels(cs []<-chan Any) <-chan Any {
	var wg sync.WaitGroup
	out := make(chan Any)

	// Start an output goroutine for each input channel in cs.  output
	// copies values from c to out until c is closed, then calls wg.Done.
	output := func(c <-chan Any) {
		for n := range c {
			out <- n
		}
		wg.Done()
	}
	wg.Add(len(cs))
	for _, c := range cs {
		go output(c)
	}

	// Start a goroutine to close out once all the output goroutines are
	// done.  This must start after the wg.Add call.
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

// Is the symbol exported by Go? Only names with upper case are exported.
func is_exported(name string) bool {
	runes := []rune(name)
	return runes[0] == unicode.ToUpper(runes[0])
}

func _Callable(method_value reflect.Value, field_name string) bool {
	if !method_value.IsValid() {
		return false
	}

	// The name must be exportable.
	if !is_exported(field_name) {
		return false
	}

	// The function must have no args.
	if method_value.Type().NumIn() != 0 {
		return false
	}

	return true
}

func IsNil(a interface{}) bool {
	defer func() { recover() }()
	return a == nil || reflect.ValueOf(a).IsNil()
}

// A real type which encodes to JSON NULL. Using go's nil is dangerous
// because it forced constant checking for nil pointer dereference. It
// is safer to just return this value when VQL needs to return NULL.
type Null struct{}

func (self Null) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

func (self Null) String() string {
	return "Null"
}

func is_null_obj(a Any) bool {
	switch a.(type) {
	case Null, *Null:
		return true
	default:
		return false
	}
}

type _NullAssociative struct{}

func (self _NullAssociative) Applicable(a Any, b Any) bool {
	return is_null_obj(a)
}

func (self _NullAssociative) Associative(scope *Scope, a Any, b Any) (Any, bool) {
	return Null{}, true
}

func (self _NullAssociative) GetMembers(scope *Scope, a Any) []string {
	return []string{}
}

type _NullEqProtocol struct{}

func (self _NullEqProtocol) Applicable(a Any, b Any) bool {
	return is_null_obj(a) || is_null_obj(b)
}

func (self _NullEqProtocol) Eq(scope *Scope, a Any, b Any) bool {
	if is_null_obj(a) && is_null_obj(b) {
		return true
	}
	return false
}
