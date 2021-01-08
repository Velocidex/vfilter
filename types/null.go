package types

// A real type which encodes to JSON NULL. Using go's nil is dangerous
// because it forces constant checking for nil pointer dereference. It
// is safer to just return this value when VQL needs to return NULL.
type Null struct{}

func (self Null) MarshalJSON() ([]byte, error) {
	return []byte("null"), nil
}

func (self Null) String() string {
	return "Null"
}

func IsNullObject(a interface{}) bool {
	if a == nil {
		return true
	}

	switch a.(type) {
	case Null, *Null:
		return true
	default:
		return false
	}
}
