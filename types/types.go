package types

// These are the public types exposed to package clients.

// A Generic object which may be returned in a row from a plugin.
type Any interface{}

// A Special type which can be used as a plugin parameter. This is
// used to prevent any kind of reducing or wrapping of the arg and
// leaves the caller to handle all aspects. This is mostly used in
// if() where it is critical to not evaluate unused branches in any
// circumstance.
type LazyAny interface{}

// Plugins may return anything as long as there is a valid
// Associative() protocol handler. VFilter will simply call
// scope.Associative(row, column) to retrieve the cell value for each
// column. Note that VFilter will use reflection to implement the
// DefaultAssociative{} protocol - this means that plugins may just
// return any struct with exported methods and fields and it will be
// supported automatically.
type Row interface{}
