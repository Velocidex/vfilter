package types

// These are the public types exposed to package clients.

// A Generic object which may be returned in a row from a plugin.
type Any interface{}

// Plugins may return anything as long as there is a valid
// Associative() protocol handler. VFilter will simply call
// scope.Associative(row, column) to retrieve the cell value for each
// column. Note that VFilter will use reflection to implement the
// DefaultAssociative{} protocol - this means that plugins may just
// return any struct with exported methods and fields and it will be
// supported automatically.
type Row interface{}
