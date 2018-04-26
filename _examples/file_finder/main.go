// This is an example application using vfilter.  The application
// allow selecting of files from the filesystem based on a VQL
// query. This example demonstrates how a third party application can
// integrate with VFilter and extend the VQL language to cater for
// application specific functionality.

package main

import (
	"context"
	"os"
	"path/filepath"

	"gitlab.com/velocidex/vfilter"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	queries = kingpin.Arg("query", "The VQL Query to run.").Required().Strings()
)

// This application will pass back these types to describe each file
// examined.
// examined. Note: Go's convention is that only fields starting with
// upper case can be accessed. Therefore if you want to be able to
// refer to any of these fields in a VQL query you need to start them
// with a capital letter. Example:
// Get all files with names ending with "go"
// SELECT info.Path from glob() where info.Path =~ "go$"

// You can dereference a contained struct using the dot operator. Example:

// Get all files with size smaller than 100
// SELECT info.Path from glob() where info.Stat.Size < 100
type FileInfo struct {
	Path string
	Stat os.FileInfo
}

// Calling a getter (A method with no args) on the struct happens
// transparently. Example:
// SELECT info.FileType from glob()
func (self FileInfo) FileType() string {
	stat, err := os.Stat(self.Path)
	if err != nil {
		return ""
	}

	switch mode := stat.Mode(); {
	case mode.IsRegular():
		return "regular file"
	case mode.IsDir():
		return "directory"
	case mode&os.ModeSymlink != 0:
		return "symbolic link"
	case mode&os.ModeNamedPipe != 0:
		return "named pipe"
	default:
		return ""
	}
}


// In this example we assume running a Stat operation is very
// expensive and so we wish to do it sparingly - i.e. only when
// absolutely required. For example, if the query does not require any
// of the information in the Stat (e.g. modified time etc), then there
// is no need to actually perform the Stat operation on each file
// emitted by the glob plugin.

// In order to determine if the Stat field is required by query we
// extend the Associative protocol to inform VFilter about the special
// handling for the FileInfo struct.
type FileInfoSpecialHandler struct{}
func (self FileInfoSpecialHandler) Applicable(a vfilter.Any, b vfilter.Any) bool {
	_, a_ok := a.(FileInfo)
	b_string, b_ok := b.(string)
	return a_ok && b_ok && b_string == "Stat"
}

func (self FileInfoSpecialHandler) Associative(
	scope *vfilter.Scope, a vfilter.Any, b vfilter.Any) (vfilter.Any, bool) {
	// This should never panic because Applicable ensures it is ok.
	file_info := a.(FileInfo)
	field := b.(string)

	if field == "Stat" {
		stat, err := os.Stat(file_info.Path)
		if err == nil {
			return stat, true
		}
	}

	return false, false
}

func (self FileInfoSpecialHandler) GetMembers(
	scope *vfilter.Scope, a vfilter.Any) []string {
	return vfilter.DefaultAssociative{}.GetMembers(scope, a)
}


// ---------------------------------------------------------------------
// Plugins - VQL plugins are data sources analogous to tables in
// SQL. However, VQL allows the user to specify parameters to plugins
// which may control the data produced.

// Under the covers plugins are expected to return a channel into
// which they feed each row.
// ---------------------------------------------------------------------

// This is a plugin which generates FileInfo objects from a glob
// expression. Examples:
// select * from glob(pattern='/*')
// select * from glob() where info.Path =~ '.+go'
type Glob struct{}
func (self Glob) Call(
	ctx context.Context,
	scope *vfilter.Scope,
	args vfilter.Dict) <- chan vfilter.Row {
	output_chan := make(chan vfilter.Row)
	go func() {
		defer close(output_chan)
		var pattern string
		pattern_arg, pres := scope.Associative(args, "pattern")
		// If no pattern parameter is provided, then just
		// assume the glob is '*'.
		if !pres {
			pattern = "*"
		} else {
			pattern = pattern_arg.(string)
		}

		matches, err := filepath.Glob(pattern)
		if err != nil {
			return
		}

		for _, hit := range matches {
			output_chan <- FileInfo{Path: hit}
		}
	}()

	return output_chan
}

func (self Glob) Name() string {
	return "glob"
}

func (self Glob) Info(type_map *vfilter.TypeMap) *vfilter.PluginInfo {
	return &vfilter.PluginInfo{
		Name: "glob",
		Doc: "Glob files by expression",
		RowType: type_map.AddType(FileInfo{}),
	}
}


func MakeScope() *vfilter.Scope {
	return vfilter.NewScope().AppendPlugins(Glob{}).
		AddProtocolImpl(FileInfoSpecialHandler{})
}

func evalQuery(vql *vfilter.VQL) {
	scope := MakeScope()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	output_chan := vql.Eval(ctx, scope)
	for {
		row, ok := <- output_chan
		if !ok {
			return
		}
		vfilter.Debug(row)
	}
}

func main() {
	kingpin.Parse()
	for _, query := range *queries {
		vql, err := vfilter.Parse(query)
		if err != nil {
			kingpin.FatalIfError(err, "Unable to parse VQL Query")
		}
		vfilter.Debug(vql)
		evalQuery(vql)
	}
}
