package vfilter

import (
	"testing"
)

func TestInspect(t *testing.T) {
	query := "LET Y = 5\n" +
		"SELECT Foo(X=1), Bar FROM Artifact.Linux.Sys.Users() WHERE Foo > 3 AND baz(X=Y)"
	vqls, err := MultiParseWithComments(query)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}

	var inspection *Inspection
	for _, vql := range vqls {
		ins := Inspect(vql)
		if inspection == nil {
			inspection = ins
		} else {
			inspection.Calls = append(inspection.Calls, ins.Calls...)
			inspection.Symbols = append(inspection.Symbols, ins.Symbols...)
			inspection.Lets = append(inspection.Lets, ins.Lets...)
		}
	}
	if inspection == nil {
		t.Fatal("No inspection returned")
	}

	if len(inspection.Lets) != 1 || inspection.Lets[0].Name != "Y" {
		t.Fatalf("Expected LET Y, got %+v", inspection.Lets)
	}

	// Expect 3 calls: plugin Artifact.Linux.Sys.Users, function Foo, function baz.
	if len(inspection.Calls) != 3 {
		t.Fatalf("Expected 3 calls, got %d: %+v", len(inspection.Calls), inspection.Calls)
	}

	var plugin, foo, baz *CallInfo
	for i := range inspection.Calls {
		switch inspection.Calls[i].Name {
		case "Artifact.Linux.Sys.Users":
			plugin = &inspection.Calls[i]
		case "Foo":
			foo = &inspection.Calls[i]
		case "baz":
			baz = &inspection.Calls[i]
		}
	}

	if plugin == nil || !plugin.IsPlugin {
		t.Fatalf("Expected plugin Artifact.Linux.Sys.Users, got %+v", inspection.Calls)
	}
	if len(plugin.Args) != 0 {
		t.Fatalf("Expected no args on plugin, got %+v", plugin.Args)
	}

	if foo == nil || foo.IsPlugin {
		t.Fatalf("Expected function Foo, got %+v", inspection.Calls)
	}
	if len(foo.Args) != 1 || foo.Args[0].Name != "X" {
		t.Fatalf("Expected arg X on Foo, got %+v", foo.Args)
	}

	if baz == nil {
		t.Fatalf("Expected function baz, got %+v", inspection.Calls)
	}

	// Symbols should include Y (the LET var reference) and the column Foo
	// in the select list.
	found_y := false
	found_foo := false
	for _, symbol := range inspection.Symbols {
		if symbol.Name == "Y" {
			found_y = true
		}
		if symbol.Name == "Foo" {
			found_foo = true
		}
	}
	if !found_y || !found_foo {
		t.Fatalf("Expected symbols Y and Foo, got %+v", inspection.Symbols)
	}

	// Check position of the plugin call (line 2, col 27).
	plugin_pos := plugin.Pos
	if plugin_pos.Line != 2 || plugin_pos.Column != 27 {
		t.Fatalf("Expected plugin at 2:27, got %d:%d",
			plugin_pos.Line, plugin_pos.Column)
	}
}

func TestInspectNil(t *testing.T) {
	ins := Inspect(nil)
	if ins == nil || len(ins.Calls) != 0 {
		t.Fatal("Expected empty inspection for nil")
	}
}
