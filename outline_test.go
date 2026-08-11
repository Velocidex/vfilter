package vfilter

import (
	"testing"
)

func TestOutline(t *testing.T) {
	query := "LET Y = SELECT Foo FROM pslist(pid=1)\n" +
		"SELECT upcase(str=X), Bar AS baz FROM Artifact.Linux.Sys.Users() WHERE Foo > 3"
	vqls, err := MultiParseWithComments(query)
	if err != nil {
		t.Fatalf("Failed to parse: %v", err)
	}
	if len(vqls) != 2 {
		t.Fatalf("Expected 2 statements, got %d", len(vqls))
	}

	// Statement 1: LET Y = SELECT ...
	let := Outline(vqls[0])
	if let == nil {
		t.Fatal("Expected LET outline")
	}
	if let.Kind != OutlineKindLet || let.Name != "Y" {
		t.Fatalf("Expected LET Y, got %+v", let)
	}
	if len(let.Children) != 1 {
		t.Fatalf("Expected 1 child (subquery), got %d", len(let.Children))
	}
	subquery := let.Children[0]
	if subquery.Kind != OutlineKindQuery || subquery.Name != "pslist" {
		t.Fatalf("Expected subquery pslist, got %+v", subquery)
	}
	if len(subquery.Children) != 1 {
		t.Fatalf("Expected 1 column in subquery, got %d", len(subquery.Children))
	}
	if subquery.Children[0].Kind != OutlineKindColumn {
		t.Fatalf("Expected column child, got %+v", subquery.Children[0])
	}

	// Statement 2: SELECT ... FROM Artifact.Linux.Sys.Users()
	query_ := Outline(vqls[1])
	if query_ == nil {
		t.Fatal("Expected query outline")
	}
	if query_.Kind != OutlineKindQuery || query_.Name != "Artifact.Linux.Sys.Users" {
		t.Fatalf("Expected query Artifact.Linux.Sys.Users, got %+v", query_)
	}
	// 2 columns: upcase(str=X), Bar AS baz
	if len(query_.Children) != 2 {
		t.Fatalf("Expected 2 columns, got %d", len(query_.Children))
	}
	col0 := query_.Children[0]
	if col0.Kind != OutlineKindColumn || col0.Name != "" {
		t.Fatalf("Expected unnamed column, got %+v", col0)
	}
	// Column 0 is upcase(str=X) - should have a function child.
	if len(col0.Children) != 1 || col0.Children[0].Kind != OutlineKindFunction ||
		col0.Children[0].Name != "upcase" {
		t.Fatalf("Expected upcase function child, got %+v", col0.Children)
	}
	col1 := query_.Children[1]
	if col1.Kind != OutlineKindColumn || col1.Name != "baz" {
		t.Fatalf("Expected column aliased baz, got %+v", col1)
	}
	// WHERE Foo > 3 has no function calls.
	if len(col1.Children) != 0 {
		t.Fatalf("Expected no function children in Bar, got %+v", col1.Children)
	}
}

func TestOutlineNil(t *testing.T) {
	if Outline(nil) != nil {
		t.Fatal("Expected nil for nil vql")
	}
}
