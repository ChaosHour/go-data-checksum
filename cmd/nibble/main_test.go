package main

import "testing"

func TestParseTables_Valid(t *testing.T) {
	pairs, err := parseTables("sbtest.orders,sbtest.users")
	if err != nil {
		t.Fatalf("parseTables failed: %v", err)
	}
	if len(pairs) != 2 {
		t.Fatalf("pairs = %d, want 2", len(pairs))
	}
	want := tablePair{sourceDB: "sbtest", sourceTable: "orders", targetDB: "sbtest", targetTable: "orders"}
	if pairs[0] != want {
		t.Errorf("pairs[0] = %+v, want %+v", pairs[0], want)
	}
	if pairs[1].sourceTable != "users" {
		t.Errorf("pairs[1].sourceTable = %q, want %q", pairs[1].sourceTable, "users")
	}
}

func TestParseTables_TrimsWhitespace(t *testing.T) {
	pairs, err := parseTables(" sbtest.orders , sbtest.users ")
	if err != nil {
		t.Fatalf("parseTables failed: %v", err)
	}
	if len(pairs) != 2 {
		t.Fatalf("pairs = %d, want 2", len(pairs))
	}
	if pairs[0].sourceDB != "sbtest" || pairs[0].sourceTable != "orders" {
		t.Errorf("pairs[0] = %+v, want sbtest.orders", pairs[0])
	}
}

func TestParseTables_SkipsEmptyEntries(t *testing.T) {
	pairs, err := parseTables("sbtest.orders,,sbtest.users,")
	if err != nil {
		t.Fatalf("parseTables failed: %v", err)
	}
	if len(pairs) != 2 {
		t.Fatalf("pairs = %d, want 2 (empty entries should be skipped)", len(pairs))
	}
}

func TestParseTables_RejectsMissingDot(t *testing.T) {
	if _, err := parseTables("sbtest_orders"); err == nil {
		t.Error("entry without a db.table dot must be rejected")
	}
}

func TestParseTables_RejectsEmptyDBOrTable(t *testing.T) {
	for _, spec := range []string{".orders", "sbtest.", "."} {
		if _, err := parseTables(spec); err == nil {
			t.Errorf("spec %q with empty db or table must be rejected", spec)
		}
	}
}

func TestParseTables_RejectsEmptySpec(t *testing.T) {
	for _, spec := range []string{"", "   ", ",,,"} {
		if _, err := parseTables(spec); err == nil {
			t.Errorf("spec %q must be rejected as having no tables", spec)
		}
	}
}

func TestTablePair_String(t *testing.T) {
	p := tablePair{sourceDB: "sbtest", sourceTable: "orders", targetDB: "sbtest", targetTable: "orders"}
	want := "sbtest.orders => sbtest.orders"
	if got := p.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}
