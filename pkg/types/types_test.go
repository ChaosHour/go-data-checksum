package types

import (
	"strings"
	"testing"
	"time"
)

func TestParseColumnList(t *testing.T) {
	cl := ParseColumnList("id,name,email")
	if cl.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", cl.Len())
	}
	if got := cl.Names(); got[0] != "id" || got[1] != "name" || got[2] != "email" {
		t.Errorf("Names() = %v, want [id name email]", got)
	}
	if cl.Ordinals["email"] != 2 {
		t.Errorf("Ordinals[email] = %d, want 2", cl.Ordinals["email"])
	}
	if cl.String() != "id,name,email" {
		t.Errorf("String() = %q, want %q", cl.String(), "id,name,email")
	}
}

func TestColumnListEquals(t *testing.T) {
	a := NewColumnList([]string{"id", "name"})
	b := NewColumnList([]string{"id", "name"})
	c := NewColumnList([]string{"id", "email"})

	if !a.Equals(b) {
		t.Error("identical column lists should be equal")
	}
	if a.Equals(c) {
		t.Error("different column lists should not be equal")
	}
}

func TestColumnListEqualsByNames(t *testing.T) {
	a := NewColumnList([]string{"id", "name"})
	b := NewColumnList([]string{"id", "name"})
	if !a.EqualsByNames(b) {
		t.Error("lists with identical names should be equal by names")
	}
}

func TestColumnListIsSubsetOf(t *testing.T) {
	sub := NewColumnList([]string{"id", "name"})
	super := NewColumnList([]string{"name", "id", "email"})
	if !sub.IsSubsetOf(super) {
		t.Error("[id name] should be a subset of [name id email]")
	}
	if super.IsSubsetOf(sub) {
		t.Error("[name id email] should not be a subset of [id name]")
	}
}

func TestColumnValuesStringColumn(t *testing.T) {
	cv := ToColumnValues([]interface{}{[]uint8("50003"), int64(7), nil})
	if got := cv.StringColumn(0); got != "50003" {
		t.Errorf("StringColumn(0) = %q, want %q (byte slices must render as text)", got, "50003")
	}
	if got := cv.StringColumn(1); got != "7" {
		t.Errorf("StringColumn(1) = %q, want %q", got, "7")
	}
	if got := cv.String(); !strings.HasPrefix(got, "50003,7,") {
		t.Errorf("String() = %q, want prefix %q", got, "50003,7,")
	}
}

func TestNewColumnValuesPointersAreBound(t *testing.T) {
	cv := NewColumnValues(2)
	*(cv.ValuesPointers[0].(*interface{})) = "hello"
	*(cv.ValuesPointers[1].(*interface{})) = 42
	values := cv.AbstractValues()
	if values[0] != "hello" || values[1] != 42 {
		t.Errorf("AbstractValues() = %v, want [hello 42] — scan pointers must write through", values)
	}
}

func TestSetChunkSizeClamping(t *testing.T) {
	tests := []struct {
		input    int64
		expected int64
	}{
		{5, 10},          // below minimum
		{1000, 1000},     // in range
		{999999, 100000}, // above maximum
	}
	for _, tt := range tests {
		ctx := NewBaseContext()
		ctx.SetChunkSize(tt.input)
		if ctx.ChunkSize != tt.expected {
			t.Errorf("SetChunkSize(%d): ChunkSize = %d, want %d", tt.input, ctx.ChunkSize, tt.expected)
		}
	}
}

func TestSetDefaultNumRetries(t *testing.T) {
	ctx := NewBaseContext()
	ctx.SetDefaultNumRetries(3)
	if ctx.DefaultNumRetries != 3 {
		t.Errorf("DefaultNumRetries = %d, want 3", ctx.DefaultNumRetries)
	}
	// Non-positive values keep the existing setting
	ctx.SetDefaultNumRetries(0)
	if ctx.DefaultNumRetries != 3 {
		t.Errorf("DefaultNumRetries = %d, want 3 (zero must be ignored)", ctx.DefaultNumRetries)
	}
}

func TestSetSpecifiedDatetimeRange(t *testing.T) {
	ctx := NewBaseContext()
	if err := ctx.SetSpecifiedDatetimeRange("2026-01-01 00:00:00", "2026-02-01 00:00:00"); err != nil {
		t.Fatalf("valid range rejected: %v", err)
	}

	ctx = NewBaseContext()
	if err := ctx.SetSpecifiedDatetimeRange("2026-02-01 00:00:00", "2026-01-01 00:00:00"); err == nil {
		t.Error("end before begin should be rejected")
	}

	ctx = NewBaseContext()
	if err := ctx.SetSpecifiedDatetimeRange("not-a-date", ""); err == nil {
		t.Error("unparseable begin time should be rejected")
	}
}

func TestIsDatetimeColumnSpecified(t *testing.T) {
	ctx := NewBaseContext()
	if ctx.IsDatetimeColumnSpecified() {
		t.Error("no time column configured: should be false")
	}

	ctx.SpecifiedDatetimeColumn = "updated_at"
	if ctx.IsDatetimeColumnSpecified() {
		t.Error("time column without begin/end range: should be false")
	}

	ctx.SpecifiedDatetimeRangeBegin = time.Date(2026, 1, 1, 0, 0, 0, 0, time.Local)
	ctx.SpecifiedDatetimeRangeEnd = time.Date(2026, 2, 1, 0, 0, 0, 0, time.Local)
	if !ctx.IsDatetimeColumnSpecified() {
		t.Error("time column with full range: should be true")
	}
}

func TestGetDBUriCharset(t *testing.T) {
	ctx := NewBaseContext()
	ctx.SourceDBUser, ctx.SourceDBHost, ctx.SourceDBPort = "u", "h1", 3306
	ctx.TargetDBUser, ctx.TargetDBHost, ctx.TargetDBPort = "u", "h2", 3307
	sourceURI, targetURI := ctx.GetDBUri("information_schema")

	for name, uri := range map[string]string{"source": sourceURI, "target": targetURI} {
		if !strings.Contains(uri, "charset=utf8mb4") {
			t.Errorf("%s DSN must use utf8mb4 (latin1 corrupts non-latin1 data in sync SQL): %s", name, uri)
		}
		if !strings.Contains(uri, "parseTime=true") {
			t.Errorf("%s DSN must keep parseTime=true: %s", name, uri)
		}
	}
	if !strings.Contains(sourceURI, "h1:3306") || !strings.Contains(targetURI, "h2:3307") {
		t.Error("DSNs must target the configured hosts/ports")
	}
}

func TestEscapeName(t *testing.T) {
	if got := EscapeName("my_table"); got != "`my_table`" {
		t.Errorf("EscapeName = %q, want %q", got, "`my_table`")
	}
}

func TestUniqueKey(t *testing.T) {
	uk := &UniqueKey{Name: "PRIMARY", Columns: *NewColumnList([]string{"id"})}
	if !uk.IsPrimary() {
		t.Error("PRIMARY key should report IsPrimary")
	}
	if uk.Len() != 1 {
		t.Errorf("Len() = %d, want 1", uk.Len())
	}
	other := &UniqueKey{Name: "uk_email", Columns: *NewColumnList([]string{"email"})}
	if other.IsPrimary() {
		t.Error("non-PRIMARY key should not report IsPrimary")
	}
}
