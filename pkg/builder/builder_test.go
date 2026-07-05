package builder

import (
	"strings"
	"testing"

	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

func TestEscapeName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain name", "users", "`users`"},
		{"name with underscore", "user_accounts", "`user_accounts`"},
		{"already quoted", `"users"`, "`users`"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EscapeName(tt.input); got != tt.expected {
				t.Errorf("EscapeName(%q) = %q, want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestBuildValueComparison(t *testing.T) {
	comparison, err := BuildValueComparison("id", "?", GreaterThanComparisonSign)
	if err != nil {
		t.Fatalf("BuildValueComparison failed: %v", err)
	}
	if comparison != "(`id` > ?)" {
		t.Errorf("BuildValueComparison = %q, want %q", comparison, "(`id` > ?)")
	}

	if _, err := BuildValueComparison("", "?", EqualsComparisonSign); err == nil {
		t.Error("BuildValueComparison with empty column should fail")
	}
	if _, err := BuildValueComparison("id", "", EqualsComparisonSign); err == nil {
		t.Error("BuildValueComparison with empty value should fail")
	}
}

func TestBuildEqualsComparison(t *testing.T) {
	result, err := BuildEqualsComparison([]string{"col1", "col2"}, []string{"?", "?"})
	if err != nil {
		t.Fatalf("BuildEqualsComparison failed: %v", err)
	}
	expected := "((`col1` = ?) and (`col2` = ?))"
	if result != expected {
		t.Errorf("BuildEqualsComparison = %q, want %q", result, expected)
	}

	if _, err := BuildEqualsComparison([]string{}, []string{}); err == nil {
		t.Error("BuildEqualsComparison with no columns should fail")
	}
	if _, err := BuildEqualsComparison([]string{"col1"}, []string{"?", "?"}); err == nil {
		t.Error("BuildEqualsComparison with mismatched lengths should fail")
	}
}

func TestBuildRangeComparison_SingleColumn(t *testing.T) {
	result, explodedArgs, err := BuildRangeComparison(
		[]string{"id"}, []string{"?"}, []interface{}{100}, GreaterThanComparisonSign)
	if err != nil {
		t.Fatalf("BuildRangeComparison failed: %v", err)
	}
	if result != "((`id` > ?))" {
		t.Errorf("BuildRangeComparison = %q, want %q", result, "((`id` > ?))")
	}
	if len(explodedArgs) != 1 || explodedArgs[0] != 100 {
		t.Errorf("explodedArgs = %v, want [100]", explodedArgs)
	}
}

func TestBuildRangeComparison_CompositeKeyWithEquals(t *testing.T) {
	// (a, b) <= (v1, v2) explodes to (a < v1) or (a = v1 and b < v2) or (a = v1 and b = v2)
	result, explodedArgs, err := BuildRangeComparison(
		[]string{"a", "b"}, []string{"?", "?"}, []interface{}{1, 2}, LessThanOrEqualsComparisonSign)
	if err != nil {
		t.Fatalf("BuildRangeComparison failed: %v", err)
	}
	expected := "((`a` < ?) or (((`a` = ?)) AND (`b` < ?)) or ((`a` = ?) and (`b` = ?)))"
	if result != expected {
		t.Errorf("BuildRangeComparison =\n%q\nwant\n%q", result, expected)
	}
	// args: [1] + [1, 2] + [1, 2] = 5 args
	expectedArgs := []interface{}{1, 1, 2, 1, 2}
	if len(explodedArgs) != len(expectedArgs) {
		t.Fatalf("explodedArgs length = %d, want %d (%v)", len(explodedArgs), len(expectedArgs), explodedArgs)
	}
	for i := range expectedArgs {
		if explodedArgs[i] != expectedArgs[i] {
			t.Errorf("explodedArgs[%d] = %v, want %v", i, explodedArgs[i], expectedArgs[i])
		}
	}
}

func TestBuildRangePreparedComparison(t *testing.T) {
	columns := types.NewColumnList([]string{"id"})
	result, args, err := BuildRangePreparedComparison(columns, []interface{}{42}, GreaterThanOrEqualsComparisonSign)
	if err != nil {
		t.Fatalf("BuildRangePreparedComparison failed: %v", err)
	}
	if !strings.Contains(result, "`id` > ?") || !strings.Contains(result, "`id` = ?") {
		t.Errorf("expected >= to explode into > and = comparisons, got %q", result)
	}
	if len(args) != 2 {
		t.Errorf("args length = %d, want 2 (%v)", len(args), args)
	}
}

func TestBuildRangeChecksumPreparedQuery_AggregateLevel(t *testing.T) {
	checkColumns := types.NewColumnList([]string{"id", "name"})
	uniqueKey := types.NewColumnList([]string{"id"})

	query, args, err := BuildRangeChecksumPreparedQuery(
		"db1", "tab1", checkColumns, uniqueKey,
		[]interface{}{1}, []interface{}{100},
		true, 1)
	if err != nil {
		t.Fatalf("BuildRangeChecksumPreparedQuery failed: %v", err)
	}
	for _, want := range []string{"BIT_XOR", "crc32", "hex(`id`), hex(`name`)", "`db1`.`tab1`", "order by `id` asc"} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q:\n%s", want, query)
		}
	}
	if len(args) == 0 {
		t.Error("expected prepared args, got none")
	}
}

func TestBuildRangeChecksumPreparedQuery_RowLevel(t *testing.T) {
	checkColumns := types.NewColumnList([]string{"id"})
	uniqueKey := types.NewColumnList([]string{"id"})

	query, _, err := BuildRangeChecksumPreparedQuery(
		"db1", "tab1", checkColumns, uniqueKey,
		[]interface{}{1}, []interface{}{100},
		false, 2)
	if err != nil {
		t.Fatalf("BuildRangeChecksumPreparedQuery failed: %v", err)
	}
	if strings.Contains(query, "BIT_XOR") {
		t.Errorf("row-level query (checkLevel=2) must not aggregate with BIT_XOR:\n%s", query)
	}
	if !strings.Contains(query, "crc32") {
		t.Errorf("query missing crc32:\n%s", query)
	}
}

func TestBuildRangeChecksumPreparedQuery_InvalidLevel(t *testing.T) {
	checkColumns := types.NewColumnList([]string{"id"})
	uniqueKey := types.NewColumnList([]string{"id"})
	if _, _, err := BuildRangeChecksumPreparedQuery(
		"db1", "tab1", checkColumns, uniqueKey,
		[]interface{}{1}, []interface{}{100}, true, 3); err == nil {
		t.Error("checkLevel=3 should be rejected")
	}
}

func TestBuildTimeRangeChecksumSQL(t *testing.T) {
	checkColumns := types.NewColumnList([]string{"id", "updated_at"})

	// Non-final chunk: [begin, end) — end bound must be exclusive.
	query, err := BuildTimeRangeChecksumSQL("db1", "tab1", checkColumns, "updated_at", false, 1)
	if err != nil {
		t.Fatalf("BuildTimeRangeChecksumSQL failed: %v", err)
	}
	for _, want := range []string{"BIT_XOR", "`updated_at` >= ?", "`updated_at` < ?", "`db1`.`tab1`"} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q:\n%s", want, query)
		}
	}
	if strings.Contains(query, "<= ?") {
		t.Errorf("non-final chunk must use exclusive end bound:\n%s", query)
	}
	if strings.Contains(query, "order by") {
		t.Errorf("aggregate query (checkLevel=1) needs no ordering:\n%s", query)
	}

	// Final chunk: [begin, end] — end bound inclusive.
	query, err = BuildTimeRangeChecksumSQL("db1", "tab1", checkColumns, "updated_at", true, 2)
	if err != nil {
		t.Fatalf("BuildTimeRangeChecksumSQL failed: %v", err)
	}
	if !strings.Contains(query, "`updated_at` <= ?") {
		t.Errorf("final chunk must use inclusive end bound:\n%s", query)
	}
	if !strings.Contains(query, "order by `updated_at` asc") {
		t.Errorf("row-level query (checkLevel=2) must be ordered:\n%s", query)
	}

	if _, err := BuildTimeRangeChecksumSQL("db1", "tab1", checkColumns, "", false, 1); err == nil {
		t.Error("empty time column should be rejected")
	}
	if _, err := BuildTimeRangeChecksumSQL("db1", "tab1", checkColumns, "updated_at", false, 9); err == nil {
		t.Error("invalid checkLevel should be rejected")
	}
}

func TestBuildTimeRangeEstimateQuery(t *testing.T) {
	query := BuildTimeRangeEstimateQuery("db1", "tab1", "created_at")
	for _, want := range []string{"EXPLAIN", "`db1`.`tab1`", "`created_at` >= ?", "`created_at` <= ?"} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q:\n%s", want, query)
		}
	}
}

func TestBuildUniqueKeyMinMaxValuesPreparedQuery(t *testing.T) {
	columns := types.NewColumnList([]string{"a", "b"})

	minQuery, err := BuildUniqueKeyMinValuesPreparedQuery("db1", "tab1", columns)
	if err != nil {
		t.Fatalf("BuildUniqueKeyMinValuesPreparedQuery failed: %v", err)
	}
	if !strings.Contains(minQuery, "`a` asc, `b` asc") || !strings.Contains(minQuery, "limit 1") {
		t.Errorf("min query should order asc with limit 1:\n%s", minQuery)
	}

	maxQuery, err := BuildUniqueKeyMaxValuesPreparedQuery("db1", "tab1", columns)
	if err != nil {
		t.Fatalf("BuildUniqueKeyMaxValuesPreparedQuery failed: %v", err)
	}
	if !strings.Contains(maxQuery, "`a` desc, `b` desc") {
		t.Errorf("max query should order desc:\n%s", maxQuery)
	}

	empty := types.NewColumnList([]string{})
	if _, err := BuildUniqueKeyMinValuesPreparedQuery("db1", "tab1", empty); err == nil {
		t.Error("empty column list should be rejected")
	}
}

func TestBuildUniqueKeyRangeEndPreparedQueryViaOffset(t *testing.T) {
	columns := types.NewColumnList([]string{"id"})
	query, args, err := BuildUniqueKeyRangeEndPreparedQueryViaOffset(
		"db1", "tab1", columns,
		[]interface{}{1}, []interface{}{5000},
		1000, true, "iteration:0", "PRIMARY")
	if err != nil {
		t.Fatalf("BuildUniqueKeyRangeEndPreparedQueryViaOffset failed: %v", err)
	}
	for _, want := range []string{"force index(`PRIMARY`)", "offset 999", "limit 1", "`id` asc"} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q:\n%s", want, query)
		}
	}
	if len(args) == 0 {
		t.Error("expected prepared args, got none")
	}
}

func TestBuildUniqueKeyRangeEndPreparedQueryViaTemptable(t *testing.T) {
	columns := types.NewColumnList([]string{"id"})
	query, _, err := BuildUniqueKeyRangeEndPreparedQueryViaTemptable(
		"db1", "tab1", columns,
		[]interface{}{1}, []interface{}{5000},
		1000, false, "iteration:5", "PRIMARY")
	if err != nil {
		t.Fatalf("BuildUniqueKeyRangeEndPreparedQueryViaTemptable failed: %v", err)
	}
	for _, want := range []string{"limit 1000", "`id` desc", "select_osc_chunk"} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q:\n%s", want, query)
		}
	}
}
