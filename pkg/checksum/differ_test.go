package checksum

import (
	"strings"
	"testing"

	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

// TestCompareRecordSets tests the core comparison logic
func TestCompareRecordSets(t *testing.T) {
	// Create a proper context for testing
	baseCtx := types.NewBaseContext()
	td := &TableDiffer{
		Context: &ChecksumContext{
			Context: baseCtx,
		},
	}

	tests := []struct {
		name           string
		sourceRecords  map[string]RecordData
		targetRecords  map[string]RecordData
		expectSourceOnly int64
		expectTargetOnly int64
		expectModified   int64
		expectIdentical  int64
	}{
		{
			name: "identical records",
			sourceRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
				"2": {
					PrimaryKeyValues: map[string]interface{}{"id": 2},
					Checksum:         "def456",
				},
			},
			targetRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
				"2": {
					PrimaryKeyValues: map[string]interface{}{"id": 2},
					Checksum:         "def456",
				},
			},
			expectSourceOnly: 0,
			expectTargetOnly: 0,
			expectModified:   0,
			expectIdentical:  2,
		},
		{
			name: "source only records",
			sourceRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
				"2": {
					PrimaryKeyValues: map[string]interface{}{"id": 2},
					Checksum:         "def456",
				},
			},
			targetRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
			},
			expectSourceOnly: 1,
			expectTargetOnly: 0,
			expectModified:   0,
			expectIdentical:  1,
		},
		{
			name: "target only records",
			sourceRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
			},
			targetRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
				"2": {
					PrimaryKeyValues: map[string]interface{}{"id": 2},
					Checksum:         "def456",
				},
			},
			expectSourceOnly: 0,
			expectTargetOnly: 1,
			expectModified:   0,
			expectIdentical:  1,
		},
		{
			name: "modified records",
			sourceRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
				"2": {
					PrimaryKeyValues: map[string]interface{}{"id": 2},
					Checksum:         "def456",
				},
			},
			targetRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "xyz789",
				},
				"2": {
					PrimaryKeyValues: map[string]interface{}{"id": 2},
					Checksum:         "def456",
				},
			},
			expectSourceOnly: 0,
			expectTargetOnly: 0,
			expectModified:   1,
			expectIdentical:  1,
		},
		{
			name: "mixed differences",
			sourceRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
				"2": {
					PrimaryKeyValues: map[string]interface{}{"id": 2},
					Checksum:         "def456",
				},
				"3": {
					PrimaryKeyValues: map[string]interface{}{"id": 3},
					Checksum:         "ghi789",
				},
			},
			targetRecords: map[string]RecordData{
				"1": {
					PrimaryKeyValues: map[string]interface{}{"id": 1},
					Checksum:         "abc123",
				},
				"2": {
					PrimaryKeyValues: map[string]interface{}{"id": 2},
					Checksum:         "modified",
				},
				"4": {
					PrimaryKeyValues: map[string]interface{}{"id": 4},
					Checksum:         "jkl012",
				},
			},
			expectSourceOnly: 1,
			expectTargetOnly: 1,
			expectModified:   1,
			expectIdentical:  1,
		},
		{
			name:             "empty tables",
			sourceRecords:    map[string]RecordData{},
			targetRecords:    map[string]RecordData{},
			expectSourceOnly: 0,
			expectTargetOnly: 0,
			expectModified:   0,
			expectIdentical:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := td.compareRecordSets(tt.sourceRecords, tt.targetRecords)

			if report.SourceOnlyRecords != tt.expectSourceOnly {
				t.Errorf("SourceOnlyRecords = %d, want %d", report.SourceOnlyRecords, tt.expectSourceOnly)
			}
			if report.TargetOnlyRecords != tt.expectTargetOnly {
				t.Errorf("TargetOnlyRecords = %d, want %d", report.TargetOnlyRecords, tt.expectTargetOnly)
			}
			if report.ModifiedRecords != tt.expectModified {
				t.Errorf("ModifiedRecords = %d, want %d", report.ModifiedRecords, tt.expectModified)
			}
			if report.IdenticalRecords != tt.expectIdentical {
				t.Errorf("IdenticalRecords = %d, want %d", report.IdenticalRecords, tt.expectIdentical)
			}
		})
	}
}

// TestCompareRecordSets_SampleLimit tests that sample differences are limited
func TestCompareRecordSets_SampleLimit(t *testing.T) {
	baseCtx := types.NewBaseContext()
	baseCtx.MaxSampleDifferences = 10 // Set a low limit for testing
	
	td := &TableDiffer{
		Context: &ChecksumContext{
			Context: baseCtx,
		},
	}

	// Create 20 source-only records
	sourceRecords := make(map[string]RecordData)
	for i := 1; i <= 20; i++ {
		key := string(rune(i))
		sourceRecords[key] = RecordData{
			PrimaryKeyValues: map[string]interface{}{"id": i},
			Checksum:         "checksum",
		}
	}

	targetRecords := map[string]RecordData{}

	report := td.compareRecordSets(sourceRecords, targetRecords)

	if report.SourceOnlyRecords != 20 {
		t.Errorf("SourceOnlyRecords = %d, want 20", report.SourceOnlyRecords)
	}

	// Should only capture first 10 samples
	if len(report.SampleDifferences) != 10 {
		t.Errorf("SampleDifferences length = %d, want 10", len(report.SampleDifferences))
	}
}

// TestRecordDifference_PrimaryKeyTypes tests handling of different primary key types
func TestRecordDifference_PrimaryKeyTypes(t *testing.T) {
	tests := []struct {
		name     string
		pkValues map[string]interface{}
		wantType string
	}{
		{
			name: "integer primary key",
			pkValues: map[string]interface{}{
				"id": 12345,
			},
			wantType: "int",
		},
		{
			name: "byte slice primary key",
			pkValues: map[string]interface{}{
				"id": []byte("50003"),
			},
			wantType: "[]byte",
		},
		{
			name: "string primary key",
			pkValues: map[string]interface{}{
				"uuid": "550e8400-e29b-41d4-a716-446655440000",
			},
			wantType: "string",
		},
		{
			name: "composite primary key",
			pkValues: map[string]interface{}{
				"tenant_id": 123,
				"user_id":   456,
			},
			wantType: "composite",
		},
		{
			name: "nil value",
			pkValues: map[string]interface{}{
				"optional_id": nil,
			},
			wantType: "nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diff := RecordDifference{
				PrimaryKeyValues: tt.pkValues,
				DifferenceType:   "source_only",
			}

			// Verify the structure is valid
			if diff.PrimaryKeyValues == nil {
				t.Error("PrimaryKeyValues should not be nil")
			}

			// Verify we can iterate over the keys
			for key, value := range diff.PrimaryKeyValues {
				if key == "" {
					t.Error("Primary key name should not be empty")
				}
				// Verify the value exists (even if nil)
				_ = value
			}
		})
	}
}

// TestDifferenceReport_Aggregation tests report aggregation across chunks
func TestDifferenceReport_Aggregation(t *testing.T) {
	report := &DifferenceReport{
		SampleDifferences: make([]RecordDifference, 0),
	}

	// Simulate processing 3 chunks
	chunks := []struct {
		sourceOnly int64
		targetOnly int64
		modified   int64
		identical  int64
	}{
		{5, 3, 2, 100},
		{7, 1, 4, 200},
		{3, 2, 1, 150},
	}

	for _, chunk := range chunks {
		report.SourceOnlyRecords += chunk.sourceOnly
		report.TargetOnlyRecords += chunk.targetOnly
		report.ModifiedRecords += chunk.modified
		report.IdenticalRecords += chunk.identical
	}

	expectedSourceOnly := int64(15)
	expectedTargetOnly := int64(6)
	expectedModified := int64(7)
	expectedIdentical := int64(450)

	if report.SourceOnlyRecords != expectedSourceOnly {
		t.Errorf("Total SourceOnlyRecords = %d, want %d", report.SourceOnlyRecords, expectedSourceOnly)
	}
	if report.TargetOnlyRecords != expectedTargetOnly {
		t.Errorf("Total TargetOnlyRecords = %d, want %d", report.TargetOnlyRecords, expectedTargetOnly)
	}
	if report.ModifiedRecords != expectedModified {
		t.Errorf("Total ModifiedRecords = %d, want %d", report.ModifiedRecords, expectedModified)
	}
	if report.IdenticalRecords != expectedIdentical {
		t.Errorf("Total IdenticalRecords = %d, want %d", report.IdenticalRecords, expectedIdentical)
	}
}

// TestBuildRecordQuery_ColumnEscaping tests SQL column name escaping
func TestBuildRecordQuery_ColumnEscaping(t *testing.T) {
	// Create a minimal context for testing
	minValues := types.ToColumnValues([]interface{}{1})
	maxValues := types.ToColumnValues([]interface{}{100})
	
	ctx := &ChecksumContext{
		UniqueKey: types.NewColumnList([]string{"id"}),
		CheckColumns: types.NewColumnList([]string{"name", "email"}),
		ChecksumIterationRangeMinValues: minValues,
		ChecksumIterationRangeMaxValues: maxValues,
	}

	td := &TableDiffer{Context: ctx}

	query, args, err := td.buildRecordQuery("test_db", "test_table")
	
	if err != nil {
		t.Fatalf("buildRecordQuery failed: %v", err)
	}

	if query == "" {
		t.Error("Query should not be empty")
	}

	if args == nil {
		t.Error("Args should not be nil")
	}

	// Verify query contains escaped names
	if !contains(query, "`id`") {
		t.Error("Query should contain escaped column name `id`")
	}

	if !contains(query, "`test_db`") {
		t.Error("Query should contain escaped database name `test_db`")
	}

	if !contains(query, "`test_table`") {
		t.Error("Query should contain escaped table name `test_table`")
	}

	// Verify query structure
	if !contains(query, "SELECT") {
		t.Error("Query should contain SELECT")
	}

	if !contains(query, "FROM") {
		t.Error("Query should contain FROM")
	}

	if !contains(query, "WHERE") {
		t.Error("Query should contain WHERE clause for range")
	}

	if !contains(query, "ORDER BY") {
		t.Error("Query should contain ORDER BY for primary key")
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && 
		len(s) >= len(substr) && 
		indexOfSubstring(s, substr) >= 0
}

func indexOfSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// TestFormatPrimaryKeyValue tests the type-aware formatting function
func TestFormatPrimaryKeyValue(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"nil value", nil, "NULL"},
		{"byte slice", []byte("12345"), "12345"},
		{"int", 42, "42"},
		{"int8", int8(42), "42"},
		{"int16", int16(42), "42"},
		{"int32", int32(42), "42"},
		{"int64", int64(42), "42"},
		{"uint", uint(42), "42"},
		{"uint8", uint8(42), "42"},
		{"uint16", uint16(42), "42"},
		{"uint32", uint32(42), "42"},
		{"uint64", uint64(42), "42"},
		{"float32", float32(3.14), "3.140000"},
		{"float64", 3.14159, "3.141590"},
		{"string", "test-uuid", "test-uuid"},
		{"bool", true, "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatPrimaryKeyValue(tt.input)
			if result != tt.expected {
				t.Errorf("formatPrimaryKeyValue(%v) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}

// TestFormatValueForSQL tests SQL value formatting
func TestFormatValueForSQL(t *testing.T) {
	baseCtx := types.NewBaseContext()
	td := &TableDiffer{
		Context: &ChecksumContext{
			Context: baseCtx,
		},
	}

	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{"nil value", nil, "NULL"},
		{"int", 42, "42"},
		{"int64", int64(100), "100"},
		{"uint", uint(42), "42"},
		{"float64", 3.14, "3.14"},
		{"string", "test", "'test'"},
		{"string with quote", "test's", "'test''s'"},
		{"byte slice", []byte("data"), "'data'"},
		{"bool true", true, "1"},
		{"bool false", false, "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := td.formatValueForSQL(tt.input)
			if result != tt.expected {
				t.Errorf("formatValueForSQL(%v) = %s, want %s", tt.input, result, tt.expected)
			}
		})
	}
}

// TestBuildReplaceIntoStatement tests REPLACE INTO statement generation
func TestBuildReplaceIntoStatement(t *testing.T) {
	baseCtx := types.NewBaseContext()
	perTableCtx := types.NewTableContext("source_db", "source_table", "target_db", "target_table")
	
	ctx := &ChecksumContext{
		Context:         baseCtx,
		PerTableContext: perTableCtx,
		CheckColumns:    types.NewColumnList([]string{"id", "name", "email"}),
	}
	
	td := &TableDiffer{Context: ctx}

	rowData := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
	}

	result := td.buildReplaceIntoStatement(rowData)

	// Verify the statement structure
	if !strings.Contains(result, "REPLACE INTO") {
		t.Error("Statement should contain REPLACE INTO")
	}
	if !strings.Contains(result, "`target_db`.`target_table`") {
		t.Error("Statement should contain escaped target database and table")
	}
	if !strings.Contains(result, "`id`, `name`, `email`") {
		t.Error("Statement should contain escaped column names")
	}
	if !strings.Contains(result, "VALUES") {
		t.Error("Statement should contain VALUES clause")
	}
	if !strings.Contains(result, "1, 'John Doe', 'john@example.com'") {
		t.Errorf("Statement should contain correct values, got: %s", result)
	}
	if !strings.HasSuffix(result, ";") {
		t.Error("Statement should end with semicolon")
	}
}

// TestBuildReplaceIntoStatement_SpecialCharacters tests SQL escaping
func TestBuildReplaceIntoStatement_SpecialCharacters(t *testing.T) {
	baseCtx := types.NewBaseContext()
	perTableCtx := types.NewTableContext("source_db", "source_table", "target_db", "target_table")
	
	ctx := &ChecksumContext{
		Context:         baseCtx,
		PerTableContext: perTableCtx,
		CheckColumns:    types.NewColumnList([]string{"id", "description"}),
	}
	
	td := &TableDiffer{Context: ctx}

	rowData := map[string]interface{}{
		"id":          1,
		"description": "Test's value with 'quotes'",
	}

	result := td.buildReplaceIntoStatement(rowData)

	// Verify SQL escaping of quotes
	if !strings.Contains(result, "Test''s value with ''quotes''") {
		t.Errorf("Statement should escape single quotes, got: %s", result)
	}
}

// TestBuildReplaceIntoStatement_NullValues tests NULL handling
func TestBuildReplaceIntoStatement_NullValues(t *testing.T) {
	baseCtx := types.NewBaseContext()
	perTableCtx := types.NewTableContext("source_db", "source_table", "target_db", "target_table")
	
	ctx := &ChecksumContext{
		Context:         baseCtx,
		PerTableContext: perTableCtx,
		CheckColumns:    types.NewColumnList([]string{"id", "optional_field"}),
	}
	
	td := &TableDiffer{Context: ctx}

	rowData := map[string]interface{}{
		"id":             1,
		"optional_field": nil,
	}

	result := td.buildReplaceIntoStatement(rowData)

	// Verify NULL is used without quotes
	if !strings.Contains(result, "1, NULL") {
		t.Errorf("Statement should contain NULL without quotes, got: %s", result)
	}
}
