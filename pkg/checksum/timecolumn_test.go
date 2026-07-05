package checksum

import (
	"testing"
	"time"

	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

func TestIsOrderedSubset(t *testing.T) {
	tests := []struct {
		name     string
		subset   []string
		superset []string
		expected bool
	}{
		{"identical", []string{"a", "b", "c"}, []string{"a", "b", "c"}, true},
		{"proper subset in order", []string{"a", "c"}, []string{"a", "b", "c"}, true},
		{"empty subset", []string{}, []string{"a"}, true},
		{"both empty", []string{}, []string{}, true},
		{"missing element", []string{"a", "x"}, []string{"a", "b", "c"}, false},
		{"superset larger but out of order", []string{"c", "a"}, []string{"a", "b", "c"}, false},
		{"duplicate in subset needs duplicate in superset", []string{"a", "a"}, []string{"a", "b"}, false},
		{"duplicate matched by duplicate", []string{"a", "a"}, []string{"a", "a", "b"}, true},
		{"subset larger than superset", []string{"a", "b"}, []string{"a"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isOrderedSubset(tt.subset, tt.superset); got != tt.expected {
				t.Errorf("isOrderedSubset(%v, %v) = %t, want %t", tt.subset, tt.superset, got, tt.expected)
			}
		})
	}
}

func newTimeRangeContext(begin, end time.Time, step time.Duration) *ChecksumContext {
	baseCtx := types.NewBaseContext()
	baseCtx.SpecifiedDatetimeRangeBegin = begin
	baseCtx.SpecifiedDatetimeRangeEnd = end
	baseCtx.SpecifiedTimeRangePerStep = step
	return NewChecksumContext(baseCtx, types.NewTableContext("db", "tab", "db", "tab"))
}

func TestCalculateNextIterationTimeRange_WalksWholeRange(t *testing.T) {
	begin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := begin.Add(25 * time.Minute)
	ctx := newTimeRangeContext(begin, end, 10*time.Minute)

	// Chunk 1: [00:00, 00:10)
	hasFurther, err := ctx.CalculateNextIterationTimeRange()
	if err != nil || !hasFurther {
		t.Fatalf("chunk 1: hasFurther=%t err=%v, want true/nil", hasFurther, err)
	}
	if !ctx.TimeIterationRangeMinValue.Equal(begin) || !ctx.TimeIterationRangeMaxValue.Equal(begin.Add(10*time.Minute)) {
		t.Errorf("chunk 1 range = [%v, %v], want [%v, %v]", ctx.TimeIterationRangeMinValue, ctx.TimeIterationRangeMaxValue, begin, begin.Add(10*time.Minute))
	}
	if ctx.isFinalTimeChunk() {
		t.Error("chunk 1 should not be the final chunk")
	}
	ctx.AddIteration()

	// Chunk 2: [00:10, 00:20)
	if hasFurther, _ = ctx.CalculateNextIterationTimeRange(); !hasFurther {
		t.Fatal("chunk 2 expected")
	}
	ctx.AddIteration()

	// Chunk 3: [00:20, 00:25] — capped at end, final.
	if hasFurther, _ = ctx.CalculateNextIterationTimeRange(); !hasFurther {
		t.Fatal("chunk 3 expected")
	}
	if !ctx.TimeIterationRangeMaxValue.Equal(end) {
		t.Errorf("chunk 3 max = %v, want capped at %v", ctx.TimeIterationRangeMaxValue, end)
	}
	if !ctx.isFinalTimeChunk() {
		t.Error("chunk 3 should be the final chunk (inclusive end bound)")
	}
	ctx.AddIteration()

	// No further chunks.
	if hasFurther, _ = ctx.CalculateNextIterationTimeRange(); hasFurther {
		t.Error("iteration should stop after the range end is reached")
	}
}

func TestCalculateNextIterationTimeRange_ExactMultiple(t *testing.T) {
	begin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	end := begin.Add(20 * time.Minute)
	ctx := newTimeRangeContext(begin, end, 10*time.Minute)

	chunks := 0
	for {
		hasFurther, err := ctx.CalculateNextIterationTimeRange()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !hasFurther {
			break
		}
		chunks++
		ctx.AddIteration()
		if chunks > 10 {
			t.Fatal("iteration did not terminate")
		}
	}
	if chunks != 2 {
		t.Errorf("chunks = %d, want 2 for a range that is an exact multiple of the step", chunks)
	}
}

func TestMergeChunkReport_CapsSamples(t *testing.T) {
	baseCtx := types.NewBaseContext()
	td := &TableDiffer{Context: &ChecksumContext{Context: baseCtx}}

	report := &DifferenceReport{SampleDifferences: make([]RecordDifference, 0)}
	chunk := &DifferenceReport{
		SourceOnlyRecords: 4,
		SampleDifferences: make([]RecordDifference, 4),
	}

	// maxSamples=5: first merge fits entirely, second merge must be trimmed.
	td.mergeChunkReport(report, chunk, 5)
	if len(report.SampleDifferences) != 4 {
		t.Fatalf("after first merge: samples = %d, want 4", len(report.SampleDifferences))
	}
	td.mergeChunkReport(report, chunk, 5)
	if len(report.SampleDifferences) != 5 {
		t.Errorf("after second merge: samples = %d, want capped at 5", len(report.SampleDifferences))
	}
	if report.SourceOnlyRecords != 8 {
		t.Errorf("SourceOnlyRecords = %d, want 8 (counts must aggregate even when samples are capped)", report.SourceOnlyRecords)
	}

	// Once at the cap, further merges add no samples but still count.
	td.mergeChunkReport(report, chunk, 5)
	if len(report.SampleDifferences) != 5 {
		t.Errorf("after third merge: samples = %d, want 5", len(report.SampleDifferences))
	}
	if report.SourceOnlyRecords != 12 {
		t.Errorf("SourceOnlyRecords = %d, want 12", report.SourceOnlyRecords)
	}
}

func TestFormatPrimaryKeyMap(t *testing.T) {
	got := formatPrimaryKeyMap(map[string]interface{}{"id": []byte("42")})
	if got != "id=42" {
		t.Errorf("formatPrimaryKeyMap = %q, want %q", got, "id=42")
	}
}
