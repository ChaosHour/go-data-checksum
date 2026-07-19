package checksum

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

func newTrackingTestContext() *ChecksumContext {
	baseContext := types.NewBaseContext()
	tableContext := types.NewTableContext("srcdb", "t1", "tgtdb", "t1")
	return NewChecksumContext(baseContext, tableContext)
}

// With a nil JobTracker every hook must be a no-op that never panics.
func TestTrackingHooksNilTracker(t *testing.T) {
	ctx := newTrackingTestContext()

	ctx.TrackTableStart()
	ctx.TrackChunk(0, true, nil, time.Second)
	ctx.TrackChunk(1, false, nil, time.Second)
	ctx.TrackChunk(2, false, errors.New("boom"), time.Second)
	ctx.TrackTableDone(false, nil)
	ctx.TrackDifferenceDetails([]RecordDifference{{DifferenceType: "modified"}})

	// Counters must not move when tracking is disabled
	if ctx.chunksEqual != 0 || ctx.chunksDifferent != 0 || ctx.chunksError != 0 {
		t.Errorf("chunk counters moved with nil tracker: %d/%d/%d", ctx.chunksEqual, ctx.chunksDifferent, ctx.chunksError)
	}
	if ctx.ComparisonID != 0 {
		t.Errorf("ComparisonID changed with nil tracker: %d", ctx.ComparisonID)
	}
}

func TestNewChecksumContextRowCountsUnknown(t *testing.T) {
	ctx := newTrackingTestContext()
	if ctx.SourceRowCount != -1 || ctx.TargetRowCount != -1 {
		t.Errorf("new context row counts should be -1 (unknown), got %d/%d", ctx.SourceRowCount, ctx.TargetRowCount)
	}
}

func TestChecksumSummary(t *testing.T) {
	if got := checksumSummary([]string{"deadbeef"}); got != "deadbeef" {
		t.Errorf("single-element summary should pass through, got %q", got)
	}
	if got := checksumSummary(nil); got != "" {
		t.Errorf("empty summary should be empty, got %q", got)
	}
	if got := checksumSummary([]string{"aa", "bb"}); got != "aa,bb" {
		t.Errorf("multi-element summary should join, got %q", got)
	}
	long := checksumSummary([]string{strings.Repeat("a", 50), strings.Repeat("b", 50)})
	if len(long) != 64 {
		t.Errorf("long summary should truncate to 64 chars, got %d", len(long))
	}
}

func TestColumnValuesToStrings(t *testing.T) {
	if got := columnValuesToStrings(nil); got != nil {
		t.Errorf("nil ColumnValues should produce nil, got %v", got)
	}
	cv := types.ToColumnValues([]interface{}{[]uint8("abc"), int64(42)})
	got := columnValuesToStrings(cv)
	if len(got) != 2 || got[0] != "abc" || got[1] != "42" {
		t.Errorf("[]uint8 values must render as text, got %#v", got)
	}
}

func TestDifferenceDetailType(t *testing.T) {
	cases := map[string]string{
		"source_only": "missing_in_target",
		"target_only": "extra_in_target",
		"modified":    "data_mismatch",
		"anything":    "data_mismatch",
	}
	for in, want := range cases {
		if got := differenceDetailType(in); got != want {
			t.Errorf("differenceDetailType(%q) = %q, want %q", in, got, want)
		}
	}
}
