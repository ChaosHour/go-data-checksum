package checksum

import (
	"strings"
	"time"

	"github.com/ChaosHour/go-data-checksum/pkg/tracking"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

// Tracking hooks: every method is a no-op when JobTracker is nil, and tracker
// errors are logged as warnings — tracking must never fail a checksum run.

// checksumSummary condenses a chunk checksum result into a value that fits the
// VARCHAR(64) checksum columns. The aggregated-CRC32 case (checkLevel 1) is a
// single element; per-row results (checkLevel 2) become a truncated join.
func checksumSummary(results []string) string {
	if len(results) == 1 {
		return results[0]
	}
	joined := strings.Join(results, ",")
	if len(joined) > 64 {
		return joined[:64]
	}
	return joined
}

// columnValuesToStrings renders range values as text; json.Marshal would
// base64-encode the driver's []uint8 values otherwise.
func columnValuesToStrings(cv *types.ColumnValues) []string {
	if cv == nil {
		return nil
	}
	values := make([]string, len(cv.AbstractValues()))
	for i := range values {
		values[i] = cv.StringColumn(i)
	}
	return values
}

// chunkRanges returns the current iteration range in whichever mode is active.
func (ctx *ChecksumContext) chunkRanges() (rangeStart, rangeEnd interface{}) {
	if ctx.ChecksumIterationRangeMinValues != nil || ctx.ChecksumIterationRangeMaxValues != nil {
		return columnValuesToStrings(ctx.ChecksumIterationRangeMinValues), columnValuesToStrings(ctx.ChecksumIterationRangeMaxValues)
	}
	return ctx.TimeIterationRangeMinValue.Format("2006-01-02 15:04:05"), ctx.TimeIterationRangeMaxValue.Format("2006-01-02 15:04:05")
}

// TrackTableStart inserts the table_comparisons row, or reopens the existing
// one when ComparisonID is pre-set (resume path).
func (ctx *ChecksumContext) TrackTableStart() {
	if ctx.JobTracker == nil {
		return
	}
	if ctx.ComparisonID != 0 {
		if err := ctx.JobTracker.ReopenTableComparison(ctx.ComparisonID); err != nil {
			ctx.Context.Log.Warnf("tracking: reopen table comparison %d failed: %v", ctx.ComparisonID, err)
		}
		return
	}
	comparisonID, err := ctx.JobTracker.StartTableComparison(
		ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName,
		ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)
	if err != nil {
		ctx.Context.Log.Warnf("tracking: start table comparison failed: %v", err)
		return
	}
	ctx.ComparisonID = comparisonID
}

// TrackChunk records one chunk outcome after its retry loop concludes.
func (ctx *ChecksumContext) TrackChunk(chunkNumber int, isEqual bool, chunkErr error, d time.Duration) {
	if ctx.JobTracker == nil {
		return
	}
	switch {
	case chunkErr != nil:
		ctx.chunksError++
	case isEqual:
		ctx.chunksEqual++
	default:
		ctx.chunksDifferent++
	}
	rangeStart, rangeEnd := ctx.chunkRanges()
	if err := ctx.JobTracker.RecordChunkComparison(ctx.ComparisonID, chunkNumber, rangeStart, rangeEnd,
		ctx.lastSourceChecksum, ctx.lastTargetChecksum, tracking.ChunkStatus(isEqual, chunkErr), d); err != nil {
		ctx.Context.Log.Warnf("tracking: record chunk %d failed: %v", chunkNumber, err)
	}
}

// differenceDetailType maps differ RecordDifference types onto the
// difference_details enum.
func differenceDetailType(differenceType string) string {
	switch differenceType {
	case "source_only":
		return "missing_in_target"
	case "target_only":
		return "extra_in_target"
	default:
		return "data_mismatch"
	}
}

// TrackDifferenceDetails persists the sampled record differences of a
// differential analysis (capped upstream by MaxSampleDifferences).
func (ctx *ChecksumContext) TrackDifferenceDetails(differences []RecordDifference) {
	if ctx.JobTracker == nil || len(differences) == 0 {
		return
	}
	details := make([]tracking.DifferenceDetail, 0, len(differences))
	for _, d := range differences {
		pkValues := make(map[string]string, len(d.PrimaryKeyValues))
		for col, val := range d.PrimaryKeyValues {
			pkValues[col] = formatPrimaryKeyValue(val)
		}
		details = append(details, tracking.DifferenceDetail{
			Type:             differenceDetailType(d.DifferenceType),
			PrimaryKeyValues: pkValues,
			SourceChecksum:   d.SourceChecksum,
			TargetChecksum:   d.TargetChecksum,
		})
	}
	if err := ctx.JobTracker.RecordDifferenceDetails(ctx.ComparisonID, details); err != nil {
		ctx.Context.Log.Warnf("tracking: record difference details failed: %v", err)
	}
}

// TrackTableDone finalizes the table_comparisons row.
func (ctx *ChecksumContext) TrackTableDone(isEqual bool, err error) {
	if ctx.JobTracker == nil {
		return
	}
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
	}
	chunksProcessed := ctx.chunksEqual + ctx.chunksDifferent + ctx.chunksError
	if trackErr := ctx.JobTracker.UpdateTableComparison(ctx.ComparisonID, tracking.TableStatus(isEqual, err),
		ctx.SourceRowCount, ctx.TargetRowCount, chunksProcessed, ctx.chunksEqual, ctx.chunksDifferent, errMsg); trackErr != nil {
		ctx.Context.Log.Warnf("tracking: finalize table comparison %d failed: %v", ctx.ComparisonID, trackErr)
	}
}
