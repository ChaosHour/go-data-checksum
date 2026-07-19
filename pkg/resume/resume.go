// Package resume loads the pending tables of a previously tracked job so the
// normal checksum flow can re-check them. Resume re-checks each pending table
// from the beginning; chunk-level mid-table resume is deliberately out of scope.
package resume

import (
	"fmt"

	"github.com/ChaosHour/go-data-checksum/pkg/tracking"
)

// LoadPendingTables returns the pending/running tables of the tracker's job as
// a source→target pair map plus a source-full-name→comparison_id map. The
// caller reuses each comparison_id so results land on the existing
// table_comparisons rows instead of inserting duplicates.
func LoadPendingTables(tracker *tracking.JobTracker) (pairs map[string]string, comparisonIDs map[string]int64, err error) {
	pendingTables, err := tracker.GetPendingTables()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get pending tables: %v", err)
	}

	pairs = make(map[string]string, len(pendingTables))
	comparisonIDs = make(map[string]int64, len(pendingTables))
	for _, table := range pendingTables {
		sourceFullName := fmt.Sprintf("%s.%s", table.SourceDatabase, table.SourceTable)
		pairs[sourceFullName] = fmt.Sprintf("%s.%s", table.TargetDatabase, table.TargetTable)
		comparisonIDs[sourceFullName] = table.ComparisonID
	}
	return pairs, comparisonIDs, nil
}
