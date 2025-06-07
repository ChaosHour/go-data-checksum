package resume

import (
	"database/sql"
	"fmt"

	"github.com/ChaosHour/go-data-checksum/pkg/checksum"
	"github.com/ChaosHour/go-data-checksum/pkg/tracking"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

func ResumeJob(baseContext *types.BaseContext, trackingDB *sql.DB, jobID string) error {
	tracker := &tracking.JobTracker{
		TrackingDB: trackingDB,
		JobID:      jobID,
	}

	pendingTables, err := tracker.GetPendingTables()
	if err != nil {
		return fmt.Errorf("failed to get pending tables: %v", err)
	}

	baseContext.Log.Infof("Resuming job %s with %d pending tables", jobID, len(pendingTables))

	// Continue processing from where we left off
	for _, table := range pendingTables {
		baseContext.Log.Debugf("Processing pending table: %s.%s => %s.%s",
			table.SourceDatabase, table.SourceTable,
			table.TargetDatabase, table.TargetTable)

		// Create table context and checksum context for processing
		tableContext := types.NewTableContext(
			table.SourceDatabase, table.SourceTable,
			table.TargetDatabase, table.TargetTable)

		checksumContext := checksum.NewChecksumContext(baseContext, tableContext)
		checksumContext.JobTracker = tracker
		checksumContext.ComparisonID = table.ComparisonID

		// Perform the comparison with differential reporting
		err := performTableComparisonWithDiff(checksumContext)
		if err != nil {
			baseContext.Log.Errorf("Failed to process table %s.%s: %v",
				table.SourceDatabase, table.SourceTable, err)
		}
	}

	return nil
}

// performTableComparisonWithDiff performs table comparison and reports differences
func performTableComparisonWithDiff(ctx *checksum.ChecksumContext) error {
	// Get unique keys and check columns
	if err := ctx.GetUniqueKeys(); err != nil {
		return err
	}
	if err := ctx.GetCheckColumns(); err != nil {
		return err
	}

	// Perform differential analysis
	differ := &checksum.TableDiffer{Context: ctx}
	return differ.AnalyzeAndReportDifferences()
}
