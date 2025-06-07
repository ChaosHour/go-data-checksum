package checksum

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/ChaosHour/go-data-checksum/pkg/builder"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

// TableDiffer provides detailed differential analysis between source and target tables
type TableDiffer struct {
	Context *ChecksumContext
}

// DifferenceReport contains the results of differential analysis
type DifferenceReport struct {
	SourceOnlyRecords int64
	TargetOnlyRecords int64
	ModifiedRecords   int64
	IdenticalRecords  int64
	SampleDifferences []RecordDifference
}

// RecordDifference represents a specific record difference
type RecordDifference struct {
	PrimaryKeyValues map[string]interface{}
	DifferenceType   string // "source_only", "target_only", "modified"
	SourceChecksum   string
	TargetChecksum   string
}

// AnalyzeAndReportDifferences performs comprehensive differential analysis
func (td *TableDiffer) AnalyzeAndReportDifferences() error {
	ctx := td.Context

	ctx.Context.Log.Infof("Starting differential analysis for table pair: %s.%s => %s.%s",
		ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName,
		ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)

	// Get min/max values for iteration
	if err := ctx.ReadUniqueKeyRangeMinValues(); err != nil {
		return err
	}
	if err := ctx.ReadUniqueKeyRangeMaxValues(); err != nil {
		return err
	}

	report := &DifferenceReport{
		SampleDifferences: make([]RecordDifference, 0),
	}

	// Process data in chunks for differential analysis
	var hasFurtherRange = true
	for hasFurtherRange {
		var err error
		hasFurtherRange, err = ctx.CalculateNextIterationRangeEndValues()
		if err != nil {
			return err
		}

		if hasFurtherRange {
			chunkReport, err := td.analyzeChunkDifferences()
			if err != nil {
				return err
			}

			// Aggregate results
			report.SourceOnlyRecords += chunkReport.SourceOnlyRecords
			report.TargetOnlyRecords += chunkReport.TargetOnlyRecords
			report.ModifiedRecords += chunkReport.ModifiedRecords
			report.IdenticalRecords += chunkReport.IdenticalRecords

			// Keep sample differences (limit to avoid memory issues)
			if len(report.SampleDifferences) < 100 {
				report.SampleDifferences = append(report.SampleDifferences, chunkReport.SampleDifferences...)
			}

			ctx.AddIteration()
		}
	}

	// Report final results
	td.reportResults(report)
	return nil
}

// analyzeChunkDifferences analyzes differences in the current chunk
func (td *TableDiffer) analyzeChunkDifferences() (*DifferenceReport, error) {
	ctx := td.Context

	// Build queries to get detailed record data from both source and target
	sourceRecords, err := td.getChunkRecords(
		ctx.Context.SourceDB,
		ctx.PerTableContext.SourceDatabaseName,
		ctx.PerTableContext.SourceTableName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get source records: %v", err)
	}

	targetRecords, err := td.getChunkRecords(
		ctx.Context.TargetDB,
		ctx.PerTableContext.TargetDatabaseName,
		ctx.PerTableContext.TargetTableName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get target records: %v", err)
	}

	// Compare the records
	return td.compareRecordSets(sourceRecords, targetRecords), nil
}

// getChunkRecords retrieves records with checksums for the current chunk
func (td *TableDiffer) getChunkRecords(db *sql.DB, databaseName, tableName string) (map[string]RecordData, error) {
	ctx := td.Context

	// Build query to get primary key values and checksums
	query, explodedArgs, err := td.buildRecordQuery(databaseName, tableName)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(query, explodedArgs...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := make(map[string]RecordData)

	// Prepare scan destinations
	pkColumns := ctx.UniqueKey.Len()
	scanDest := make([]interface{}, pkColumns+1) // PK columns + checksum
	scanPtrs := make([]interface{}, pkColumns+1)
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			return nil, err
		}

		// Build primary key string for comparison
		pkValues := make([]string, pkColumns)
		pkMap := make(map[string]interface{})
		for i := 0; i < pkColumns; i++ {
			pkValues[i] = fmt.Sprintf("%v", scanDest[i])
			pkMap[ctx.UniqueKey.Columns()[i].Name] = scanDest[i]
		}
		pkKey := strings.Join(pkValues, "|")

		checksum := fmt.Sprintf("%v", scanDest[pkColumns])

		records[pkKey] = RecordData{
			PrimaryKeyValues: pkMap,
			Checksum:         checksum,
		}
	}

	return records, rows.Err()
}

// RecordData represents a single record's data
type RecordData struct {
	PrimaryKeyValues map[string]interface{}
	Checksum         string
}

// buildRecordQuery builds a query to get primary key values and record checksums
func (td *TableDiffer) buildRecordQuery(databaseName, tableName string) (string, []interface{}, error) {
	ctx := td.Context

	// Build column list for primary key + checksum
	pkColumnNames := ctx.UniqueKey.Names()
	escapedPKColumns := make([]string, len(pkColumnNames))
	for i, col := range pkColumnNames {
		escapedPKColumns[i] = types.EscapeName(col)
	}

	// Build checksum column list
	checkColumnNames := ctx.CheckColumns.Names()
	escapedCheckColumns := make([]string, len(checkColumnNames))
	for i, col := range checkColumnNames {
		escapedCheckColumns[i] = fmt.Sprintf("hex(%s)", types.EscapeName(col))
	}

	selectColumns := append(escapedPKColumns,
		fmt.Sprintf("COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#', %s)) as UNSIGNED), 10, 16)), 0) as record_checksum",
			strings.Join(escapedCheckColumns, ", ")))

	// Build range comparison for the current chunk
	rangeStartComparison, rangeStartArgs, err := builder.BuildRangePreparedComparison(
		ctx.UniqueKey,
		ctx.ChecksumIterationRangeMinValues.AbstractValues(),
		builder.GreaterThanOrEqualsComparisonSign,
	)
	if err != nil {
		return "", nil, err
	}

	rangeEndComparison, rangeEndArgs, err := builder.BuildRangePreparedComparison(
		ctx.UniqueKey,
		ctx.ChecksumIterationRangeMaxValues.AbstractValues(),
		builder.LessThanOrEqualsComparisonSign,
	)
	if err != nil {
		return "", nil, err
	}

	query := fmt.Sprintf(`
		SELECT %s
		FROM %s.%s
		WHERE %s AND %s
		ORDER BY %s
	`,
		strings.Join(selectColumns, ", "),
		types.EscapeName(databaseName),
		types.EscapeName(tableName),
		rangeStartComparison,
		rangeEndComparison,
		strings.Join(escapedPKColumns, ", "),
	)

	// Combine arguments
	allArgs := append(rangeStartArgs, rangeEndArgs...)

	return query, allArgs, nil
}

// compareRecordSets compares two sets of records and returns differences
func (td *TableDiffer) compareRecordSets(sourceRecords, targetRecords map[string]RecordData) *DifferenceReport {
	report := &DifferenceReport{
		SampleDifferences: make([]RecordDifference, 0),
	}

	// Find records only in source
	for pkKey, sourceRecord := range sourceRecords {
		if targetRecord, exists := targetRecords[pkKey]; !exists {
			report.SourceOnlyRecords++
			if len(report.SampleDifferences) < 10 {
				report.SampleDifferences = append(report.SampleDifferences, RecordDifference{
					PrimaryKeyValues: sourceRecord.PrimaryKeyValues,
					DifferenceType:   "source_only",
					SourceChecksum:   sourceRecord.Checksum,
					TargetChecksum:   "",
				})
			}
		} else if sourceRecord.Checksum != targetRecord.Checksum {
			report.ModifiedRecords++
			if len(report.SampleDifferences) < 10 {
				report.SampleDifferences = append(report.SampleDifferences, RecordDifference{
					PrimaryKeyValues: sourceRecord.PrimaryKeyValues,
					DifferenceType:   "modified",
					SourceChecksum:   sourceRecord.Checksum,
					TargetChecksum:   targetRecord.Checksum,
				})
			}
		} else {
			report.IdenticalRecords++
		}
	}

	// Find records only in target
	for pkKey, targetRecord := range targetRecords {
		if _, exists := sourceRecords[pkKey]; !exists {
			report.TargetOnlyRecords++
			if len(report.SampleDifferences) < 10 {
				report.SampleDifferences = append(report.SampleDifferences, RecordDifference{
					PrimaryKeyValues: targetRecord.PrimaryKeyValues,
					DifferenceType:   "target_only",
					SourceChecksum:   "",
					TargetChecksum:   targetRecord.Checksum,
				})
			}
		}
	}

	return report
}

// reportResults outputs the final difference report
func (td *TableDiffer) reportResults(report *DifferenceReport) {
	ctx := td.Context

	ctx.Context.Log.Infof("=== DIFFERENTIAL ANALYSIS RESULTS ===")
	ctx.Context.Log.Infof("Table Pair: %s.%s => %s.%s",
		ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName,
		ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)

	if report.SourceOnlyRecords > 0 {
		ctx.Context.Log.Errorf("- %d records exist only in SOURCE", report.SourceOnlyRecords)
	}
	if report.TargetOnlyRecords > 0 {
		ctx.Context.Log.Errorf("+ %d records exist only in TARGET", report.TargetOnlyRecords)
	}
	if report.ModifiedRecords > 0 {
		ctx.Context.Log.Errorf("~ %d records have different data", report.ModifiedRecords)
	}
	if report.IdenticalRecords > 0 {
		ctx.Context.Log.Infof("= %d records are identical", report.IdenticalRecords)
	}

	// Show sample differences
	if len(report.SampleDifferences) > 0 {
		ctx.Context.Log.Infof("=== SAMPLE DIFFERENCES ===")
		for i, diff := range report.SampleDifferences {
			if i >= 10 { // Limit output
				break
			}

			pkStr := ""
			for key, value := range diff.PrimaryKeyValues {
				if pkStr != "" {
					pkStr += ", "
				}
				pkStr += fmt.Sprintf("%s=%v", key, value)
			}

			switch diff.DifferenceType {
			case "source_only":
				ctx.Context.Log.Errorf("- Record (%s) exists only in source", pkStr)
			case "target_only":
				ctx.Context.Log.Errorf("+ Record (%s) exists only in target", pkStr)
			case "modified":
				ctx.Context.Log.Errorf("~ Record (%s) modified: source_checksum=%s, target_checksum=%s",
					pkStr, diff.SourceChecksum, diff.TargetChecksum)
			}
		}
	}

	ctx.Context.Log.Infof("=== END DIFFERENTIAL ANALYSIS ===")
}
