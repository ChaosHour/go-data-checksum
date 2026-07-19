package checksum

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

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
	FullRowData      map[string]interface{} // Full row data from source for REPLACE INTO
}

// AnalyzeAndReportDifferences performs comprehensive differential analysis.
// It always scans the entire table pair from the beginning, regardless of the
// state the preceding checksum loop stopped in.
func (td *TableDiffer) AnalyzeAndReportDifferences() error {
	ctx := td.Context

	ctx.Context.Log.Infof("Starting differential analysis for table pair: %s.%s => %s.%s",
		ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName,
		ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)

	// Reset any iteration state left over from the checksum loop so the
	// analysis covers the whole table, not just the range after the first
	// mismatched chunk.
	atomic.StoreInt64(&ctx.PerTableContext.Iteration, 0)
	ctx.ChecksumIterationRangeMinValues = nil
	ctx.ChecksumIterationRangeMaxValues = nil

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
	maxSamples := ctx.Context.MaxSampleDifferences

	sourceIsEmpty := len(ctx.UniqueKeyRangeMinValues.AbstractValues()) == 0 ||
		ctx.UniqueKeyRangeMinValues.AbstractValues()[0] == nil

	if !sourceIsEmpty {
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
				td.mergeChunkReport(report, chunkReport, maxSamples)
				ctx.AddIteration()
			}
		}
	}

	// Chunk boundaries are driven from the source table, so target rows with
	// keys outside the source key range (or every target row, when the source
	// table is empty) have not been seen yet. Sweep them as target-only.
	if err := td.collectOutOfRangeTargetRecords(report, sourceIsEmpty, maxSamples); err != nil {
		return err
	}

	// Report final results
	td.reportResults(report)

	// Persist sampled differences when tracking is enabled
	ctx.TrackDifferenceDetails(report.SampleDifferences)

	// Generate sync SQL if requested
	if ctx.Context.GenerateSyncSQL {
		if err := td.generateSyncSQL(report); err != nil {
			ctx.Context.Log.Errorf("Failed to generate sync SQL: %v", err)
			return err
		}
	}

	return nil
}

// mergeChunkReport aggregates a chunk report into the total report, keeping
// the sample list capped at maxSamples.
func (td *TableDiffer) mergeChunkReport(report, chunkReport *DifferenceReport, maxSamples int) {
	report.SourceOnlyRecords += chunkReport.SourceOnlyRecords
	report.TargetOnlyRecords += chunkReport.TargetOnlyRecords
	report.ModifiedRecords += chunkReport.ModifiedRecords
	report.IdenticalRecords += chunkReport.IdenticalRecords

	if len(report.SampleDifferences) < maxSamples {
		report.SampleDifferences = append(report.SampleDifferences, chunkReport.SampleDifferences...)
		if len(report.SampleDifferences) > maxSamples {
			report.SampleDifferences = report.SampleDifferences[:maxSamples]
		}
	}
}

// analyzeChunkDifferences analyzes differences in the current chunk
func (td *TableDiffer) analyzeChunkDifferences() (*DifferenceReport, error) {
	ctx := td.Context

	// Build the range condition shared by the source and target chunk queries
	rangeStartComparison, rangeStartArgs, err := builder.BuildRangePreparedComparison(
		ctx.UniqueKey,
		ctx.ChecksumIterationRangeMinValues.AbstractValues(),
		builder.GreaterThanOrEqualsComparisonSign,
	)
	if err != nil {
		return nil, err
	}
	rangeEndComparison, rangeEndArgs, err := builder.BuildRangePreparedComparison(
		ctx.UniqueKey,
		ctx.ChecksumIterationRangeMaxValues.AbstractValues(),
		builder.LessThanOrEqualsComparisonSign,
	)
	if err != nil {
		return nil, err
	}
	whereClause := fmt.Sprintf("%s AND %s", rangeStartComparison, rangeEndComparison)
	args := append(rangeStartArgs, rangeEndArgs...)

	sourceRecords, err := td.getChunkRecords(
		ctx.Context.SourceDB,
		ctx.PerTableContext.SourceDatabaseName,
		ctx.PerTableContext.SourceTableName,
		whereClause, args,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get source records: %v", err)
	}

	targetRecords, err := td.getChunkRecords(
		ctx.Context.TargetDB,
		ctx.PerTableContext.TargetDatabaseName,
		ctx.PerTableContext.TargetTableName,
		whereClause, args,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get target records: %v", err)
	}

	// Compare the records
	return td.compareRecordSets(sourceRecords, targetRecords), nil
}

// collectOutOfRangeTargetRecords finds target rows whose keys fall outside the
// source table's key range; every such row exists only in the target.
func (td *TableDiffer) collectOutOfRangeTargetRecords(report *DifferenceReport, sourceIsEmpty bool, maxSamples int) error {
	ctx := td.Context

	type sweep struct {
		whereClause string
		args        []interface{}
	}
	var sweeps []sweep

	if sourceIsEmpty {
		sweeps = append(sweeps, sweep{whereClause: "1=1"})
	} else {
		belowComparison, belowArgs, err := builder.BuildRangePreparedComparison(
			ctx.UniqueKey,
			ctx.UniqueKeyRangeMinValues.AbstractValues(),
			builder.LessThanComparisonSign,
		)
		if err != nil {
			return err
		}
		aboveComparison, aboveArgs, err := builder.BuildRangePreparedComparison(
			ctx.UniqueKey,
			ctx.UniqueKeyRangeMaxValues.AbstractValues(),
			builder.GreaterThanComparisonSign,
		)
		if err != nil {
			return err
		}
		sweeps = append(sweeps,
			sweep{whereClause: belowComparison, args: belowArgs},
			sweep{whereClause: aboveComparison, args: aboveArgs},
		)
	}

	for _, s := range sweeps {
		targetRecords, err := td.getChunkRecords(
			ctx.Context.TargetDB,
			ctx.PerTableContext.TargetDatabaseName,
			ctx.PerTableContext.TargetTableName,
			s.whereClause, s.args,
		)
		if err != nil {
			return fmt.Errorf("failed to get out-of-range target records: %v", err)
		}
		for _, targetRecord := range targetRecords {
			report.TargetOnlyRecords++
			if len(report.SampleDifferences) < maxSamples {
				report.SampleDifferences = append(report.SampleDifferences, RecordDifference{
					PrimaryKeyValues: targetRecord.PrimaryKeyValues,
					DifferenceType:   "target_only",
					SourceChecksum:   "",
					TargetChecksum:   targetRecord.Checksum,
				})
			}
		}
	}
	return nil
}

// getChunkRecords retrieves records with checksums matching the given where clause
func (td *TableDiffer) getChunkRecords(db *sql.DB, databaseName, tableName, whereClause string, args []interface{}) (map[string]RecordData, error) {
	ctx := td.Context

	query, err := td.buildRecordQuery(databaseName, tableName, whereClause)
	if err != nil {
		return nil, err
	}

	rows, err := db.Query(query, args...)
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
			pkValues[i] = formatPrimaryKeyValue(scanDest[i])
			pkMap[ctx.UniqueKey.Columns()[i].Name] = scanDest[i]
		}
		pkKey := strings.Join(pkValues, "|")

		records[pkKey] = RecordData{
			PrimaryKeyValues: pkMap,
			Checksum:         formatPrimaryKeyValue(scanDest[pkColumns]),
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
func (td *TableDiffer) buildRecordQuery(databaseName, tableName, whereClause string) (string, error) {
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

	query := fmt.Sprintf(`
		SELECT %s
		FROM %s.%s
		WHERE %s
		ORDER BY %s
	`,
		strings.Join(selectColumns, ", "),
		types.EscapeName(databaseName),
		types.EscapeName(tableName),
		whereClause,
		strings.Join(escapedPKColumns, ", "),
	)

	return query, nil
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
			if len(report.SampleDifferences) < td.Context.Context.MaxSampleDifferences {
				report.SampleDifferences = append(report.SampleDifferences, RecordDifference{
					PrimaryKeyValues: sourceRecord.PrimaryKeyValues,
					DifferenceType:   "source_only",
					SourceChecksum:   sourceRecord.Checksum,
					TargetChecksum:   "",
				})
			}
		} else if sourceRecord.Checksum != targetRecord.Checksum {
			report.ModifiedRecords++
			if len(report.SampleDifferences) < td.Context.Context.MaxSampleDifferences {
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
			if len(report.SampleDifferences) < td.Context.Context.MaxSampleDifferences {
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

	totalDifferences := report.SourceOnlyRecords + report.TargetOnlyRecords + report.ModifiedRecords
	if totalDifferences == 0 {
		ctx.Context.Log.Infof("No record differences found (%d records are identical).", report.IdenticalRecords)
		ctx.Context.Log.Infof("=== END DIFFERENTIAL ANALYSIS ===")
		return
	}

	if report.SourceOnlyRecords > 0 {
		ctx.Context.Log.Errorf("- %d records exist only in SOURCE", report.SourceOnlyRecords)
	}
	if report.TargetOnlyRecords > 0 {
		ctx.Context.Log.Errorf("+ %d records exist only in TARGET", report.TargetOnlyRecords)
	}
	if report.ModifiedRecords > 0 {
		ctx.Context.Log.Errorf("~ %d records have different data", report.ModifiedRecords)
	}
	ctx.Context.Log.Infof("= %d records are identical", report.IdenticalRecords)

	// Show sample differences
	if len(report.SampleDifferences) > 0 {
		ctx.Context.Log.Infof("=== SAMPLE DIFFERENCES ===")
		maxDisplay := ctx.Context.MaxDisplayDifferences
		if maxDisplay > len(report.SampleDifferences) {
			maxDisplay = len(report.SampleDifferences)
		}

		for i := 0; i < maxDisplay; i++ {
			diff := report.SampleDifferences[i]

			pkStr := ""
			for key, value := range diff.PrimaryKeyValues {
				if pkStr != "" {
					pkStr += ", "
				}
				pkStr += fmt.Sprintf("%s=%s", key, formatPrimaryKeyValue(value))
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

		if totalDifferences > int64(maxDisplay) {
			ctx.Context.Log.Infof("... and %d more differences (use --max-display-differences / --max-sample-differences to show more)",
				totalDifferences-int64(maxDisplay))
		}
	}

	ctx.Context.Log.Infof("=== END DIFFERENTIAL ANALYSIS ===")
}

// formatPrimaryKeyValue converts a primary key value to a readable string
func formatPrimaryKeyValue(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case []byte:
		return string(v)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", v)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", v)
	case float32, float64:
		return fmt.Sprintf("%f", v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// generateSyncSQL generates REPLACE INTO statements for synchronizing differences
func (td *TableDiffer) generateSyncSQL(report *DifferenceReport) error {
	ctx := td.Context

	if len(report.SampleDifferences) == 0 {
		ctx.Context.Log.Infof("No differences to sync")
		return nil
	}

	// REPLACE INTO deletes and re-inserts the whole row, so the statements must
	// always cover every column of the table -- even when the checksum only
	// compared a subset via --check-column-names.
	allColumns, err := ctx.GetAllColumns()
	if err != nil {
		return err
	}

	// For each difference, fetch the full row data from source and generate REPLACE INTO
	var output strings.Builder
	output.WriteString(fmt.Sprintf("-- Sync SQL for %s.%s => %s.%s\n",
		ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName,
		ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName))
	output.WriteString(fmt.Sprintf("-- Generated at: %s\n", time.Now().Format("2006-01-02 15:04:05")))
	output.WriteString(fmt.Sprintf("-- Total differences: source_only=%d, target_only=%d, modified=%d\n\n",
		report.SourceOnlyRecords, report.TargetOnlyRecords, report.ModifiedRecords))

	sqlCount := 0
	for _, diff := range report.SampleDifferences {
		// Only generate REPLACE INTO for source_only and modified records
		// For target_only, we would need DELETE statements (skipping for safety)
		if diff.DifferenceType == "target_only" {
			continue
		}

		// Fetch full row data for this record
		rowData, err := td.fetchFullRowData(diff.PrimaryKeyValues, allColumns)
		if err != nil {
			ctx.Context.Log.Warnf("Failed to fetch full row data for PK %s: %v", formatPrimaryKeyMap(diff.PrimaryKeyValues), err)
			continue
		}

		// Generate REPLACE INTO statement
		replaceStmt := td.buildReplaceIntoStatement(rowData, allColumns)
		output.WriteString(replaceStmt)
		output.WriteString("\n")
		sqlCount++
	}

	output.WriteString(fmt.Sprintf("\n-- Total REPLACE INTO statements generated: %d\n", sqlCount))

	// Warn loudly when the sync SQL does not cover every difference found.
	totalSyncable := report.SourceOnlyRecords + report.ModifiedRecords
	if int64(sqlCount) < totalSyncable {
		warning := fmt.Sprintf("sync SQL is INCOMPLETE: %d of %d syncable differences covered; re-run with a higher --max-sample-differences", sqlCount, totalSyncable)
		output.WriteString(fmt.Sprintf("-- WARNING: %s\n", warning))
		ctx.Context.Log.Warnf("Warning: %s", warning)
	}
	if report.TargetOnlyRecords > 0 {
		output.WriteString(fmt.Sprintf("-- NOTE: %d target-only records were NOT included (deleting requires manual review)\n", report.TargetOnlyRecords))
	}

	// Write to file or stdout
	if ctx.Context.SyncSQLFile != "" {
		if err := td.writeSyncSQLToFile(output.String()); err != nil {
			return err
		}
		ctx.Context.Log.Infof("Sync SQL written to file: %s (%d statements)", ctx.Context.SyncSQLFile, sqlCount)
	} else {
		fmt.Println(output.String())
		ctx.Context.Log.Infof("Sync SQL written to stdout (%d statements)", sqlCount)
	}

	return nil
}

// formatPrimaryKeyMap renders a primary key map for log messages
func formatPrimaryKeyMap(pkValues map[string]interface{}) string {
	parts := make([]string, 0, len(pkValues))
	for key, value := range pkValues {
		parts = append(parts, fmt.Sprintf("%s=%s", key, formatPrimaryKeyValue(value)))
	}
	return strings.Join(parts, ", ")
}

// fetchFullRowData retrieves the complete row data for a given primary key
func (td *TableDiffer) fetchFullRowData(pkValues map[string]interface{}, columns *types.ColumnList) (map[string]interface{}, error) {
	ctx := td.Context

	// Build WHERE clause for primary key
	whereClause := make([]string, 0, len(pkValues))
	args := make([]interface{}, 0, len(pkValues))

	for _, pkCol := range ctx.UniqueKey.Columns() {
		whereClause = append(whereClause, fmt.Sprintf("%s = ?", types.EscapeName(pkCol.Name)))
		args = append(args, pkValues[pkCol.Name])
	}

	// Build SELECT query for all columns
	columnNames := columns.Names()
	escapedColumns := make([]string, len(columnNames))
	for i, col := range columnNames {
		escapedColumns[i] = types.EscapeName(col)
	}

	query := fmt.Sprintf("SELECT %s FROM %s.%s WHERE %s",
		strings.Join(escapedColumns, ", "),
		types.EscapeName(ctx.PerTableContext.SourceDatabaseName),
		types.EscapeName(ctx.PerTableContext.SourceTableName),
		strings.Join(whereClause, " AND "))

	// Execute query
	row := ctx.Context.SourceDB.QueryRow(query, args...)

	// Prepare scan destinations
	scanDest := make([]interface{}, len(columnNames))
	scanPtrs := make([]interface{}, len(columnNames))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	if err := row.Scan(scanPtrs...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %v", err)
	}

	// Build result map
	result := make(map[string]interface{})
	for i, colName := range columnNames {
		result[colName] = scanDest[i]
	}

	return result, nil
}

// buildReplaceIntoStatement generates a REPLACE INTO statement for the given row data
func (td *TableDiffer) buildReplaceIntoStatement(rowData map[string]interface{}, columns *types.ColumnList) string {
	ctx := td.Context

	// Get column names in order
	columnNames := columns.Names()

	// Build column list
	escapedColumns := make([]string, len(columnNames))
	for i, col := range columnNames {
		escapedColumns[i] = types.EscapeName(col)
	}

	// Build values list
	values := make([]string, len(columnNames))
	for i, col := range columnNames {
		values[i] = td.formatValueForSQL(rowData[col])
	}

	return fmt.Sprintf("REPLACE INTO %s.%s (%s) VALUES (%s);",
		types.EscapeName(ctx.PerTableContext.TargetDatabaseName),
		types.EscapeName(ctx.PerTableContext.TargetTableName),
		strings.Join(escapedColumns, ", "),
		strings.Join(values, ", "))
}

// sqlStringEscaper escapes special characters for MySQL string literals.
// Backslash must be escaped because MySQL's default sql_mode treats it as an
// escape character; newlines and control characters are escaped so every
// generated statement stays on a single line (the sync file format).
var sqlStringEscaper = strings.NewReplacer(
	`\`, `\\`,
	`'`, `''`,
	"\n", `\n`,
	"\r", `\r`,
	"\x00", `\0`,
	"\x1a", `\Z`,
)

// formatValueForSQL formats a value for use in SQL statements
func (td *TableDiffer) formatValueForSQL(value interface{}) string {
	if value == nil {
		return "NULL"
	}

	switch v := value.(type) {
	case []byte:
		// Escape and quote byte slices as strings
		return fmt.Sprintf("'%s'", sqlStringEscaper.Replace(string(v)))
	case string:
		// Escape and quote strings
		return fmt.Sprintf("'%s'", sqlStringEscaper.Replace(v))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	case time.Time:
		// DATETIME/TIMESTAMP columns scan as time.Time because the DSN sets
		// parseTime=true; render them in MySQL literal format.
		return fmt.Sprintf("'%s'", v.Format("2006-01-02 15:04:05.999999"))
	default:
		// For other types, convert to string and quote
		str := fmt.Sprintf("%v", v)
		return fmt.Sprintf("'%s'", sqlStringEscaper.Replace(str))
	}
}

// writeSyncSQLToFile writes the sync SQL to the specified file
func (td *TableDiffer) writeSyncSQLToFile(content string) error {
	ctx := td.Context

	file, err := os.OpenFile(ctx.Context.SyncSQLFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open sync SQL file: %v", err)
	}
	defer file.Close()

	if _, err := file.WriteString(content); err != nil {
		return fmt.Errorf("failed to write sync SQL: %v", err)
	}

	return nil
}
