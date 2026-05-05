package checksum

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
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

			// Keep sample differences (use configured limit to avoid memory issues)
			maxSamples := ctx.Context.MaxSampleDifferences
			if len(report.SampleDifferences) < maxSamples {
				report.SampleDifferences = append(report.SampleDifferences, chunkReport.SampleDifferences...)
			}

			ctx.AddIteration()
		}
	}

	// Report final results
	td.reportResults(report)
	
	// Generate sync SQL if requested
	if ctx.Context.GenerateSyncSQL {
		if err := td.generateSyncSQL(report); err != nil {
			ctx.Context.Log.Errorf("Failed to generate sync SQL: %v", err)
			return err
		}
	}
	
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
		
		if len(report.SampleDifferences) > maxDisplay {
			ctx.Context.Log.Infof("... and %d more differences (use --max-display-differences to show more)", 
				len(report.SampleDifferences)-maxDisplay)
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
		rowData, err := td.fetchFullRowData(diff.PrimaryKeyValues)
		if err != nil {
			ctx.Context.Log.Warnf("Failed to fetch full row data for PK %v: %v", diff.PrimaryKeyValues, err)
			continue
		}
		
		// Generate REPLACE INTO statement
		replaceStmt := td.buildReplaceIntoStatement(rowData)
		output.WriteString(replaceStmt)
		output.WriteString("\n")
		sqlCount++
	}
	
	output.WriteString(fmt.Sprintf("\n-- Total REPLACE INTO statements generated: %d\n", sqlCount))
	
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

// fetchFullRowData retrieves the complete row data for a given primary key
func (td *TableDiffer) fetchFullRowData(pkValues map[string]interface{}) (map[string]interface{}, error) {
	ctx := td.Context
	
	// Build WHERE clause for primary key
	whereClause := make([]string, 0, len(pkValues))
	args := make([]interface{}, 0, len(pkValues))
	
	for _, pkCol := range ctx.UniqueKey.Columns() {
		whereClause = append(whereClause, fmt.Sprintf("%s = ?", types.EscapeName(pkCol.Name)))
		args = append(args, pkValues[pkCol.Name])
	}
	
	// Build SELECT query for all columns
	columnNames := ctx.CheckColumns.Names()
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
func (td *TableDiffer) buildReplaceIntoStatement(rowData map[string]interface{}) string {
	ctx := td.Context
	
	// Get column names in order
	columnNames := ctx.CheckColumns.Names()
	
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

// formatValueForSQL formats a value for use in SQL statements
func (td *TableDiffer) formatValueForSQL(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	
	switch v := value.(type) {
	case []byte:
		// Escape and quote byte slices as strings
		return fmt.Sprintf("'%s'", strings.ReplaceAll(string(v), "'", "''"))
	case string:
		// Escape and quote strings
		return fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%v", v)
	case float32, float64:
		return fmt.Sprintf("%v", v)
	case bool:
		if v {
			return "1"
		}
		return "0"
	default:
		// For other types, convert to string and quote
		str := fmt.Sprintf("%v", v)
		return fmt.Sprintf("'%s'", strings.ReplaceAll(str, "'", "''"))
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
