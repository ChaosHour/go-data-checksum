package checksum

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"time"

	"github.com/ChaosHour/go-data-checksum/pkg/builder"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

// GetTimeColumn gets the specified time column for range dataCheck
func (ctx *ChecksumContext) GetTimeColumn() (err error) {
	if ctx.Context.SpecifiedDatetimeColumn == "" {
		return fmt.Errorf("no time column specified for table %s.%s",
			ctx.PerTableContext.SourceDatabaseName,
			ctx.PerTableContext.SourceTableName)
	}

	// Verify the column actually exists on the source table so a typo fails
	// loudly instead of producing an empty (and therefore "equal") check.
	var columnName string
	query := `
      select COLUMN_NAME
        from information_schema.columns
       where table_schema = ? and table_name = ? and column_name = ?
    `
	if err := ctx.Context.SourceDB.QueryRow(query,
		ctx.PerTableContext.SourceDatabaseName,
		ctx.PerTableContext.SourceTableName,
		ctx.Context.SpecifiedDatetimeColumn).Scan(&columnName); err != nil {
		return fmt.Errorf("critical: time column %s not found on table %s.%s: %v",
			ctx.Context.SpecifiedDatetimeColumn,
			ctx.PerTableContext.SourceDatabaseName,
			ctx.PerTableContext.SourceTableName, err)
	}

	ctx.TimeColumn = types.ParseColumnList(columnName)
	return nil
}

// EstimateTableRowsViaExplain estimates the number of rows that match the time range criteria
func (ctx *ChecksumContext) EstimateTableRowsViaExplain() (int, error) {
	query := builder.BuildTimeRangeEstimateQuery(
		ctx.PerTableContext.SourceDatabaseName,
		ctx.PerTableContext.SourceTableName,
		ctx.Context.SpecifiedDatetimeColumn)

	rows, err := ctx.Context.SourceDB.Query(query,
		ctx.Context.SpecifiedDatetimeRangeBegin,
		ctx.Context.SpecifiedDatetimeRangeEnd)
	if err != nil {
		return 0, fmt.Errorf("critical: table %s.%s estimate rows via EXPLAIN failed: %v",
			ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return 0, err
	}
	rowsColumnIndex := -1
	for i, col := range columns {
		if col == "rows" {
			rowsColumnIndex = i
			break
		}
	}
	if rowsColumnIndex < 0 {
		return 0, fmt.Errorf("critical: EXPLAIN output has no 'rows' column for table %s.%s",
			ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	}

	estimatedRows := 0
	values := types.NewColumnValues(len(columns))
	for rows.Next() {
		if err := rows.Scan(values.ValuesPointers...); err != nil {
			return 0, err
		}
		var rowCount int
		if _, err := fmt.Sscanf(values.StringColumn(rowsColumnIndex), "%d", &rowCount); err == nil {
			estimatedRows += rowCount
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	ctx.Context.Log.Debugf("Debug: estimated %d rows in time range for table %s.%s",
		estimatedRows, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	return estimatedRows, nil
}

// CalculateNextIterationTimeRange calculates the next time range to check.
// Chunks are [min, max) except the final chunk which is [min, end].
func (ctx *ChecksumContext) CalculateNextIterationTimeRange() (bool, error) {
	if ctx.GetIteration() == 0 {
		ctx.TimeIterationRangeMinValue = ctx.Context.SpecifiedDatetimeRangeBegin
	} else {
		// For subsequent iterations, use the end of the previous range as the start
		ctx.TimeIterationRangeMinValue = ctx.TimeIterationRangeMaxValue
	}

	// Reached (or passed) the end of the requested range: nothing further.
	if !ctx.TimeIterationRangeMinValue.Before(ctx.Context.SpecifiedDatetimeRangeEnd) {
		return false, nil
	}

	ctx.TimeIterationRangeMaxValue = ctx.TimeIterationRangeMinValue.Add(ctx.Context.SpecifiedTimeRangePerStep)
	if ctx.TimeIterationRangeMaxValue.After(ctx.Context.SpecifiedDatetimeRangeEnd) {
		ctx.TimeIterationRangeMaxValue = ctx.Context.SpecifiedDatetimeRangeEnd
	}
	return true, nil
}

// isFinalTimeChunk reports whether the current chunk ends exactly at the requested range end,
// in which case the end bound is inclusive.
func (ctx *ChecksumContext) isFinalTimeChunk() bool {
	return ctx.TimeIterationRangeMaxValue.Equal(ctx.Context.SpecifiedDatetimeRangeEnd)
}

// IterationTimeRangeQueryChecksum performs a checksum query for the current time range
// on both source and target and compares the results, mirroring IterationQueryChecksum.
func (ctx *ChecksumContext) IterationTimeRangeQueryChecksum() (isChunkChecksumEqual bool, duration time.Duration, err error) {
	startTime := time.Now()
	defer func() {
		duration = time.Since(startTime)
	}()

	// checkLevel 1 = aggregated CRC32XOR, 2 = per-row CRC32 values
	var checkLevel int64 = 1
	if ctx.Context.IsSuperSetAsEqual {
		checkLevel = 2
	}

	go ctx.queryTimeRangeChecksumFunc(ctx.Context.SourceDB, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, checkLevel, ctx.SourceResultQueue)
	go ctx.queryTimeRangeChecksumFunc(ctx.Context.TargetDB, ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName, checkLevel, ctx.TargetResultQueue)
	sourceResultStruct, targetResultStruct := <-ctx.SourceResultQueue, <-ctx.TargetResultQueue
	if sourceResultStruct.err != nil {
		return false, duration, sourceResultStruct.err
	}
	if targetResultStruct.err != nil {
		return false, duration, targetResultStruct.err
	}
	sourceResult, targetResult := sourceResultStruct.result, targetResultStruct.result
	ctx.lastSourceChecksum = checksumSummary(sourceResult)
	ctx.lastTargetChecksum = checksumSummary(targetResult)

	if reflect.DeepEqual(sourceResult, targetResult) {
		return true, duration, nil
	}
	if checkLevel == 2 {
		return isOrderedSubset(sourceResult, targetResult), duration, nil
	}
	return false, duration, nil
}

// queryTimeRangeChecksumFunc fetches the checksum result for the current time chunk (aggregated CRC32XOR or per-row CRC32)
func (ctx *ChecksumContext) queryTimeRangeChecksumFunc(db *gosql.DB, databaseName, tableName string, checkLevel int64, ch chan *crc32ResultStruct) {
	var ret []string
	query, err := builder.BuildTimeRangeChecksumSQL(
		databaseName,
		tableName,
		ctx.CheckColumns,
		ctx.Context.SpecifiedDatetimeColumn,
		ctx.isFinalTimeChunk(),
		checkLevel,
	)
	if err != nil {
		ch <- newCrc32ResultStruct(ret, err)
		return
	}

	rows, err := db.Query(query, ctx.TimeIterationRangeMinValue, ctx.TimeIterationRangeMaxValue)
	if err != nil {
		ch <- newCrc32ResultStruct(ret, err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		rowValues := types.NewColumnValues(1)
		if err := rows.Scan(rowValues.ValuesPointers...); err != nil {
			ch <- newCrc32ResultStruct(ret, err)
			return
		}
		ret = append(ret, rowValues.StringColumn(0))
	}
	if err = rows.Err(); err != nil {
		ch <- newCrc32ResultStruct(ret, err)
		return
	}
	ch <- newCrc32ResultStruct(ret, nil)
}

// isOrderedSubset reports whether subset is contained in superset, respecting order
func isOrderedSubset(subset []string, superset []string) bool {
	startIndex := 0
	for i := 0; i < len(subset); i++ {
		founded := false
		for j := startIndex; j < len(superset); j++ {
			if subset[i] == superset[j] {
				startIndex = j + 1
				founded = true
				break
			}
		}
		if !founded {
			return false
		}
	}
	return true
}
