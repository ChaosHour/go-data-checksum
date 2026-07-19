/*
@Author: wangzihuacool
@Date: 2022-08-28
*/

package checksum

import (
	gosql "database/sql"
	"fmt"
	"reflect"
	"sync/atomic"
	"time"

	"github.com/ChaosHour/go-data-checksum/pkg/builder"
	"github.com/ChaosHour/go-data-checksum/pkg/tracking"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

type crc32ResultStruct struct {
	result []string
	err    error
}

func newCrc32ResultStruct(ret []string, err error) *crc32ResultStruct {
	return &crc32ResultStruct{result: ret, err: err}
}

type ChecksumContext struct {
	CheckColumns                    *types.ColumnList
	UniqueKey                       *types.ColumnList
	UniqueIndexName                 string
	TimeColumn                      *types.ColumnList
	UniqueKeyRangeMinValues         *types.ColumnValues
	UniqueKeyRangeMaxValues         *types.ColumnValues
	TimeIterationRangeMinValue      time.Time
	TimeIterationRangeMaxValue      time.Time
	ChecksumIterationRangeMinValues *types.ColumnValues
	ChecksumIterationRangeMaxValues *types.ColumnValues
	Context                         *types.BaseContext
	PerTableContext                 *types.TableContext
	SourceResultQueue               chan *crc32ResultStruct
	TargetResultQueue               chan *crc32ResultStruct

	// Tracking state; JobTracker nil means tracking is disabled.
	JobTracker   *tracking.JobTracker
	ComparisonID int64
	// Row counts from the count precheck; -1 means unknown (recorded as NULL).
	SourceRowCount int64
	TargetRowCount int64

	lastSourceChecksum string
	lastTargetChecksum string
	chunksEqual        int
	chunksDifferent    int
	chunksError        int
}

// NewChecksumContext(context *types.BaseContext, perTableContext *types.TableContext) *ChecksumContext {
func NewChecksumContext(context *types.BaseContext, perTableContext *types.TableContext) *ChecksumContext {
	return &ChecksumContext{
		Context:           context,
		PerTableContext:   perTableContext,
		SourceResultQueue: make(chan *crc32ResultStruct),
		TargetResultQueue: make(chan *crc32ResultStruct),
		SourceRowCount:    -1,
		TargetRowCount:    -1,
	}
}

// GetIteration returns the current check iteration
func (ctx *ChecksumContext) GetIteration() int64 {
	return atomic.LoadInt64(&ctx.PerTableContext.Iteration)
}

// AddIteration increments the check iteration counter
func (ctx *ChecksumContext) AddIteration() {
	atomic.AddInt64(&ctx.PerTableContext.Iteration, 1)
}

// GetChunkSize returns the configured chunk size
func (ctx *ChecksumContext) GetChunkSize() int64 {
	return atomic.LoadInt64(&ctx.Context.ChunkSize)
}

// GetCheckColumns investigates a table and returns the list of columns candidate for calculating checksum. default all columns.
func (ctx *ChecksumContext) GetCheckColumns() (err error) {
	if ctx.Context.RequestedColumnNames != "" {
		ctx.CheckColumns = types.ParseColumnList(ctx.Context.RequestedColumnNames)
		return nil
	}

	// GROUP_CONCAT() returns at most 1024 bytes by default, which wide tables can exceed, so raise group_concat_max_len for the session first.
	// The Go MySQL driver does not guarantee consecutive queries run on the same session unless a transaction is used.
	query := `
    select 
      GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION ASC) AS COLUMN_NAMES
      from information_schema.columns 
     where table_schema= ? and table_name = ?
    order by ORDINAL_POSITION ASC
  `
	err = func() error {
		trx, err := ctx.Context.SourceDB.Begin()
		if err != nil {
			return err
		}
		defer trx.Rollback()
		groupConcatMaxLength := 10240
		sessionQuery := fmt.Sprintf(`SET SESSION group_concat_max_len = %d`, groupConcatMaxLength)
		if _, err := trx.Exec(sessionQuery); err != nil {
			return err
		}

		var columnNames string
		if err := trx.QueryRow(query, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName).Scan(&columnNames); err != nil {
			ctx.Context.Log.Errorf("Critical: table %s.%s get CheckColumns failed.\n", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
			return err
		}
		ctx.Context.Log.Debugf("Debug: table %s.%s CheckColumns are %s\n", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, columnNames)
		ctx.CheckColumns = types.ParseColumnList(columnNames)
		return trx.Commit()
	}()
	return err
}

// GetAllColumns returns the complete ordered column list of the source table.
// Sync SQL must always cover every column: REPLACE INTO deletes the target row
// and re-inserts it, so a partial column list would reset unlisted columns.
func (ctx *ChecksumContext) GetAllColumns() (*types.ColumnList, error) {
	query := `
    select
      GROUP_CONCAT(COLUMN_NAME ORDER BY ORDINAL_POSITION ASC) AS COLUMN_NAMES
      from information_schema.columns
     where table_schema= ? and table_name = ?
  `
	var columnList *types.ColumnList
	err := func() error {
		trx, err := ctx.Context.SourceDB.Begin()
		if err != nil {
			return err
		}
		defer trx.Rollback()
		if _, err := trx.Exec(`SET SESSION group_concat_max_len = 10240`); err != nil {
			return err
		}
		var columnNames string
		if err := trx.QueryRow(query, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName).Scan(&columnNames); err != nil {
			return fmt.Errorf("critical: table %s.%s get all columns failed: %v", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, err)
		}
		columnList = types.ParseColumnList(columnNames)
		return trx.Commit()
	}()
	return columnList, err
}

// GetUniqueKeys investigates a table and returns the list of unique keys
// candidate for chunking
func (ctx *ChecksumContext) GetUniqueKeys() (err error) {
	query := `
    SELECT
      UNIQUES.INDEX_NAME,
      UNIQUES.FIRST_COLUMN_NAME,
      UNIQUES.COLUMN_NAMES,
      UNIQUES.COUNT_COLUMN_IN_INDEX,
      COLUMNS.DATA_TYPE,
      IFNULL(COLUMNS.CHARACTER_SET_NAME, '') as CHARACTER_SET_NAME,
	  has_nullable
    FROM INFORMATION_SCHEMA.COLUMNS INNER JOIN (
      SELECT
        TABLE_SCHEMA,
        TABLE_NAME,
        INDEX_NAME,
        COUNT(*) AS COUNT_COLUMN_IN_INDEX,
        GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC) AS COLUMN_NAMES,
        SUBSTRING_INDEX(GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX ASC), ',', 1) AS FIRST_COLUMN_NAME,
        SUM(NULLABLE='YES') > 0 AS has_nullable
      FROM INFORMATION_SCHEMA.STATISTICS
      WHERE
				NON_UNIQUE=0
				AND TABLE_SCHEMA = ?
      	AND TABLE_NAME = ?
      GROUP BY TABLE_SCHEMA, TABLE_NAME, INDEX_NAME
    ) AS UNIQUES
    ON (
      COLUMNS.COLUMN_NAME = UNIQUES.FIRST_COLUMN_NAME
    )
    WHERE
      COLUMNS.TABLE_SCHEMA = ?
      AND COLUMNS.TABLE_NAME = ?
    ORDER BY
      COLUMNS.TABLE_SCHEMA, COLUMNS.TABLE_NAME,
      CASE UNIQUES.INDEX_NAME
        WHEN 'PRIMARY' THEN 0
        ELSE 1
      END,
      CASE has_nullable
        WHEN 0 THEN 0
        ELSE 1
      END,
      CASE IFNULL(CHARACTER_SET_NAME, '')
          WHEN '' THEN 0
          ELSE 1
      END,
      CASE DATA_TYPE
        WHEN 'tinyint' THEN 0
        WHEN 'smallint' THEN 1
        WHEN 'int' THEN 2
        WHEN 'bigint' THEN 3
        ELSE 100
      END,
      COUNT_COLUMN_IN_INDEX
    limit 1
  `

	var indexName string
	var firstColumnName string
	var columnNames string
	var countColumninIndex int
	var dataType string
	var characterSetName string
	var hasNullable bool

	if err := ctx.Context.SourceDB.QueryRow(query, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName).Scan(&indexName, &firstColumnName, &columnNames, &countColumninIndex, &dataType, &characterSetName, &hasNullable); err != nil {
		return fmt.Errorf("critical: table %s.%s get uniqueKey failed", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	}
	// fmt.Printf("%s, %s, %s, %d, %s, %s, %t\n", indexName, firstColumnName, columnNames, countColumninIndex, dataType, characterSetName, hasNullable)
	if hasNullable {
		return fmt.Errorf("critical: table %s.%s got an uniqueKey with null values", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	}
	ctx.Context.Log.Debugf("Debug: UniqueKeys of source table: %s.%s is %s", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, columnNames)
	ctx.UniqueKey = types.ParseColumnList(columnNames)
	ctx.UniqueIndexName = indexName
	return nil
}

// ReadUniqueKeyRangeMinValues returns the minimum values to be iterated on checksum
func (ctx *ChecksumContext) ReadUniqueKeyRangeMinValues() (err error) {
	query, err := builder.BuildUniqueKeyMinValuesPreparedQuery(ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.UniqueKey)
	if err != nil {
		return err
	}
	rows, err := ctx.Context.SourceDB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close() // Add this to prevent connection leaks

	ctx.UniqueKeyRangeMinValues = types.NewColumnValues(ctx.UniqueKey.Len())
	for rows.Next() {
		if err = rows.Scan(ctx.UniqueKeyRangeMinValues.ValuesPointers...); err != nil {
			return err
		}
	}
	ctx.Context.Log.Debugf("Debug: UniqueKey min values: [%s] of source table: %s.%s", ctx.UniqueKeyRangeMinValues, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	return rows.Err() // Properly check for errors after scanning
}

// ReadUniqueKeyRangeMaxValues returns the maximum values to be iterated on checksum
func (ctx *ChecksumContext) ReadUniqueKeyRangeMaxValues() (err error) {
	query, err := builder.BuildUniqueKeyMaxValuesPreparedQuery(ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.UniqueKey)
	if err != nil {
		return err
	}
	rows, err := ctx.Context.SourceDB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close() // Add this to prevent connection leaks

	ctx.UniqueKeyRangeMaxValues = types.NewColumnValues(ctx.UniqueKey.Len())
	for rows.Next() {
		if err = rows.Scan(ctx.UniqueKeyRangeMaxValues.ValuesPointers...); err != nil {
			return err
		}
	}
	ctx.Context.Log.Debugf("Debug: UniqueKey max values: [%s] of source table: %s.%s", ctx.UniqueKeyRangeMaxValues, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	return rows.Err() // Properly check for errors after scanning
}

// CalculateNextIterationRangeEndValues computes the unique-key range for the next check iteration
func (ctx *ChecksumContext) CalculateNextIterationRangeEndValues() (hasFurtherRange bool, err error) {
	ctx.ChecksumIterationRangeMinValues = ctx.ChecksumIterationRangeMaxValues
	if ctx.ChecksumIterationRangeMinValues == nil {
		ctx.ChecksumIterationRangeMinValues = ctx.UniqueKeyRangeMinValues
	}

	// Normally BuildUniqueKeyRangeEndPreparedQueryViaOffset returns the chunk upper bound.
	// On the final chunk it returns no rows, so the second pass queries the max values via BuildUniqueKeyRangeEndPreparedQueryViaTemptable.
	for i := 0; i < 2; i++ {
		buildFunc := builder.BuildUniqueKeyRangeEndPreparedQueryViaOffset
		if i == 1 {
			buildFunc = builder.BuildUniqueKeyRangeEndPreparedQueryViaTemptable
		}
		query, explodedArgs, err := buildFunc(
			ctx.PerTableContext.SourceDatabaseName,
			ctx.PerTableContext.SourceTableName,
			ctx.UniqueKey,
			ctx.ChecksumIterationRangeMinValues.AbstractValues(),
			ctx.UniqueKeyRangeMaxValues.AbstractValues(),
			atomic.LoadInt64(&ctx.Context.ChunkSize),
			ctx.GetIteration() == 0,
			fmt.Sprintf("iteration:%d", ctx.GetIteration()),
			ctx.UniqueIndexName,
		)
		if err != nil {
			return hasFurtherRange, err
		}
		rows, err := ctx.Context.SourceDB.Query(query, explodedArgs...)
		if err != nil {
			return hasFurtherRange, err
		}
		defer rows.Close() // Add this to prevent connection leaks

		iterationRangeMaxValues := types.NewColumnValues(ctx.UniqueKey.Len())
		// While the result set is open the underlying connection stays busy. Reading past the last row closes it automatically,
		// but leaving the loop early would leak the connection, so the explicit Close matters (Close is safe to call more than once).
		for rows.Next() {
			if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
				return hasFurtherRange, err
			}
			hasFurtherRange = true
		}
		if err = rows.Err(); err != nil {
			return hasFurtherRange, err
		}
		// If there is a further chunk, store its upper bound in the context
		if hasFurtherRange {
			ctx.ChecksumIterationRangeMaxValues = iterationRangeMaxValues
			return hasFurtherRange, nil
		}
	}
	ctx.Context.Log.Debugf("Debug: Iteration complete: no further range to iterate of source table: %s.%s", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	return hasFurtherRange, nil
}

// IterationQueryChecksum issues a chunk-Checksum query on the table.
// 1. Chunk-level check: XOR-aggregated CRC32 of the rows in the chunk: COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#',C1,C2,C3,Cn)) as UNSIGNED)), 10, 16)), 0)
// 2. Row-level check: per-row CRC32 values in the chunk, used to test whether the source rows are a subset of the target rows: COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#',id, ftime, c1, c2)) as UNSIGNED), 10, 16)), 0)
func (ctx *ChecksumContext) IterationQueryChecksum() (isChunkChecksumEqual bool, duration time.Duration, err error) {
	startTime := time.Now()
	defer func() {
		duration = time.Since(startTime)
	}()

	// checkLevel 1 = aggregated CRC32XOR, 2 = per-row CRC32 values
	var checkLevel int64 = 1
	if ctx.Context.IsSuperSetAsEqual {
		checkLevel = 2
	}

	var sourceResult []string
	var targetResult []string

	go ctx.QueryChecksumFunc(ctx.Context.SourceDB, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.UniqueKey, checkLevel, ctx.SourceResultQueue)
	go ctx.QueryChecksumFunc(ctx.Context.TargetDB, ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName, ctx.UniqueKey, checkLevel, ctx.TargetResultQueue)
	sourceResultStruct, targetResultStruct := <-ctx.SourceResultQueue, <-ctx.TargetResultQueue
	if sourceResultStruct.err != nil {
		return false, duration, sourceResultStruct.err
	} else if targetResultStruct.err != nil {
		return false, duration, targetResultStruct.err
	} else {
		sourceResult, targetResult = sourceResultStruct.result, targetResultStruct.result
	}
	ctx.lastSourceChecksum = checksumSummary(sourceResult)
	ctx.lastTargetChecksum = checksumSummary(targetResult)

	// atomic.AddInt64(&this.PerTableContext.Iteration, 1)
	if reflect.DeepEqual(sourceResult, targetResult) {
		return true, duration, nil
	} else if checkLevel == 2 {
		// The ordered-subset check works because the unique-key ordering guarantees both sides sort identically
		return isOrderedSubset(sourceResult, targetResult), duration, nil
	}
	return false, duration, nil
}

// QueryChecksumFunc fetches the chunk checksum result (aggregated CRC32XOR or per-row CRC32)
func (ctx *ChecksumContext) QueryChecksumFunc(db *gosql.DB, databaseName, tableName string, uniqueColumn *types.ColumnList, checkLevel int64, ch chan *crc32ResultStruct) {
	var ret []string
	query, explodedArgs, err := builder.BuildRangeChecksumPreparedQuery(
		databaseName,
		tableName,
		ctx.CheckColumns,
		uniqueColumn,
		ctx.ChecksumIterationRangeMinValues.AbstractValues(),
		ctx.ChecksumIterationRangeMaxValues.AbstractValues(),
		ctx.GetIteration() == 0,
		checkLevel,
	)
	if err != nil {
		ch <- newCrc32ResultStruct(ret, err)
		return
	}

	rows, err := db.Query(query, explodedArgs...)
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

// DataChecksumByCount compares the total row counts of the source and target tables. With IsSuperSetAsEqual=false only equal counts pass; otherwise source <= target also passes. Returns whether the counts match and whether further checking is needed.
func (ctx *ChecksumContext) DataChecksumByCount() (isTableCountEqual bool, isMoreCheckNeeded bool, sourceRowCount int64, targetRowCount int64, err error) {
	SourceQueryTableCount := fmt.Sprintf("select /* dataChecksum */ count(*) from %s.%s", types.EscapeName(ctx.PerTableContext.SourceDatabaseName), types.EscapeName(ctx.PerTableContext.SourceTableName))
	TargetQueryTableCount := fmt.Sprintf("select /* dataChecksum */ count(*) from %s.%s", types.EscapeName(ctx.PerTableContext.TargetDatabaseName), types.EscapeName(ctx.PerTableContext.TargetTableName))
	sourceRowCount, targetRowCount = -1, -1
	if err = ctx.Context.SourceDB.QueryRow(SourceQueryTableCount).Scan(&sourceRowCount); err != nil {
		return false, false, sourceRowCount, targetRowCount, fmt.Errorf("critical: Table %s.%s query sourceRowCount failed", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	}
	if err = ctx.Context.TargetDB.QueryRow(TargetQueryTableCount).Scan(&targetRowCount); err != nil {
		return false, false, sourceRowCount, targetRowCount, fmt.Errorf("critical: Table %s.%s query TargetRowCount failed", ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)
	}

	if sourceRowCount == targetRowCount {
		ctx.Context.Log.Debugf("Debug: Record check result (sourceTable %d rows, targetTable %d rows) is equal of table pair: %s.%s => %s.%s. More check needed.", sourceRowCount, targetRowCount, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)
		isTableCountEqual = true
		isMoreCheckNeeded = true
	} else if ctx.Context.IsSuperSetAsEqual && sourceRowCount < targetRowCount {
		ctx.Context.Log.Debugf("Debugf: Record check result (sourceTable %d rows, targetTable %d rows) is equal of table pair: %s.%s => %s.%s. Need more check due to IsSuperSetAsEqual=%t", sourceRowCount, targetRowCount, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName, ctx.Context.IsSuperSetAsEqual)
		isTableCountEqual = false
		isMoreCheckNeeded = true
	} else if ctx.Context.IsSuperSetAsEqual && sourceRowCount > targetRowCount {
		ctx.Context.Log.Errorf("Critical: Record check result (sourceTable %d rows, targetTable %d rows) is not equal of table pair: %s.%s => %s.%s.", sourceRowCount, targetRowCount, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)
		isTableCountEqual = false
		isMoreCheckNeeded = false
	} else if !ctx.Context.IsSuperSetAsEqual {
		ctx.Context.Log.Errorf("Critical: Record check result (sourceTable %d rows, targetTable %d rows) is not equal of table pair: %s.%s => %s.%s.", sourceRowCount, targetRowCount, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)
		isTableCountEqual = false
		isMoreCheckNeeded = false
	}

	return isTableCountEqual, isMoreCheckNeeded, sourceRowCount, targetRowCount, nil
}
