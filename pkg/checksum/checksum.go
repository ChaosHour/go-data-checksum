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

	// Add tracking fields
	JobTracker   *tracking.JobTracker
	ComparisonID int64
	ChunkTracker map[int]*ChunkProgress
}

type ChunkProgress struct {
	ChunkNumber    int
	StartTime      time.Time
	EndTime        time.Time
	SourceChecksum string
	TargetChecksum string
	IsEqual        bool
	ErrorMessage   string
}

// NewChecksumContext(context *types.BaseContext, perTableContext *types.TableContext) *ChecksumContext {
func NewChecksumContext(context *types.BaseContext, perTableContext *types.TableContext) *ChecksumContext {
	return &ChecksumContext{
		Context:           context,
		PerTableContext:   perTableContext,
		SourceResultQueue: make(chan *crc32ResultStruct),
		TargetResultQueue: make(chan *crc32ResultStruct),
		ChunkTracker:      make(map[int]*ChunkProgress),
	}
}

// GetIteration 获取当前核对批次
func (ctx *ChecksumContext) GetIteration() int64 {
	return atomic.LoadInt64(&ctx.PerTableContext.Iteration)
}

// AddIteration 核对批次累加
func (ctx *ChecksumContext) AddIteration() {
	atomic.AddInt64(&ctx.PerTableContext.Iteration, 1)
}

// GetChunkSize 获取chunk大小
func (ctx *ChecksumContext) GetChunkSize() int64 {
	return atomic.LoadInt64(&ctx.Context.ChunkSize)
}

// GetCheckColumns investigates a table and returns the list of columns candidate for calculating checksum. default all columns.
func (ctx *ChecksumContext) GetCheckColumns() (err error) {
	if ctx.Context.RequestedColumnNames != "" {
		ctx.CheckColumns = types.ParseColumnList(ctx.Context.RequestedColumnNames)
		return nil
	}

	// GROUP_CONCAT()函数默认只能返回1024字节，如果表字段过多可能超过这个限制，需要先设置会话级参数 SET SESSION group_concat_max_len = 10240;
	// GO的MySQL驱动不能保证前后两个查询是在同一个session，除非使用transaction。
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
	// 构造获取唯一键最小值的SQL
	query, err := builder.BuildUniqueKeyMinValuesPreparedQuery(ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.UniqueKey)
	if err != nil {
		return err
	}
	rows, err := ctx.Context.SourceDB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close() // Add this to prevent connection leaks

	// UniqueKeyRangeMinValues 为 ColumnValues结构体
	ctx.UniqueKeyRangeMinValues = types.NewColumnValues(ctx.UniqueKey.Len())
	for rows.Next() {
		// SQL查询结构赋值给UniqueKeyRangeMinValues
		if err = rows.Scan(ctx.UniqueKeyRangeMinValues.ValuesPointers...); err != nil {
			return err
		}
	}
	ctx.Context.Log.Debugf("Debug: UniqueKey min values: [%s] of source table: %s.%s", ctx.UniqueKeyRangeMinValues, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	return rows.Err() // Properly check for errors after scanning
}

// ReadUniqueKeyRangeMaxValues returns the maximum values to be iterated on checksum
func (ctx *ChecksumContext) ReadUniqueKeyRangeMaxValues() (err error) {
	// 构造获取唯一键最小值的SQL
	query, err := builder.BuildUniqueKeyMaxValuesPreparedQuery(ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName, ctx.UniqueKey)
	if err != nil {
		return err
	}
	rows, err := ctx.Context.SourceDB.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close() // Add this to prevent connection leaks

	// UniqueKeyRangeMinValues 为 ColumnValues结构体
	ctx.UniqueKeyRangeMaxValues = types.NewColumnValues(ctx.UniqueKey.Len())
	for rows.Next() {
		// SQL查询结构赋值给UniqueKeyRangeMinValues
		if err = rows.Scan(ctx.UniqueKeyRangeMaxValues.ValuesPointers...); err != nil {
			return err
		}
	}
	ctx.Context.Log.Debugf("Debug: UniqueKey max values: [%s] of source table: %s.%s", ctx.UniqueKeyRangeMaxValues, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	return rows.Err() // Properly check for errors after scanning
}

// CalculateNextIterationRangeEndValues 计算下一批次核对的起始值
func (ctx *ChecksumContext) CalculateNextIterationRangeEndValues() (hasFurtherRange bool, err error) {
	ctx.ChecksumIterationRangeMinValues = ctx.ChecksumIterationRangeMaxValues
	if ctx.ChecksumIterationRangeMinValues == nil {
		ctx.ChecksumIterationRangeMinValues = ctx.UniqueKeyRangeMinValues
	}

	// 正常使用BuildUniqueKeyRangeEndPreparedQueryViaOffset返回最大值
	// 最后一批时使用BuildUniqueKeyRangeEndPreparedQueryViaOffset返回值为空，进入第二次循环，通过BuildUniqueKeyRangeEndPreparedQueryViaTemptable查询最大值
	for i := 0; i < 2; i++ {
		// 构造分批范围的分批下限值查询SQL
		buildFunc := builder.BuildUniqueKeyRangeEndPreparedQueryViaOffset
		if i == 1 {
			// BuildUniqueKeyRangeEndPreparedQueryViaTemptable 构造分批范围的分批下限值查询SQL，与BuildUniqueKeyRangeEndPreparedQueryViaOffset稍有差异
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
		// 实际执行查询
		rows, err := ctx.Context.SourceDB.Query(query, explodedArgs...)
		if err != nil {
			return hasFurtherRange, err
		}
		defer rows.Close() // Add this to prevent connection leaks

		// NewColumnValues 将ColumnValues结构体的abstractValues的值复制到ValuesPointers
		iterationRangeMaxValues := types.NewColumnValues(ctx.UniqueKey.Len())
		// rows.Next() 遍历读取结果期，使用rows.Scan()将结果集存到变量。
		// 结果集(rows)未关闭前，底层的连接处于繁忙状态。当遍历读到最后一条记录时，会发生一个内部EOF错误，自动调用rows.Close()。
		// 但是如果提前退出循环，rows不会关闭，连接不会回到连接池中，连接也不会关闭。所以手动关闭非常重要。rows.Close()可以多次调用，是无害操作。
		for rows.Next() {
			// Scan()函数将查询结果赋值给iterationRangeMaxValues.ValuesPointers...
			if err = rows.Scan(iterationRangeMaxValues.ValuesPointers...); err != nil {
				return hasFurtherRange, err
			}
			hasFurtherRange = true
		}
		if err = rows.Err(); err != nil {
			return hasFurtherRange, err
		}
		// 如果还有下一批次结果，将当前查询结果赋值给context的 MigrationIterationRangeMaxValues
		if hasFurtherRange {
			ctx.ChecksumIterationRangeMaxValues = iterationRangeMaxValues
			return hasFurtherRange, nil
		}
	}
	ctx.Context.Log.Debugf("Debug: Iteration complete: no further range to iterate of source table: %s.%s", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	return hasFurtherRange, nil
}

// IterationQueryChecksum issues a chunk-Checksum query on the table.
// 1. 批次核对：单个批次内聚合结果CRC32值按位异或结果，计算方式：COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#',C1,C2,C3,Cn)) as UNSIGNED)), 10, 16)), 0)
// 2. 记录级核对：单个批次内每条记录的CRC32值，判断源端的CRC32是不是目标端的CRC32的子集，计算方式: COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#',id, ftime, c1, c2)) as UNSIGNED), 10, 16)), 0)
func (ctx *ChecksumContext) IterationQueryChecksum() (isChunkChecksumEqual bool, duration time.Duration, err error) {
	startTime := time.Now()
	defer func() {
		duration = time.Since(startTime)
	}()

	// 判断有序集subset是否superset的子集,这里可以沿用类似归档排序方式,主要由于主键是可以保证顺序一样
	subsetCheckFunc := func(subset []string, superset []string) bool {
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

	// 计算CRC32XOR聚合值，还是逐行CRC32值
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

	// atomic.AddInt64(&this.PerTableContext.Iteration, 1)
	if reflect.DeepEqual(sourceResult, targetResult) {
		return true, duration, nil
	} else if checkLevel == 2 {
		if isSuperset := subsetCheckFunc(sourceResult, targetResult); !isSuperset {
			return false, duration, nil
		}
		return true, duration, nil
	}
	return false, duration, nil
}

// QueryChecksumFunc 获取ChunkChecksum结果(聚合CRC32XOR 或者 逐行CRC32)
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

// DataChecksumByCount 比较源表和目标表的总记录数，如果IsSuperSetAsEqual为false则只有记录数相等才认为核平，否则源表记录数少于等于目标表则认为核平，返回是否核平以及是否需要继续核对
func (ctx *ChecksumContext) DataChecksumByCount() (isTableCountEqual bool, isMoreCheckNeeded bool, err error) {
	SourceQueryTableCount := fmt.Sprintf("select /* dataChecksum */ count(*) from %s.%s", types.EscapeName(ctx.PerTableContext.SourceDatabaseName), types.EscapeName(ctx.PerTableContext.SourceTableName))
	TargetQueryTableCount := fmt.Sprintf("select /* dataChecksum */ count(*) from %s.%s", types.EscapeName(ctx.PerTableContext.TargetDatabaseName), types.EscapeName(ctx.PerTableContext.TargetTableName))
	var sourceRowCount int64
	var targetRowCount int64
	if err = ctx.Context.SourceDB.QueryRow(SourceQueryTableCount).Scan(&sourceRowCount); err != nil {
		return false, false, fmt.Errorf("critical: Table %s.%s query sourceRowCount failed", ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName)
	}
	if err = ctx.Context.TargetDB.QueryRow(TargetQueryTableCount).Scan(&targetRowCount); err != nil {
		return false, false, fmt.Errorf("critical: Table %s.%s query TargetRowCount failed", ctx.PerTableContext.TargetDatabaseName, ctx.PerTableContext.TargetTableName)
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

	return isTableCountEqual, isMoreCheckNeeded, nil
}
