/*
@Author: wangzihuacool
@Date: 2022-08-28
*/

package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ChaosHour/go-data-checksum/pkg/checksum"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

var AppVersion string

// ChecksumJob 用于协程池管理
type ChecksumJob struct {
	ChecksumJobChan chan int
	wg              *sync.WaitGroup
}

func NewChecksumJob(threads int) *ChecksumJob {
	return &ChecksumJob{
		ChecksumJobChan: make(chan int, threads),
		wg:              &sync.WaitGroup{},
	}
}

// GenerateTableList 生成源表和目标表对应关系
func GenerateTableList(baseContext *types.BaseContext) (err error) {
	queryWithDatabase := func(databases []string, tablesRegexp string, hint string) (tablesList []string, err error) {
		var query string
		if hint == "QueryTableNameWithDatabase" {
			query = fmt.Sprintf(`
              select concat(table_schema, '.', table_name) as table_name
                from information_schema.tables
               where table_schema in ('%s')
               order by 1
            `, strings.Join(databases, "', '"),
			)
		} else if hint == "QueryTableNameWithRegexp" {
			query = fmt.Sprintf(`
              select concat(table_schema, '.', table_name) as table_name
                from information_schema.tables
               where concat(table_schema, '.', table_name) regexp "%s"
               order by 1
            `, tablesRegexp)
		}
		// Make sure we're using the right DB object type
		rows, err := baseContext.SourceDB.Query(query)
		if err != nil {
			return tablesList, err
		}
		defer rows.Close()
		for rows.Next() {
			rowValues := types.NewColumnValues(1)
			if err := rows.Scan(rowValues.ValuesPointers...); err != nil {
				return tablesList, err
			}
			tablesList = append(tablesList, rowValues.StringColumn(0))
		}
		err = rows.Err()
		if err != nil {
			return tablesList, err
		}
		return tablesList, nil
	}

	// 指定源端库表名
	if baseContext.SourceDatabases != "" && baseContext.SourceTables != "" {
		baseContext.SourceDatabaseList = strings.Split(baseContext.SourceDatabases, ",")
		baseContext.SourceTableList = strings.Split(baseContext.SourceTables, ",")
		for _, databaseName := range baseContext.SourceDatabaseList {
			for _, tableName := range baseContext.SourceTableList {
				baseContext.SourceTableFullNameList = append(baseContext.SourceTableFullNameList, fmt.Sprintf("%s.%s", databaseName, tableName))
			}
		}
	} else if baseContext.SourceDatabases != "" && baseContext.SourceTables == "" {
		// 指定源库的所有表
		baseContext.SourceDatabaseList = strings.Split(baseContext.SourceDatabases, ",")
		baseContext.TableQueryHint = "QueryTableNameWithDatabase"
	} else if baseContext.SourceTableNameRegexp != "" {
		// 源表正则匹配
		baseContext.TableQueryHint = "QueryTableNameWithRegexp"
	} else {
		return fmt.Errorf("no source tables specified: use --source-db-name (with optional --source-table-name) or --source-table-regexp")
	}

	if baseContext.SourceTableFullNameList == nil {
		if baseContext.SourceTableFullNameList, err = queryWithDatabase(baseContext.SourceDatabaseList, baseContext.SourceTableNameRegexp, baseContext.TableQueryHint); err != nil {
			return fmt.Errorf("critical: Get source table names failed. Please check arguments")
		}
		if baseContext.SourceTableFullNameList == nil {
			return fmt.Errorf("no source tables matched. Please check arguments")
		}
	}

	baseContext.PairOfSourceAndTargetTables = make(map[string]string, len(baseContext.SourceTableFullNameList))
	if baseContext.TargetDatabases != "" && baseContext.TargetTables != "" {
		// 指定目标库表
		baseContext.TargetDatabaseList = strings.Split(baseContext.TargetDatabases, ",")
		baseContext.TargetTableList = strings.Split(baseContext.TargetTables, ",")
		for _, databaseName := range baseContext.TargetDatabaseList {
			for _, tableName := range baseContext.TargetTableList {
				baseContext.TargetTableFullNameList = append(baseContext.TargetTableFullNameList, fmt.Sprintf("%s.%s", databaseName, tableName))
			}
		}
		if len(baseContext.SourceTableFullNameList) != len(baseContext.TargetTableFullNameList) {
			return fmt.Errorf("source table list (%d tables) and target table list (%d tables) do not match",
				len(baseContext.SourceTableFullNameList), len(baseContext.TargetTableFullNameList))
		}
		for i, tableName := range baseContext.SourceTableFullNameList {
			baseContext.PairOfSourceAndTargetTables[tableName] = baseContext.TargetTableFullNameList[i]
		}
	} else if baseContext.TargetDatabaseAddSuffix != "" && baseContext.TargetTableAddSuffix != "" {
		// 目标库表名加后缀
		for _, tableName := range baseContext.SourceTableFullNameList {
			sourceDatabaseName := strings.Split(tableName, ".")[0]
			sourceTableName := strings.Split(tableName, ".")[1]
			targetTableName := fmt.Sprintf("%s.%s", sourceDatabaseName+baseContext.TargetDatabaseAddSuffix, sourceTableName+baseContext.TargetTableAddSuffix)
			baseContext.PairOfSourceAndTargetTables[tableName] = targetTableName
		}
	} else if baseContext.TargetDatabaseAsSource && baseContext.TargetTableAddSuffix != "" {
		// 目标库名相同，表名加后缀
		for _, tableName := range baseContext.SourceTableFullNameList {
			sourceDatabaseName := strings.Split(tableName, ".")[0]
			sourceTableName := strings.Split(tableName, ".")[1]
			targetTableName := fmt.Sprintf("%s.%s", sourceDatabaseName, sourceTableName+baseContext.TargetTableAddSuffix)
			baseContext.PairOfSourceAndTargetTables[tableName] = targetTableName
		}
	} else if baseContext.TargetDatabaseAddSuffix != "" && baseContext.TargetTableAsSource {
		// 目标库名加后缀，表名相同
		for _, tableName := range baseContext.SourceTableFullNameList {
			sourceDatabaseName := strings.Split(tableName, ".")[0]
			sourceTableName := strings.Split(tableName, ".")[1]
			targetTableName := fmt.Sprintf("%s.%s", sourceDatabaseName+baseContext.TargetDatabaseAddSuffix, sourceTableName)
			baseContext.PairOfSourceAndTargetTables[tableName] = targetTableName
		}
	} else if baseContext.TargetDatabaseAsSource && baseContext.TargetTableAsSource {
		// 目标库表名与源库表一致
		for _, tableName := range baseContext.SourceTableFullNameList {
			targetTableName := tableName
			baseContext.PairOfSourceAndTargetTables[tableName] = targetTableName
		}
	}

	if len(baseContext.PairOfSourceAndTargetTables) == 0 {
		return fmt.Errorf("no source/target table pairs could be built. Please check arguments")
	}

	return nil
}

// runDifferentialAnalysis 运行记录级差异分析，必要时先解析核对字段和唯一键
func runDifferentialAnalysis(baseContext *types.BaseContext, checksumContext *checksum.ChecksumContext) {
	baseContext.Log.Infof("Running differential analysis for table pair: %s.%s => %s.%s", checksumContext.PerTableContext.SourceDatabaseName, checksumContext.PerTableContext.SourceTableName, checksumContext.PerTableContext.TargetDatabaseName, checksumContext.PerTableContext.TargetTableName)
	if checksumContext.CheckColumns == nil {
		if err := checksumContext.GetCheckColumns(); err != nil {
			baseContext.Log.Errorf("Failed to perform differential analysis: %v", err)
			return
		}
	}
	if checksumContext.UniqueKey == nil {
		if err := checksumContext.GetUniqueKeys(); err != nil {
			baseContext.Log.Errorf("Failed to perform differential analysis: %v", err)
			return
		}
	}
	differ := &checksum.TableDiffer{Context: checksumContext}
	if diffErr := differ.AnalyzeAndReportDifferences(); diffErr != nil {
		baseContext.Log.Errorf("Failed to perform differential analysis: %v", diffErr)
	}
}

// ChecksumPerTable 先判断总记录数，然后逐个分片判断checksum值.
// 返回该表是否核对一致；每个表只返回一次结果，由调用方负责写入结果通道。
func (job *ChecksumJob) ChecksumPerTable(baseContext *types.BaseContext, tableContext *types.TableContext) (isEqual bool, err error) {
	startTime := time.Now()
	var tableCheckDuration time.Duration
	defer func() {
		<-job.ChecksumJobChan
		job.wg.Done()
	}()

	ChecksumContext := checksum.NewChecksumContext(baseContext, tableContext)
	baseContext.Log.Infof("Starting check table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

	// 先核对全表count(*)值是否一致
	if !baseContext.IgnoreRowCountCheck {
		baseContext.Log.Debugf("DataChecksumByCount of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
		_, isMoreCheckNeeded, err := ChecksumContext.DataChecksumByCount()
		if err != nil {
			return false, err
		}
		if !isMoreCheckNeeded {
			// 行数不一致：如果开启了差异报告，仍然进行记录级分析
			if baseContext.EnableDifferentialReporting {
				runDifferentialAnalysis(baseContext, ChecksumContext)
			}
			return false, nil
		}
	} else {
		baseContext.Log.Debugf("Ignore DataChecksumByCount of table pair: %s.%s => %s.%s due to IgnoreRowCountCheck=true.", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	}

	// 判断用户有没有输入checkcolumn，没有则取全表所有字段作为核对字段
	baseContext.Log.Debugf("Get user-request check columns of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if ChecksumContext.CheckColumns == nil {
		if err := ChecksumContext.GetCheckColumns(); err != nil {
			return false, err
		}
	}

	// 获取唯一键、最大最小值
	baseContext.Log.Debugf("GetUniqueKeys of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if err := ChecksumContext.GetUniqueKeys(); err != nil {
		return false, err
	}
	baseContext.Log.Debugf("ReadUniqueKeyRangeMinValues of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if err := ChecksumContext.ReadUniqueKeyRangeMinValues(); err != nil {
		return false, err
	}
	baseContext.Log.Debugf("ReadUniqueKeyRangeMaxValues of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if err := ChecksumContext.ReadUniqueKeyRangeMaxValues(); err != nil {
		return false, err
	}

	// 计算checksum值
	var hasFurtherRange = true
	for hasFurtherRange {
		hasFurtherRange, err = ChecksumContext.CalculateNextIterationRangeEndValues()
		if err != nil {
			return false, err
		}
		baseContext.Log.Debugf("CalculateNextIterationRangeEndValues of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

		var isChunkChecksumEqual bool
		var duration time.Duration
		if hasFurtherRange {
			// 增加重试机制
			for i := 0; i < int(ChecksumContext.Context.DefaultNumRetries); i++ {
				if i != 0 {
					time.Sleep(1 * time.Second)
					baseContext.Log.Debugf("IterationQueryChecksum [%s-%s] retry times %d of table pair: %s.%s => %s.%s .", ChecksumContext.ChecksumIterationRangeMinValues.AbstractValues(), ChecksumContext.ChecksumIterationRangeMaxValues.AbstractValues(), i, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
				} else {
					baseContext.Log.Debugf("IterationQueryChecksum [%s-%s] of table pair: %s.%s => %s.%s .", ChecksumContext.ChecksumIterationRangeMinValues.AbstractValues(), ChecksumContext.ChecksumIterationRangeMaxValues.AbstractValues(), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
				}
				isChunkChecksumEqual, duration, err = ChecksumContext.IterationQueryChecksum()
				if err == nil && isChunkChecksumEqual {
					break
				}
			}
			ChecksumContext.AddIteration()
			if err != nil {
				tableCheckDuration = time.Since(startTime)
				baseContext.Log.Errorf("Critical: record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration)
				return false, err
			}
			if !isChunkChecksumEqual {
				tableCheckDuration = time.Since(startTime)
				baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
				baseContext.Log.Errorf("Critical: record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration)

				// If differential reporting is enabled and we found differences, run detailed analysis
				if baseContext.EnableDifferentialReporting {
					runDifferentialAnalysis(baseContext, ChecksumContext)
				}

				return false, nil
			}
			baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
		}
	}
	estimatedRows := int(ChecksumContext.GetIteration() * ChecksumContext.GetChunkSize())
	tableCheckDuration = time.Since(startTime)
	elapsedSecond := int(tableCheckDuration / time.Second)
	if elapsedSecond < 1 {
		elapsedSecond = 1
	}
	CheckSpeed := estimatedRows / elapsedSecond
	baseContext.Log.Infof("Info: record CRC32 checksum value is equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v, tableCheckSpeed= %+v rows/second.", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration, CheckSpeed)
	baseContext.Log.Infof("End check table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	return true, nil
}

// ChecksumPerTableViaTimeColumn 使用指定的时间字段增量核对.
// 返回该表是否核对一致；每个表只返回一次结果，由调用方负责写入结果通道。
func (job *ChecksumJob) ChecksumPerTableViaTimeColumn(baseContext *types.BaseContext, tableContext *types.TableContext) (isEqual bool, err error) {
	startTime := time.Now()
	var tableCheckDuration time.Duration
	defer func() {
		<-job.ChecksumJobChan
		job.wg.Done()
	}()

	ChecksumContext := checksum.NewChecksumContext(baseContext, tableContext)
	baseContext.Log.Infof("Starting check table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

	// 判断用户有没有输入checkcolumn，没有则取全表所有字段作为核对字段
	baseContext.Log.Debugf("Get user-request check columns of table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if ChecksumContext.CheckColumns == nil {
		if err := ChecksumContext.GetCheckColumns(); err != nil {
			return false, err
		}
	}

	// 获取时间列、最大最小值
	baseContext.Log.Debugf("GetTimeColumn of table pair: %s.%s => %s.%s.", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	if err := ChecksumContext.GetTimeColumn(); err != nil {
		return false, err
	}
	baseContext.Log.Debugf("Time column values range [%s-%s] of table pair: %s.%s => %s.%s .", ChecksumContext.Context.SpecifiedDatetimeRangeBegin, ChecksumContext.Context.SpecifiedDatetimeRangeEnd, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

	// 获取满足TimeRange核对条件的估算行数
	estimatedRows, err := ChecksumContext.EstimateTableRowsViaExplain()
	if err != nil {
		return false, err
	}

	// 计算checksum值
	var hasFurtherRange = true
	for hasFurtherRange {
		hasFurtherRange, err = ChecksumContext.CalculateNextIterationTimeRange()
		if err != nil {
			return false, err
		}
		baseContext.Log.Debugf("CalculateNextIterationTimeRange [hasFurtherRange=%t] of table pair: %s.%s => %s.%s.", hasFurtherRange, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)

		var isChunkChecksumEqual bool
		var duration time.Duration
		if hasFurtherRange {
			// 增加重试机制
			for i := 0; i < int(ChecksumContext.Context.DefaultNumRetries); i++ {
				if i != 0 {
					time.Sleep(1 * time.Second)
					baseContext.Log.Debugf("IterationTimeRangeQueryChecksum [%s-%s] retry times %d of table pair: %s.%s => %s.%s .", ChecksumContext.TimeIterationRangeMinValue, ChecksumContext.TimeIterationRangeMaxValue, i, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
				} else {
					baseContext.Log.Debugf("IterationTimeRangeQueryChecksum [%s-%s] of table pair: %s.%s => %s.%s .", ChecksumContext.TimeIterationRangeMinValue, ChecksumContext.TimeIterationRangeMaxValue, ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
				}
				isChunkChecksumEqual, duration, err = ChecksumContext.IterationTimeRangeQueryChecksum()
				if err == nil && isChunkChecksumEqual {
					break
				}
			}
			ChecksumContext.AddIteration()
			if err != nil {
				tableCheckDuration = time.Since(startTime)
				baseContext.Log.Errorf("Critical: record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration)
				return false, err
			}
			if !isChunkChecksumEqual {
				tableCheckDuration = time.Since(startTime)
				baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
				baseContext.Log.Errorf("Critical: record CRC32 checksum value is not equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration)

				// If differential reporting is enabled and we found differences, run detailed analysis.
				// Note: the differential analysis walks the unique key over the whole table.
				if baseContext.EnableDifferentialReporting {
					runDifferentialAnalysis(baseContext, ChecksumContext)
				}

				return false, nil
			}
			baseContext.Log.Debugf("Debug: Iteration %d, record CRC32 checksum value is equal of table pair: %s.%s => %s.%s , Duration=%+v", int(ChecksumContext.GetIteration()), ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, duration)
		}
	}

	tableCheckDuration = time.Since(startTime)
	elapsedSecond := int(tableCheckDuration / time.Second)
	if elapsedSecond < 1 {
		elapsedSecond = 1
	}
	CheckSpeed := estimatedRows / elapsedSecond
	baseContext.Log.Infof("Info: record CRC32 checksum value is equal of table pair: %s.%s => %s.%s , tableCheckDuration=%+v, tableCheckSpeed= %+v rows/second.", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName, tableCheckDuration, CheckSpeed)
	baseContext.Log.Infof("End check table pair: %s.%s => %s.%s .", ChecksumContext.PerTableContext.SourceDatabaseName, ChecksumContext.PerTableContext.SourceTableName, ChecksumContext.PerTableContext.TargetDatabaseName, ChecksumContext.PerTableContext.TargetTableName)
	return true, nil
}

// 核对任务
func (job *ChecksumJob) checksum(baseContext *types.BaseContext) {
	// 获取源表和目标表
	if err := GenerateTableList(baseContext); err != nil {
		baseContext.Log.Errorf("Generating source and target tables failed, %s", err.Error())
		baseContext.PanicAbort <- err
		return
	}

	// 逐个表进行核对, 按顺序去除map的元素
	tableNum := len(baseContext.PairOfSourceAndTargetTables)
	tableResultEqualNum := 0
	baseContext.ChecksumResChan = make(chan bool, tableNum)
	baseContext.ChecksumErrChan = make(chan error, tableNum)
	var keys []string
	for key := range baseContext.PairOfSourceAndTargetTables {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	baseContext.Log.Infof("%d pairs of source and target tables:", tableNum)
	for _, key := range keys {
		baseContext.Log.Infof("Table map: %s => %s .", key, baseContext.PairOfSourceAndTargetTables[key])
	}

	isDatetimeColumnSpecified := baseContext.IsDatetimeColumnSpecified()
	for _, key := range keys {
		sourceFullTableName := key
		targetFullTableName := baseContext.PairOfSourceAndTargetTables[key]
		sourceDatabase := strings.Split(sourceFullTableName, ".")[0]
		sourceTable := strings.Split(sourceFullTableName, ".")[1]
		targetDatabase := strings.Split(targetFullTableName, ".")[0]
		targetTable := strings.Split(targetFullTableName, ".")[1]
		tableContext := types.NewTableContext(sourceDatabase, sourceTable, targetDatabase, targetTable)

		job.ChecksumJobChan <- 1
		job.wg.Add(1)
		// 使用主键/时间字段核对。每个表只向结果通道写入一次 (result, error)。
		go func() {
			var isEqual bool
			var err error
			if isDatetimeColumnSpecified {
				isEqual, err = job.ChecksumPerTableViaTimeColumn(baseContext, tableContext)
			} else {
				isEqual, err = job.ChecksumPerTable(baseContext, tableContext)
			}
			baseContext.ChecksumResChan <- isEqual
			baseContext.ChecksumErrChan <- err
		}()
	}

	for i := 0; i < tableNum; i++ {
		if ret := <-baseContext.ChecksumResChan; ret {
			tableResultEqualNum += 1
		}
		err := <-baseContext.ChecksumErrChan
		if err != nil {
			baseContext.Log.Errorf("%s", err.Error())
		}
	}
	if tableResultEqualNum == tableNum {
		baseContext.Log.Infof("All %d pairs of tables check result is equal.", tableNum)
	} else {
		baseContext.Log.Errorf("Table records check result %d equal, %d not equal.", tableResultEqualNum, tableNum-tableResultEqualNum)
	}
	job.wg.Wait()
}

func main() {
	baseContext := types.NewBaseContext()
	flag.StringVar(&baseContext.SourceDatabases, "source-db-name", "", "Source database list separated by comma, eg: db1 or db1,db2.")
	flag.StringVar(&baseContext.SourceTables, "source-table-name", "", "Source tables list separated by comma, eg: table1 or table1,table2.")
	flag.StringVar(&baseContext.TargetDatabases, "target-db-name", "", "Target database list separated by comma, eg: db1 or db1,db2.")
	flag.StringVar(&baseContext.TargetTables, "target-table-name", "", "Target tables list separated by comma, eg: table1 or table1,table2.")
	flag.BoolVar(&baseContext.TargetDatabaseAsSource, "target-database-as-source", true, "Is target database name as source?  default: true.")
	flag.BoolVar(&baseContext.TargetTableAsSource, "target-table-as-source", true, "Is target table name as source? default: true.")
	flag.StringVar(&baseContext.TargetDatabaseAddSuffix, "target-database-add-suffix", "", "Target database name add a suffix to the source database name.")
	flag.StringVar(&baseContext.TargetTableAddSuffix, "target-table-add-suffix", "", "Target table name add a suffix to the source table name.")
	flag.StringVar(&baseContext.SourceTableNameRegexp, "source-table-regexp", "", "Source table names regular expression, eg: 'test_[0-9][0-9]\\.test_20.*'")

	flag.StringVar(&baseContext.SourceDBHost, "source-db-host", "127.0.0.1", "Source MySQL hostname")
	flag.IntVar(&baseContext.SourceDBPort, "source-db-port", 3306, "Source MySQL port")
	flag.StringVar(&baseContext.SourceDBUser, "source-db-user", "", "MySQL user")
	flag.StringVar(&baseContext.SourceDBPass, "source-db-password", "", "MySQL password")
	flag.StringVar(&baseContext.TargetDBHost, "target-db-host", "127.0.0.1", "Target MySQL hostname")
	flag.IntVar(&baseContext.TargetDBPort, "target-db-port", 3306, "Target MySQL port")
	flag.StringVar(&baseContext.TargetDBUser, "target-db-user", "", "MySQL user")
	flag.StringVar(&baseContext.TargetDBPass, "target-db-password", "", "MySQL password")
	flag.IntVar(&baseContext.Timeout, "conn-db-timeout", 60, "connect db timeout")
	flag.StringVar(&baseContext.RequestedColumnNames, "check-column-names", "", "Column names to check,eg: col1,col2,col3. By default, all columns are used.")
	flag.StringVar(&baseContext.SpecifiedDatetimeColumn, "specified-time-column", "", "Specified time column for range dataCheck.")
	flag.DurationVar(&baseContext.SpecifiedTimeRangePerStep, "time-range-per-step", 5*time.Minute, "time range per step for specified time column check,default 5m,eg:1h/2m/3s/4ms")
	specifiedDatetimeRangeBegin := flag.String("specified-time-begin", "", "Specified begin time of time column to check.")
	specifiedDatetimeRangeEnd := flag.String("specified-time-end", "", "Specified end time of time column to check.")
	chunkSize := flag.Int64("chunk-size", 1000, "amount of rows to handle in each iteration (allowed range: 10-100,000)")
	defaultRetries := flag.Int64("default-retries", 10, "Default number of retries for various operations before panicking")
	flag.BoolVar(&baseContext.EnableDifferentialReporting, "enable-differential-reporting", false, "Enable detailed differential reporting showing which records differ by primary key")
	flag.IntVar(&baseContext.MaxSampleDifferences, "max-sample-differences", 100, "Maximum number of sample differences to collect during analysis (default: 100)")
	flag.IntVar(&baseContext.MaxDisplayDifferences, "max-display-differences", 10, "Maximum number of differences to display in output (default: 10)")
	flag.BoolVar(&baseContext.GenerateSyncSQL, "generate-sync-sql", false, "Generate REPLACE INTO statements for synchronizing differences to a file")
	flag.StringVar(&baseContext.SyncSQLFile, "sync-sql-file", "", "Output file for sync SQL statements (default: stdout if not specified)")
	flag.BoolVar(&baseContext.IsSuperSetAsEqual, "is-superset-as-equal", false, "Shall we think that the records in target table is the superset of the source as equal? By default, we think the records are exactly equal as equal.")
	flag.BoolVar(&baseContext.IgnoreRowCountCheck, "ignore-row-count-check", false, "Shall we ignore check by counting rows? Default: false")
	flag.IntVar(&baseContext.ParallelThreads, "threads", 1, "Parallel threads of table checksum.")
	debug := flag.Bool("debug", false, "debug mode (very verbose)")
	logFile := flag.String("logfile", "", "Log file name.")
	version := flag.Bool("version", false, "Print version & exit")

	flag.Parse()
	if *version {
		appVersion := AppVersion
		if appVersion == "" {
			appVersion = "unversioned"
		}
		fmt.Println(appVersion)
		return
	}

	go baseContext.ListenOnPanicAbort()

	if err := baseContext.SetSpecifiedDatetimeRange(*specifiedDatetimeRangeBegin, *specifiedDatetimeRangeEnd); err != nil {
		baseContext.Log.Fatalf("Illegal time range for time column (%v), please check!", err)
	}
	baseContext.SetChunkSize(*chunkSize)
	baseContext.SetDefaultNumRetries(*defaultRetries)
	baseContext.SetLogLevel(*debug, *logFile)

	// GenerateSyncSQL appends per table within a run; truncate any stale file
	// from a previous run so the output only contains this run's statements.
	if baseContext.GenerateSyncSQL && baseContext.SyncSQLFile != "" {
		if file, err := os.OpenFile(baseContext.SyncSQLFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644); err != nil {
			baseContext.Log.Fatalf("Cannot write sync SQL file %s: %v", baseContext.SyncSQLFile, err)
		} else {
			file.Close()
		}
	}

	startTime := time.Now()
	defer func() {
		baseContext.CloseDB()
		baseContext.Log.Infof("Finished go-data-checksum. TotalDuration=%+v", time.Since(startTime))
	}()
	// 初始化源和目标库连接
	baseContext.Log.Infof("Starting go-data-checksum %+v...", AppVersion)
	if err := baseContext.InitDB(); err != nil {
		baseContext.Log.Fatalf("DB connection initiate failed: %v", err)
	}

	// 核对任务
	ChecksumJob := NewChecksumJob(baseContext.ParallelThreads)
	ChecksumJob.checksum(baseContext)

}
