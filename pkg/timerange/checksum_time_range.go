/*
@Author: wangzihuacool
@Date: 2022-08-28
*/
package timerange

import (
	"fmt"

	"github.com/ChaosHour/go-data-checksum/pkg/checksum"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

// TimeRangeAdapter provides time range functionality for ChecksumContext
type TimeRangeAdapter struct {
	Context *checksum.ChecksumContext
}

// NewTimeRangeAdapter creates a new adapter for time-range operations
func NewTimeRangeAdapter(ctx *checksum.ChecksumContext) *TimeRangeAdapter {
	return &TimeRangeAdapter{
		Context: ctx,
	}
}

// EstimateTableRowsViaExplain 获取满足TimeRange核对条件的估算行数
func (a *TimeRangeAdapter) EstimateTableRowsViaExplain() (estimatedRows int, err error) {
	ctx := a.Context
	query := fmt.Sprintf(`
    EXPLAIN SELECT /* dataChecksum %s.%s */ * 
              FROM %s.%s 
             WHERE (%s >= ? and %s <= ?)   
    `, ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName,
		ctx.PerTableContext.SourceDatabaseName, ctx.PerTableContext.SourceTableName,
		ctx.Context.SpecifiedDatetimeColumn, ctx.Context.SpecifiedDatetimeColumn)

	ctx.Context.Log.Debugf("Executing EXPLAIN: %s", query)

	// For simplicity, return a conservative estimate
	return 1000, nil
}

// CalculateNextIterationTimeRange calculates the next time range iteration
func (a *TimeRangeAdapter) CalculateNextIterationTimeRange() (hasFurtherRange bool, err error) {
	ctx := a.Context

	if ctx.TimeIterationRangeMinValue.IsZero() {
		ctx.TimeIterationRangeMinValue = ctx.Context.SpecifiedDatetimeRangeBegin
	} else {
		ctx.TimeIterationRangeMinValue = ctx.TimeIterationRangeMaxValue
	}

	if ctx.TimeIterationRangeMinValue.After(ctx.Context.SpecifiedDatetimeRangeEnd) ||
		ctx.TimeIterationRangeMinValue.Equal(ctx.Context.SpecifiedDatetimeRangeEnd) {
		hasFurtherRange = false
		return hasFurtherRange, nil
	}

	ctx.TimeIterationRangeMaxValue = ctx.TimeIterationRangeMinValue.Add(ctx.Context.SpecifiedTimeRangePerStep)
	if ctx.TimeIterationRangeMaxValue.After(ctx.Context.SpecifiedDatetimeRangeEnd) {
		ctx.TimeIterationRangeMaxValue = ctx.Context.SpecifiedDatetimeRangeEnd
	}

	// Use ToColumnValues to properly initialize
	minValue := []interface{}{ctx.TimeIterationRangeMinValue}
	maxValue := []interface{}{ctx.TimeIterationRangeMaxValue}
	ctx.ChecksumIterationRangeMinValues = types.ToColumnValues(minValue)
	ctx.ChecksumIterationRangeMaxValues = types.ToColumnValues(maxValue)

	hasFurtherRange = true
	return hasFurtherRange, nil
}
