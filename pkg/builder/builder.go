package builder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

type ValueComparisonSign string

const (
	LessThanComparisonSign            ValueComparisonSign = "<"
	LessThanOrEqualsComparisonSign    ValueComparisonSign = "<="
	EqualsComparisonSign              ValueComparisonSign = "="
	GreaterThanOrEqualsComparisonSign ValueComparisonSign = ">="
	GreaterThanComparisonSign         ValueComparisonSign = ">"
	NotEqualsComparisonSign           ValueComparisonSign = "!="
)

// EscapeName will escape a db/table/column/... name by wrapping with backticks.
// It is not fool proof. I'm just trying to do the right thing here, not solving
// SQL injection issues, which should be irrelevant for this tool.
func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}

// buildColumnsPreparedValues builds prepared-value placeholders for the columns, applying enum/JSON conversions where needed
func buildColumnsPreparedValues(columns *types.ColumnList) []string {
	values := make([]string, columns.Len())
	for i, column := range columns.Columns() {
		var token string
		if column.EnumToTextConversion {
			token = fmt.Sprintf("ELT(?, %s)", column.EnumValues)
		} else if column.Type == types.JSONColumnType {
			token = "convert(? using utf8mb4)"
		} else {
			token = "?"
		}
		values[i] = token
	}
	return values
}

// buildPreparedValues returns a list of "?" placeholders of the given length
func buildPreparedValues(length int) []string {
	values := make([]string, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return values
}

// duplicateNames returns a copy of the given names slice
func duplicateNames(names []string) []string {
	duplicate := make([]string, len(names))
	copy(duplicate, names)
	return duplicate
}

// BuildValueComparison builds a comparison expression such as "(column = ?)" or "(column > ?)"
func BuildValueComparison(column string, value string, comparisonSign ValueComparisonSign) (result string, err error) {
	if column == "" {
		return "", fmt.Errorf("empty column in GetValueComparison")
	}
	if value == "" {
		return "", fmt.Errorf("empty value in GetValueComparison")
	}
	comparison := fmt.Sprintf("(%s %s %s)", EscapeName(column), string(comparisonSign), value)
	return comparison, err
}

// BuildEqualsComparison builds the equality condition over all columns, e.g. ((col1 = ?) and (col2 = ?) and (col3 = ?))
func BuildEqualsComparison(columns []string, values []string) (result string, err error) {
	if len(columns) == 0 {
		return "", fmt.Errorf("got 0 columns in GetEqualsComparison")
	}
	if len(columns) != len(values) {
		return "", fmt.Errorf("got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	comparisons := []string{}
	for i, column := range columns {
		value := values[i]
		comparison, err := BuildValueComparison(column, value, EqualsComparisonSign)
		if err != nil {
			return "", err
		}
		comparisons = append(comparisons, comparison)
	}
	result = strings.Join(comparisons, " and ")
	result = fmt.Sprintf("(%s)", result)
	return result, nil
}

// BuildEqualsPreparedComparison builds the prepared equality WHERE condition over the columns, e.g. ((col1 = ?) and (col2 = ?) and (col3 = ?))
func BuildEqualsPreparedComparison(columns []string) (result string, err error) {
	values := buildPreparedValues(len(columns))
	return BuildEqualsComparison(columns, values)
}

// BuildSetPreparedClause builds the SET clause of an UPDATE statement, e.g. col1=?, col2=?
func BuildSetPreparedClause(columns *types.ColumnList) (result string, err error) {
	if columns.Len() == 0 {
		return "", fmt.Errorf("got 0 columns in BuildSetPreparedClause")
	}
	setTokens := []string{}
	for _, column := range columns.Columns() {
		var setToken string
		// convert the timezone to UTC (+00:00)
		if column.TimezoneConversion != nil {
			setToken = fmt.Sprintf("%s=convert_tz(?, '%s', '%s')", EscapeName(column.Name), column.TimezoneConversion.ToTimezone, "+00:00")
		} else if column.EnumToTextConversion {
			setToken = fmt.Sprintf("%s=ELT(?, %s)", EscapeName(column.Name), column.EnumValues)
		} else if column.Type == types.JSONColumnType {
			setToken = fmt.Sprintf("%s=convert(? using utf8mb4)", EscapeName(column.Name))
		} else {
			setToken = fmt.Sprintf("%s=?", EscapeName(column.Name))
		}
		setTokens = append(setTokens, setToken)
	}
	return strings.Join(setTokens, ", "), nil
}

// BuildRangeComparison builds the chunk-range WHERE condition over the unique key columns.
// Example result: "((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?)))"
// Example explodedArgs: "[v1, v1, v2, v1, v2, v3, v1, v2, v3]"
func BuildRangeComparison(columns []string, values []string, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	if len(columns) == 0 {
		return "", explodedArgs, fmt.Errorf("got 0 columns in GetRangeComparison")
	}
	if len(columns) != len(values) {
		return "", explodedArgs, fmt.Errorf("got %d columns but %d values in GetEqualsComparison", len(columns), len(values))
	}
	if len(columns) != len(args) {
		return "", explodedArgs, fmt.Errorf("got %d columns but %d args in GetEqualsComparison", len(columns), len(args))
	}
	includeEquals := false
	if comparisonSign == LessThanOrEqualsComparisonSign {
		comparisonSign = LessThanComparisonSign
		includeEquals = true
	}
	if comparisonSign == GreaterThanOrEqualsComparisonSign {
		comparisonSign = GreaterThanComparisonSign
		includeEquals = true
	}
	comparisons := []string{}

	for i, column := range columns {
		value := values[i]
		rangeComparison, err := BuildValueComparison(column, value, comparisonSign)
		if err != nil {
			return "", explodedArgs, err
		}
		if i > 0 {
			equalitiesComparison, err := BuildEqualsComparison(columns[0:i], values[0:i])
			if err != nil {
				return "", explodedArgs, err
			}
			comparison := fmt.Sprintf("(%s AND %s)", equalitiesComparison, rangeComparison)
			comparisons = append(comparisons, comparison)
			explodedArgs = append(explodedArgs, args[0:i]...)
			explodedArgs = append(explodedArgs, args[i])
		} else {
			comparisons = append(comparisons, rangeComparison)
			explodedArgs = append(explodedArgs, args[i])
		}
	}

	if includeEquals {
		comparison, err := BuildEqualsComparison(columns, values)
		if err != nil {
			return "", explodedArgs, nil
		}
		comparisons = append(comparisons, comparison)
		explodedArgs = append(explodedArgs, args...)
	}
	result = strings.Join(comparisons, " or ")
	result = fmt.Sprintf("(%s)", result)
	return result, explodedArgs, nil
}

// BuildRangePreparedComparison builds the prepared upper/lower chunk-range WHERE condition over the unique key columns
func BuildRangePreparedComparison(columns *types.ColumnList, args []interface{}, comparisonSign ValueComparisonSign) (result string, explodedArgs []interface{}, err error) {
	values := buildColumnsPreparedValues(columns)
	return BuildRangeComparison(columns.Names(), values, args, comparisonSign)
}

// BuildChunkChecksumSQL builds the SQL computing either the aggregated chunk CRC32 (BIT_XOR) or the per-row CRC32 values.
// The chunk range is (rangeMin, rangeMax]; the first chunk is [rangeMin, rangeMax].
// The final SQL looks like: select /* dataChecksum */
//
//	                  COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#',id, ftime, c1, c2)) as UNSIGNED)), 10, 16)), 0) as CRC32XOR
//	               OR COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#',id, ftime, c1, c2)) as UNSIGNED), 10, 16)), 0) as CRC32
//	             from test.t_time
//	            where (((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)))
//		             and ((col1 < ?) or ((col1 = ?) and (col2 < ?)) or (((col1 = ?) and (col2 = ?)) and (col3 < ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?))))
func BuildChunkChecksumSQL(databaseName, tableName string, checkColumns, uniqueKeyColumns *types.ColumnList, rangeStartValues, rangeEndValues []string, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, checkLevel int64) (result string, explodedArgs []interface{}, err error) {
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)

	checkColumnNames := duplicateNames(checkColumns.Names())
	for i := range checkColumnNames {
		checkColumnNames[i] = EscapeName(checkColumnNames[i])
	}
	checkColumnNamesListing := strings.Join(checkColumnNames, "), hex(")
	var builder strings.Builder
	builder.WriteString("hex(")
	builder.WriteString(checkColumnNamesListing)
	builder.WriteString(")")
	checkColumnNamesListing = builder.String()

	// ">" normally; ">=" when the range start value is included (first chunk)
	var minRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		minRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}

	// Build the range-start WHERE condition over the unique key.
	rangeStartComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(), rangeStartValues, rangeStartArgs, minRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	// Build the range-end WHERE condition over the unique key.
	rangeEndComparison, rangeExplodedArgs, err := BuildRangeComparison(uniqueKeyColumns.Names(), rangeEndValues, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	var checkClause string
	switch checkLevel {
	case 1:
		checkClause = fmt.Sprintf("COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#', %s)) as UNSIGNED)), 10, 16)), 0) as CRC32XOR",
			checkColumnNamesListing)
	case 2:
		checkClause = fmt.Sprintf("COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#', %s)) as UNSIGNED), 10, 16)), 0) as CRC32",
			checkColumnNamesListing)
	default:
		return "", nil, fmt.Errorf("critical: table %s.%s wrong checkLevelFlag input in BuildChunkChecksumSQL",
			databaseName, tableName)
	}

	// escaped unique key column names, plus asc ordering expressions
	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == types.EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
		}
	}

	result = fmt.Sprintf(`
      select /* dataChecksum %s.%s */ %s
        from %s.%s 
       where (%s and %s)
      order by %s
    `, databaseName, tableName, checkClause, databaseName, tableName,
		rangeStartComparison, rangeEndComparison, strings.Join(uniqueKeyColumnAscending, ", "),
	)
	return result, explodedArgs, nil
}

// BuildRangeChecksumPreparedQuery returns the prepared chunked CRC32 checksum SQL; the chunk range is (rangeMin, rangeMax], the first chunk [rangeMin, rangeMax]
func BuildRangeChecksumPreparedQuery(databaseName, tableName string, checkColumns, uniqueKeyColumns *types.ColumnList, rangeStartArgs, rangeEndArgs []interface{}, includeRangeStartValues bool, checkLevel int64) (result string, explodedArgs []interface{}, err error) {
	rangeStartValues := buildColumnsPreparedValues(uniqueKeyColumns)
	rangeEndValues := buildColumnsPreparedValues(uniqueKeyColumns)
	return BuildChunkChecksumSQL(databaseName, tableName, checkColumns, uniqueKeyColumns, rangeStartValues, rangeEndValues, rangeStartArgs, rangeEndArgs, includeRangeStartValues, checkLevel)
}

// BuildTimeRangeChecksumSQL builds the chunked CRC32 SQL over a time column.
// The chunk range is [rangeBegin, rangeEnd); the final chunk is [rangeBegin, rangeEnd] (includeRangeEnd=true).
// checkLevel=1 returns the aggregated CRC32XOR (order independent); checkLevel=2 returns per-row CRC32 values.
func BuildTimeRangeChecksumSQL(databaseName, tableName string, checkColumns *types.ColumnList, timeColumnName string, includeRangeEnd bool, checkLevel int64) (result string, err error) {
	if timeColumnName == "" {
		return "", fmt.Errorf("empty time column in BuildTimeRangeChecksumSQL")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	escapedTimeColumn := EscapeName(timeColumnName)

	checkColumnNames := duplicateNames(checkColumns.Names())
	for i := range checkColumnNames {
		checkColumnNames[i] = EscapeName(checkColumnNames[i])
	}
	checkColumnNamesListing := fmt.Sprintf("hex(%s)", strings.Join(checkColumnNames, "), hex("))

	var checkClause string
	switch checkLevel {
	case 1:
		checkClause = fmt.Sprintf("COALESCE(LOWER(CONV(BIT_XOR(cast(crc32(CONCAT_WS('#', %s)) as UNSIGNED)), 10, 16)), 0) as CRC32XOR",
			checkColumnNamesListing)
	case 2:
		checkClause = fmt.Sprintf("COALESCE(LOWER(CONV(cast(crc32(CONCAT_WS('#', %s)) as UNSIGNED), 10, 16)), 0) as CRC32",
			checkColumnNamesListing)
	default:
		return "", fmt.Errorf("critical: table %s.%s wrong checkLevelFlag input in BuildTimeRangeChecksumSQL",
			databaseName, tableName)
	}

	endComparisonSign := LessThanComparisonSign
	if includeRangeEnd {
		endComparisonSign = LessThanOrEqualsComparisonSign
	}

	var orderClause string
	if checkLevel == 2 {
		orderClause = fmt.Sprintf("order by %s asc", escapedTimeColumn)
	}

	result = fmt.Sprintf(`
      select /* dataChecksum %s.%s */ %s
        from %s.%s
       where (%s >= ? and %s %s ?)
      %s
    `, databaseName, tableName, checkClause, databaseName, tableName,
		escapedTimeColumn, escapedTimeColumn, string(endComparisonSign), orderClause,
	)
	return result, nil
}

// BuildTimeRangeEstimateQuery builds the EXPLAIN SQL used to estimate the row count within a time range
func BuildTimeRangeEstimateQuery(databaseName, tableName, timeColumnName string) string {
	return fmt.Sprintf(`
      EXPLAIN select /* dataChecksum */ 1
        from %s.%s
       where (%s >= ? and %s <= ?)
    `, EscapeName(databaseName), EscapeName(tableName),
		EscapeName(timeColumnName), EscapeName(timeColumnName))
}

// BuildUniqueKeyRangeEndPreparedQueryViaOffset builds the query that finds the current chunk's upper boundary via LIMIT/OFFSET
// The final SQL looks like: select /* dataChecksum db.tab iteration:5 */
//
//				col1, col2, col3
//			from
//				db.tab
//		   where ((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)))
//	         and ((col1 < ?) or ((col1 = ?) and (col2 < ?)) or (((col1 = ?) and (col2 = ?)) and (col3 < ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?)))
//		order by
//				col1 asc, col2 asc, col3 asc
//		 limit 1
//		 offset {chunkSize -1}
func BuildUniqueKeyRangeEndPreparedQueryViaOffset(databaseName, tableName string, uniqueKeyColumns *types.ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string, indexName string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", explodedArgs, fmt.Errorf("got 0 columns in BuildUniqueKeyRangeEndPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	indexName = EscapeName(indexName)

	// ">=" when the range start value is included; ">" otherwise
	var startRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		startRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}

	rangeStartComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeStartArgs, startRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	// escaped unique key column names, plus asc/desc ordering expressions
	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == types.EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("concat(%s) desc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumnNames[i])
		}
	}
	result = fmt.Sprintf(`
				select  /* dataChecksum %s.%s %s */
						%s
					from
						%s.%s force index(%s)
					where %s and %s
					order by
						%s
					limit 1
					offset %d
    `, databaseName, tableName, hint,
		strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName, indexName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "),
		(chunkSize - 1),
	)
	return result, explodedArgs, nil
}

// BuildUniqueKeyRangeEndPreparedQueryViaTemptable builds the chunk upper-boundary query via a derived table; slightly different from the offset variant
// The final SQL looks like: select /* dataChecksum db.tab iteration:5 */
//
//	                    col1, col2, col3
//					 from (
//						select
//								col1, col2, col3
//							from
//								db.tab
//							where ((col1 > ?) or ((col1 = ?) and (col2 > ?)) or (((col1 = ?) and (col2 = ?)) and (col3 > ?)))
//	                       and ((col1 < ?) or ((col1 = ?) and (col2 < ?)) or (((col1 = ?) and (col2 = ?)) and (col3 < ?)) or ((col1 = ?) and (col2 = ?) and (col3 = ?)))
//							order by
//								col1 asc, col2 asc, col3 asc
//							limit {chunkSize}
//					) select_osc_chunk
//				order by
//					col1 desc, col2 desc, col3 desc
//				limit 1
func BuildUniqueKeyRangeEndPreparedQueryViaTemptable(databaseName, tableName string, uniqueKeyColumns *types.ColumnList, rangeStartArgs, rangeEndArgs []interface{}, chunkSize int64, includeRangeStartValues bool, hint string, indexName string) (result string, explodedArgs []interface{}, err error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", explodedArgs, fmt.Errorf("got 0 columns in BuildUniqueKeyRangeEndPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	indexName = EscapeName(indexName)

	// ">=" when the range start value is included; ">" otherwise
	var startRangeComparisonSign ValueComparisonSign = GreaterThanComparisonSign
	if includeRangeStartValues {
		startRangeComparisonSign = GreaterThanOrEqualsComparisonSign
	}

	rangeStartComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeStartArgs, startRangeComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)
	rangeEndComparison, rangeExplodedArgs, err := BuildRangePreparedComparison(uniqueKeyColumns, rangeEndArgs, LessThanOrEqualsComparisonSign)
	if err != nil {
		return "", explodedArgs, err
	}
	explodedArgs = append(explodedArgs, rangeExplodedArgs...)

	// escaped unique key column names, plus asc/desc ordering expressions
	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnAscending := make([]string, len(uniqueKeyColumnNames))
	uniqueKeyColumnDescending := make([]string, len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == types.EnumColumnType {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("concat(%s) asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("concat(%s) desc", uniqueKeyColumnNames[i])
		} else {
			uniqueKeyColumnAscending[i] = fmt.Sprintf("%s asc", uniqueKeyColumnNames[i])
			uniqueKeyColumnDescending[i] = fmt.Sprintf("%s desc", uniqueKeyColumnNames[i])
		}
	}
	result = fmt.Sprintf(`
      select /* dataChecksum %s.%s %s */ %s
				from (
					select
							%s
						from
							%s.%s force index(%s)
						where %s and %s
						order by
							%s
						limit %d
				) select_osc_chunk
			order by
				%s
			limit 1
    `, databaseName, tableName, hint, strings.Join(uniqueKeyColumnNames, ", "),
		strings.Join(uniqueKeyColumnNames, ", "), databaseName, tableName, indexName,
		rangeStartComparison, rangeEndComparison,
		strings.Join(uniqueKeyColumnAscending, ", "), chunkSize,
		strings.Join(uniqueKeyColumnDescending, ", "),
	)
	return result, explodedArgs, nil
}

// BuildUniqueKeyMinValuesPreparedQuery builds the SQL fetching the unique key minimum values
func BuildUniqueKeyMinValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns *types.ColumnList) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, uniqueKeyColumns, "asc")
}

// BuildUniqueKeyMaxValuesPreparedQuery builds the SQL fetching the unique key maximum values
func BuildUniqueKeyMaxValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns *types.ColumnList) (string, error) {
	return buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName, uniqueKeyColumns, "desc")
}

// buildUniqueKeyMinMaxValuesPreparedQuery builds the shared query; asc/desc ordering selects the minimum or maximum values
func buildUniqueKeyMinMaxValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns *types.ColumnList, order string) (string, error) {
	if uniqueKeyColumns.Len() == 0 {
		return "", fmt.Errorf("got 0 columns in BuildUniqueKeyMinMaxValuesPreparedQuery")
	}
	databaseName = EscapeName(databaseName)
	tableName = EscapeName(tableName)
	uniqueKeyColumnNames := duplicateNames(uniqueKeyColumns.Names())
	uniqueKeyColumnOrder := make([]string, len(uniqueKeyColumnNames))
	for i, column := range uniqueKeyColumns.Columns() {
		uniqueKeyColumnNames[i] = EscapeName(uniqueKeyColumnNames[i])
		if column.Type == types.EnumColumnType {
			uniqueKeyColumnOrder[i] = fmt.Sprintf("concat(%s) %s", uniqueKeyColumnNames[i], order)
		} else {
			uniqueKeyColumnOrder[i] = fmt.Sprintf("%s %s", uniqueKeyColumnNames[i], order)
		}
	}
	// select /* dataChecksum `db`.`tab` */ col1,col2 from `db`.`tab` order by col1 asc/desc, col2 asc/desc limit 1
	query := fmt.Sprintf(`
      select /* dataChecksum %s.%s */ %s
				from
					%s.%s
				order by
					%s
				limit 1
    `, databaseName, tableName, strings.Join(uniqueKeyColumnNames, ", "),
		databaseName, tableName,
		strings.Join(uniqueKeyColumnOrder, ", "),
	)
	return query, nil
}
