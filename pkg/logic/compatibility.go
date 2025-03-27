// This package provides backward compatibility while refactoring
package compatibility

import (
	"github.com/ChaosHour/go-data-checksum/pkg/builder"
	"github.com/ChaosHour/go-data-checksum/pkg/checksum"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

// Re-export all types and functions
type BaseContext = types.BaseContext
type TableContext = types.TableContext
type ColumnList = types.ColumnList
type ColumnValues = types.ColumnValues
type ChecksumContext = checksum.ChecksumContext

// Functions
func NewBaseContext() *BaseContext {
	return types.NewBaseContext()
}

func NewTableContext(sourceDatabaseName, sourceTableName, targetDatabaseName, targetTableName string) *TableContext {
	return types.NewTableContext(sourceDatabaseName, sourceTableName, targetDatabaseName, targetTableName)
}

func NewChecksumContext(context *BaseContext, perTableContext *TableContext) *ChecksumContext {
	return checksum.NewChecksumContext(context, perTableContext)
}

func NewColumnValues(length int) *ColumnValues {
	return types.NewColumnValues(length)
}

func ParseColumnList(names string) *ColumnList {
	return types.ParseColumnList(names)
}

// EscapeName re-export
func EscapeName(name string) string {
	return types.EscapeName(name)
}

// Add builder functions for compatibility
func BuildUniqueKeyMinValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns *ColumnList) (string, error) {
	return builder.BuildUniqueKeyMinValuesPreparedQuery(databaseName, tableName, uniqueKeyColumns)
}

func BuildUniqueKeyMaxValuesPreparedQuery(databaseName, tableName string, uniqueKeyColumns *ColumnList) (string, error) {
	return builder.BuildUniqueKeyMaxValuesPreparedQuery(databaseName, tableName, uniqueKeyColumns)
}
