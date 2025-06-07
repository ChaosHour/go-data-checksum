package types

import (
	gosql "database/sql"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

type ColumnType int

const (
	UnknownColumnType ColumnType = iota
	TimestampColumnType
	DateTimeColumnType
	EnumColumnType
	MediumIntColumnType
	JSONColumnType
	FloatColumnType
	BinaryColumnType
)

const maxMediumintUnsigned int32 = 16777215

type TableContext struct {
	SourceDatabaseName string
	SourceTableName    string
	TargetDatabaseName string
	TargetTableName    string
	FinishedFlag       int64
	Iteration          int64
}

func NewTableContext(sourceDatabaseName, sourceTableName, targetDatabaseName, targetTableName string) *TableContext {
	return &TableContext{
		SourceDatabaseName: sourceDatabaseName,
		SourceTableName:    sourceTableName,
		TargetDatabaseName: targetDatabaseName,
		TargetTableName:    targetTableName,
		FinishedFlag:       0,
		Iteration:          0,
	}
}

type BaseContext struct {
	SourceDBHost string
	SourceDBPort int
	SourceDBUser string
	SourceDBPass string
	TargetDBHost string
	TargetDBPort int
	TargetDBUser string
	TargetDBPass string
	Timeout      int
	SourceDB     *gosql.DB
	TargetDB     *gosql.DB

	SourceDatabases         string
	SourceTables            string
	TargetDatabases         string
	TargetTables            string
	TargetDatabaseAsSource  bool
	TargetTableAsSource     bool
	TargetDatabaseAddSuffix string
	TargetTableAddSuffix    string
	SourceTableNameRegexp   string
	TableQueryHint          string

	SourceTableFullNameList     []string
	TargetTableFullNameList     []string
	PairOfSourceAndTargetTables map[string]string
	SourceDatabaseList          []string
	SourceTableList             []string
	TargetDatabaseList          []string
	TargetTableList             []string
	RequestedColumnNames        string
	SpecifiedDatetimeColumn     string
	SpecifiedTimeRangePerStep   time.Duration
	SpecifiedDatetimeRangeBegin time.Time
	SpecifiedDatetimeRangeEnd   time.Time
	IgnoreRowCountCheck         bool

	ChunkSize                   int64
	DefaultNumRetries           int64
	IsSuperSetAsEqual           bool
	EnableDifferentialReporting bool
	ParallelThreads             int
	ChecksumResChan             chan bool
	ChecksumErrChan             chan error
	PanicAbort                  chan error
	throttleMutex               *sync.Mutex
	Log                         *log.Logger
	Logfile                     string
}

func NewBaseContext() *BaseContext {
	return &BaseContext{
		ChunkSize:         1000,
		DefaultNumRetries: 10,
		PanicAbort:        make(chan error),
		throttleMutex:     &sync.Mutex{},
		Log:               log.New(),
	}
}

// SetChunkSize 设置chunksize范围：10-100000
func (ctx *BaseContext) SetChunkSize(chunkSize int64) {
	if chunkSize < 10 {
		chunkSize = 10
	}
	if chunkSize > 100000 {
		chunkSize = 100000
	}
	atomic.StoreInt64(&ctx.ChunkSize, chunkSize)
}

// SetDefaultNumRetries 设置最大重试次数,默认10
func (ctx *BaseContext) SetDefaultNumRetries(retries int64) {
	ctx.throttleMutex.Lock()
	defer ctx.throttleMutex.Unlock()
	if retries > 0 {
		ctx.DefaultNumRetries = retries
	}
}

// SetLogLevel 设置日志级别
func (ctx *BaseContext) SetLogLevel(debug bool, logFile string) {
	ctx.Log.SetLevel(log.InfoLevel)
	if debug {
		ctx.Log.SetLevel(log.DebugLevel)
	}
	ctx.Log.SetOutput(os.Stdout)
	if logFile != "" {
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			ctx.Log.Out = file
		} else {
			ctx.Log.Infof("Failed to log to file, using defualt stdout.")
		}
	}
}

// SetSpecifiedDatetimeRange 设置按时间字段核对的起止时间
func (ctx *BaseContext) SetSpecifiedDatetimeRange(specifiedTimeBegin, specifiedTimeEnd string) (err error) {
	if specifiedTimeBegin != "" {
		if ctx.SpecifiedDatetimeRangeBegin, err = time.ParseInLocation("2006-01-02 15:04:05", specifiedTimeBegin, time.Local); err != nil {
			return err
		}
	}
	if specifiedTimeEnd != "" {
		if ctx.SpecifiedDatetimeRangeEnd, err = time.ParseInLocation("2006-01-02 15:04:05", specifiedTimeEnd, time.Local); err != nil {
			return err
		}
	}
	if ctx.SpecifiedDatetimeRangeEnd.Before(ctx.SpecifiedDatetimeRangeBegin) {
		return fmt.Errorf("illegal time range, specified-time-end is before specified-time-begin")
	}
	return nil
}

// IsDatetimeColumnSpecified Check whether datetime column is specified and begin/end time provided.
func (ctx *BaseContext) IsDatetimeColumnSpecified() bool {
	if ctx.SpecifiedDatetimeColumn != "" && !ctx.SpecifiedDatetimeRangeBegin.IsZero() && !ctx.SpecifiedDatetimeRangeEnd.IsZero() {
		return true
	} else {
		return false
	}
}

func (ctx *BaseContext) GetDBUri(databaseName string) (string, string) {
	sourceDBUri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local&timeout=%ds&readTimeout=%ds&writeTimeout=%ds&interpolateParams=true&charset=latin1", ctx.SourceDBUser, ctx.SourceDBPass, ctx.SourceDBHost, ctx.SourceDBPort, databaseName, ctx.Timeout, ctx.Timeout, ctx.Timeout)
	targetDBUri := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=Local&timeout=%ds&readTimeout=%ds&writeTimeout=%ds&interpolateParams=true&charset=latin1", ctx.TargetDBUser, ctx.TargetDBPass, ctx.TargetDBHost, ctx.TargetDBPort, databaseName, ctx.Timeout, ctx.Timeout, ctx.Timeout)
	return sourceDBUri, targetDBUri
}

// InitDB 建立数据库连接
func (ctx *BaseContext) InitDB() (err error) {
	databaseName := "information_schema"
	sourceDBUri, targetDBUri := ctx.GetDBUri(databaseName)

	initDBConnect := func(dbUri string) (db *gosql.DB, err error) {
		if db, err = gosql.Open("mysql", dbUri); err != nil {
			return db, err
		}
		if err = db.Ping(); err != nil {
			return db, err
		}
		db.SetConnMaxLifetime(time.Minute * 3)
		db.SetMaxIdleConns(30)
		return db, err
	}

	if ctx.SourceDB, err = initDBConnect(sourceDBUri); err != nil {
		return err
	}
	if ctx.TargetDB, err = initDBConnect(targetDBUri); err != nil {
		return err
	}
	return nil
}

// CloseDB 关闭数据库连接
func (ctx *BaseContext) CloseDB() {
	ctx.TargetDB.Close()
	ctx.SourceDB.Close()
}

// ListenOnPanicAbort aborts on abort request
func (ctx *BaseContext) ListenOnPanicAbort() {
	err := <-ctx.PanicAbort
	ctx.Log.Fatalf(err.Error())
}

type TimezoneConversion struct {
	ToTimezone string
}

type Column struct {
	Name                 string
	IsUnsigned           bool
	Charset              string
	Type                 ColumnType
	EnumValues           string
	TimezoneConversion   *TimezoneConversion
	EnumToTextConversion bool
	BinaryOctetLength    uint
}

func NewColumns(names []string) []Column {
	result := make([]Column, len(names))
	for i := range names {
		result[i].Name = names[i]
	}
	return result
}

func ParseColumns(names string) []Column {
	namesArray := strings.Split(names, ",")
	return NewColumns(namesArray)
}

// ColumnsMap maps a column name onto its ordinal position
type ColumnsMap map[string]int

func NewEmptyColumnsMap() ColumnsMap {
	columnsMap := make(map[string]int)
	return ColumnsMap(columnsMap)
}

func NewColumnsMap(orderedColumns []Column) ColumnsMap {
	columnsMap := NewEmptyColumnsMap()
	for i, column := range orderedColumns {
		columnsMap[column.Name] = i
	}
	return columnsMap
}

// ColumnList makes for a named list of columns
type ColumnList struct {
	columns  []Column
	Ordinals ColumnsMap
}

// NewColumnList creates an object given ordered list of column names
func NewColumnList(names []string) *ColumnList {
	result := &ColumnList{
		columns: NewColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

// ParseColumnList parses a comma delimited list of column names
func ParseColumnList(names string) *ColumnList {
	result := &ColumnList{
		columns: ParseColumns(names),
	}
	result.Ordinals = NewColumnsMap(result.columns)
	return result
}

func (cl *ColumnList) Columns() []Column {
	return cl.columns
}

func (cl *ColumnList) Names() []string {
	names := make([]string, len(cl.columns))
	for i := range cl.columns {
		names[i] = cl.columns[i].Name
	}
	return names
}

func (cl *ColumnList) GetColumn(columnName string) *Column {
	if ordinal, ok := cl.Ordinals[columnName]; ok {
		return &cl.columns[ordinal]
	}
	return nil
}

func (cl *ColumnList) SetUnsigned(columnName string) {
	cl.GetColumn(columnName).IsUnsigned = true
}

func (cl *ColumnList) IsUnsigned(columnName string) bool {
	return cl.GetColumn(columnName).IsUnsigned
}

func (cl *ColumnList) SetCharset(columnName string, charset string) {
	cl.GetColumn(columnName).Charset = charset
}

func (cl *ColumnList) GetCharset(columnName string) string {
	return cl.GetColumn(columnName).Charset
}

func (cl *ColumnList) SetColumnType(columnName string, columnType ColumnType) {
	cl.GetColumn(columnName).Type = columnType
}

func (cl *ColumnList) GetColumnType(columnName string) ColumnType {
	return cl.GetColumn(columnName).Type
}

func (cl *ColumnList) SetConvertDatetimeToTimestamp(columnName string, toTimezone string) {
	cl.GetColumn(columnName).TimezoneConversion = &TimezoneConversion{ToTimezone: toTimezone}
}

func (cl *ColumnList) HasTimezoneConversion(columnName string) bool {
	return cl.GetColumn(columnName).TimezoneConversion != nil
}

func (cl *ColumnList) SetEnumToTextConversion(columnName string) {
	cl.GetColumn(columnName).EnumToTextConversion = true
}

func (cl *ColumnList) IsEnumToTextConversion(columnName string) bool {
	return cl.GetColumn(columnName).EnumToTextConversion
}

func (cl *ColumnList) SetEnumValues(columnName string, enumValues string) {
	cl.GetColumn(columnName).EnumValues = enumValues
}

func (cl *ColumnList) String() string {
	return strings.Join(cl.Names(), ",")
}

func (cl *ColumnList) Equals(other *ColumnList) bool {
	return reflect.DeepEqual(cl.Columns, other.Columns)
}

func (cl *ColumnList) EqualsByNames(other *ColumnList) bool {
	return reflect.DeepEqual(cl.Names(), other.Names())
}

// IsSubsetOf returns 'true' when column names of this list are a subset of
// another list, in arbitrary order (order agnostic)
func (cl *ColumnList) IsSubsetOf(other *ColumnList) bool {
	for _, column := range cl.columns {
		if _, exists := other.Ordinals[column.Name]; !exists {
			return false
		}
	}
	return true
}

func (cl *ColumnList) Len() int {
	return len(cl.columns)
}

// UniqueKey is the combination of a key's name and columns
type UniqueKey struct {
	Name            string
	Columns         ColumnList
	HasNullable     bool
	IsAutoIncrement bool
}

// IsPrimary checks if this unique key is primary
func (uk *UniqueKey) IsPrimary() bool {
	return uk.Name == "PRIMARY"
}

func (uk *UniqueKey) Len() int {
	return uk.Columns.Len()
}

func (uk *UniqueKey) String() string {
	description := uk.Name
	if uk.IsAutoIncrement {
		description = fmt.Sprintf("%s (auto_increment)", description)
	}
	return fmt.Sprintf("%s: %s; has nullable: %+v", description, uk.Columns.Names(), uk.HasNullable)
}

type ColumnValues struct {
	abstractValues []interface{}
	ValuesPointers []interface{}
}

// NewColumnValues 将abstractValues的值复制到ValuesPointers
func NewColumnValues(length int) *ColumnValues {
	result := &ColumnValues{
		abstractValues: make([]interface{}, length),
		ValuesPointers: make([]interface{}, length),
	}
	for i := 0; i < length; i++ {
		result.ValuesPointers[i] = &result.abstractValues[i]
	}
	return result
}

func ToColumnValues(abstractValues []interface{}) *ColumnValues {
	result := &ColumnValues{
		abstractValues: abstractValues,
		ValuesPointers: make([]interface{}, len(abstractValues)),
	}
	for i := 0; i < len(abstractValues); i++ {
		result.ValuesPointers[i] = &result.abstractValues[i]
	}
	return result
}

func (cv *ColumnValues) AbstractValues() []interface{} {
	return cv.abstractValues
}

// StringColumn 返回二进制对应的ASCII字符串
func (cv *ColumnValues) StringColumn(index int) string {
	val := cv.AbstractValues()[index]
	// uint8 the set of all unsigned  8-bit integers (0 to 255),等同于byte
	if ints, ok := val.([]uint8); ok {
		// string([]uint8) 返回byte对应的字符串
		return string(ints)
	}
	return fmt.Sprintf("%+v", val)
}

func (cv *ColumnValues) String() string {
	stringValues := []string{}
	for i := range cv.AbstractValues() {
		stringValues = append(stringValues, cv.StringColumn(i))
	}
	return strings.Join(stringValues, ",")
}

// EscapeName will escape a db/table/column/... name by wrapping with backticks.
func EscapeName(name string) string {
	if unquoted, err := strconv.Unquote(name); err == nil {
		name = unquoted
	}
	return fmt.Sprintf("`%s`", name)
}
