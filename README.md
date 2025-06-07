# go-data-checksum 数据核对工具

## DESCRIPTION
go-data-checksum is a high-performance data check tool to verify data integrity between MySQL databases/tables. go-data-checksum supports full data check via primary key and incremental data check via specified time field; supports full field check or specified field check also.

go-data-checksum是一款高性能的MySQL数据库/表数据核对工具。go-data-checksum 可以支持按照主键的全量数据核对，和按照时间字段的增量数据核对；可以支持全字段核对或者指定字段核对。
go-data-checksum 可以支持跨MySQL实例的多表并行核对，并且支持目标表的数据是源表的超集的场景。核对原理为，计算并比较待核对数据的CRC32值。

## NEW FEATURES

### Enhanced Differential Reporting
- **Record-level Analysis**: Shows exactly which records differ by primary key
- **Three Types of Differences**:
  - `- (minus)`: Records that exist only in the source table
  - `+ (plus)`: Records that exist only in the target table  
  - `~ (tilde)`: Records that exist in both but have different data
- **Sample Output**: Shows actual primary key values for differing records
- **Performance Optimized**: Processes data in chunks to handle large tables

### Automatic Primary Key Detection
The tool automatically detects and uses the best unique key for data chunking:
- Prioritizes PRIMARY KEY when available
- Falls back to other unique indexes
- Prefers non-nullable keys
- Optimizes for integer data types
- Handles composite keys intelligently

## BUILD
```
go build -o bin/go-data-checksum cmd/checksum/main.go
```

Or use the Makefile:
```
make build
```

## USAGE
```
# 使用帮助
./bin/go-data-checksum --help

  -check-column-names string
        Column names to check,eg: col1,col2,col3. By default, all columns are used.
  -chunk-size int
        amount of rows to handle in each iteration (allowed range: 10-100,000) (default 1000)
  -conn-db-timeout int
        connect db timeout (default 30)
  -debug
        debug mode (very verbose)
  -default-retries int
        Default number of retries for various operations before panicking (default 10)
  -enable-differential-reporting
        Enable detailed differential reporting showing which records differ by primary key (default false)
  -ignore-row-count-check
        Shall we ignore check by counting rows? Default: false
  -is-superset-as-equal
        Shall we think that the records in target table is the superset of the source as equal? By default, we think the records are exactly equal as equal.
  -logfile string
        Log file name.
  -source-db-host string
        Source MySQL hostname (default "127.0.0.1")
  -source-db-name string
        Source database list separated by comma, eg: db1 or db1,db2.
  -source-db-password string
        MySQL password
  -source-db-port int
        Source MySQL port (default 3306)
  -source-db-user string
        MySQL user
  -source-table-name string
        Source tables list separated by comma, eg: table1 or table1,table2.
  -source-table-regexp string
        Source table names regular expression, eg: 'test_[0-9][0-9]\\.test_20.*'
  -specified-time-begin string
        Specified begin time of time column to check.
  -specified-time-column string
        Specified time column for range dataCheck.
  -specified-time-end string
        Specified end time of time column to check.
  -target-database-add-suffix string
        Target database name add a suffix to the source database name.
  -target-database-as-source
        Is target database name as source?  default: true. (default true)
  -target-db-host string
        Target MySQL hostname (default "127.0.0.1")
  -target-db-name string
        Target database list separated by comma, eg: db1 or db1,db2.
  -target-db-password string
        MySQL password
  -target-db-port int
        Target MySQL port (default 3306)
  -target-db-user string
        MySQL user
  -target-table-add-suffix string
        Target table name add a suffix to the source table name.
  -target-table-as-source
        Is target table name as source? default: true. (default true)
  -target-table-name string
        Target tables list separated by comma, eg: table1 or table1,table2.
  -threads int
        Parallel threads of table checksum. (default 1)
  -time-range-per-step duration
        time range per step for specified time column check,default 5m,eg:1h/2m/3s/4ms (default 5m0s)
```

## EXAMPLES

### 1. Your Original Use Case Enhanced with Differential Reporting
```bash
# Compare all tables in a specific database with detailed difference reporting
./bin/go-data-checksum \
  --source-db-host=dev-sandbox.com \
  --source-db-port=3306 \
  --source-db-user=flyway \
  --source-db-password="xxxx" \
  --target-db-host=dev-sandbox.com \
  --target-db-port=3306 \
  --target-db-user=flyway \
  --target-db-password="xxxx" \
  --source-db-name="dba" \
  --ignore-row-count-check \
  --is-superset-as-equal \
  --enable-differential-reporting \
  --threads=4
```

### 2. Single Table Detailed Analysis
```bash
# Get detailed differences for a specific table
./bin/go-data-checksum \
  --source-db-host="source.example.com" \
  --source-db-port=3306 \
  --source-db-user="user" \
  --source-db-password="pass" \
  --target-db-host="target.example.com" \
  --target-db-port=3306 \
  --target-db-user="user" \
  --target-db-password="pass" \
  --source-db-name="app_db" \
  --source-table-name="users" \
  --enable-differential-reporting \
  --debug \
  --threads=1
```

## UNDERSTANDING DIFFERENTIAL OUTPUT

### Sample Output with --enable-differential-reporting
```
=== DIFFERENTIAL ANALYSIS RESULTS ===
Table Pair: dba.users => dba.users
- 128799 records exist only in SOURCE
+ 135 records exist only in TARGET
~ 45 records have different data
= 1234567 records are identical

=== SAMPLE DIFFERENCES ===
- Record (id=123, tenant_id=456) exists only in source
- Record (id=789, tenant_id=999) exists only in source
+ Record (id=555, tenant_id=456) exists only in target
+ Record (id=666, tenant_id=789) exists only in target
~ Record (id=999, tenant_id=456) modified: source_checksum=abc123, target_checksum=def456
~ Record (id=111, tenant_id=789) modified: source_checksum=xyz789, target_checksum=qwe456
=== END DIFFERENTIAL ANALYSIS ===
```

### Explanation of Symbols
- **`-` (minus)**: Records that exist in the source database but are missing in the target
- **`+` (plus)**: Records that exist in the target database but are missing in the source  
- **`~` (tilde)**: Records that exist in both databases but have different data (checksum mismatch)
- **`=` (equals)**: Records that are identical in both databases

**Now you can use differential reporting by adding `--enable-differential-reporting` to your existing command!**

## TEST
```bash
# Enhanced test command with differential reporting
./bin/go-data-checksum \
  --source-db-host="1.1.1.1" \
  --source-db-port=3307 \
  --source-db-user="test" \
  --source-db-password="xxxx" \
  --target-db-host="8.8.8.8" \
  --target-db-port=3306 \
  --target-db-user="test" \
  --target-db-password="xxxx" \
  --source-table-regexp="test\.sbtest.*" \
  --ignore-row-count-check \
  --is-superset-as-equal \
  --enable-differential-reporting \
  --threads=4
```

## TRACKING FUNCTIONALITY

go-data-checksum 提供了增强的跟踪功能，允许用户更精确地监控和记录数据核对过程中的各个步骤。通过以下选项，用户可以启用并配置跟踪功能：

  -enable-tracking
        Enable tracking functionality. Default: false
  -tracking-db-host string
        Tracking MySQL hostname for storing tracking information (default "127.0.0.1")
  -tracking-db-name string
        Tracking database name for storing tracking information.
  -tracking-db-user string
        MySQL user for tracking database.
  -tracking-db-password string
        MySQL password for tracking database.
  -tracking-table-name string
        Table name for storing tracking information.
  -tracking-log-file string
        Log file for tracking information.
  -tracking-level int
        Level of tracking information to be logged. (default 1)
  -tracking-format string
        Format of tracking information, eg: json or csv (default "json")

通过以上选项，用户可以将跟踪信息存储到指定的 MySQL 数据库中，或者输出到指定的日志文件中。跟踪信息包括但不限于：核对开始时间、结束时间、持续时长、处理的表名、记录数等。

## Testing

```bash
# First, find the middle range of IDs
mysql --defaults-group-suffix=_replica1 -e "SELECT MIN(id) as min_id, MAX(id) as max_id, COUNT(*) as total FROM sbtest.sbtest3"

# Delete 5 records from around the middle (assuming IDs are roughly 1-100000)
mysql --defaults-group-suffix=_replica1 -e "DELETE FROM sbtest.sbtest3 WHERE id BETWEEN 50000 AND 50004 LIMIT 5"

# Verify the deletion
mysql --defaults-group-suffix=_replica1 -e "SELECT COUNT(*) FROM sbtest.sbtest3"
mysql --defaults-group-suffix=_replica1 -e "SELECT id FROM sbtest.sbtest3 WHERE id BETWEEN 49995 AND 50010 ORDER BY id"

mysql --defaults-group-suffix=_replica1 -e "SELECT MIN(id) as min_id, MAX(id) as max_id, COUNT(*) as total FROM sbtest.sbtest3"
+--------+--------+--------+
| min_id | max_id | total  |
+--------+--------+--------+
|      1 | 100000 | 100000 |
+--------+--------+--------+

mysql --defaults-group-suffix=_replica1 -e "DELETE FROM sbtest.sbtest3 WHERE id BETWEEN 50000 AND 50004 LIMIT 5"

mysql --defaults-group-suffix=_replica1 -e "SELECT COUNT(*) FROM sbtest.sbtest3"
+----------+
| COUNT(*) |
+----------+
|    99995 |
+----------+

mysql --defaults-group-suffix=_replica1 -e "SELECT id FROM sbtest.sbtest3 WHERE id BETWEEN 49995 AND 50010 ORDER BY id"
+-------+
| id    |
+-------+
| 49995 |
| 49996 |
| 49997 |
| 49998 |
| 49999 |
| 50005 |
| 50006 |
| 50007 |
| 50008 |
| 50009 |
| 50010 |
+-------+
```

## Result
```bash
time="2025-06-06T11:39:39-07:00" level=info msg="Staring go-data-checksum dev..."
time="2025-06-06T11:39:39-07:00" level=info msg="24 pairs of source and target tables:"
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest1 => sbtest.sbtest1 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest10 => sbtest.sbtest10 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest11 => sbtest.sbtest11 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest12 => sbtest.sbtest12 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest13 => sbtest.sbtest13 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest14 => sbtest.sbtest14 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest15 => sbtest.sbtest15 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest16 => sbtest.sbtest16 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest17 => sbtest.sbtest17 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest18 => sbtest.sbtest18 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest19 => sbtest.sbtest19 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest2 => sbtest.sbtest2 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest20 => sbtest.sbtest20 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest21 => sbtest.sbtest21 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest22 => sbtest.sbtest22 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest23 => sbtest.sbtest23 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest24 => sbtest.sbtest24 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest3 => sbtest.sbtest3 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest4 => sbtest.sbtest4 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest5 => sbtest.sbtest5 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest6 => sbtest.sbtest6 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest7 => sbtest.sbtest7 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest8 => sbtest.sbtest8 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Table map: sbtest.sbtest9 => sbtest.sbtest9 ."
time="2025-06-06T11:39:39-07:00" level=info msg="Starting check table pair: sbtest.sbtest1 => sbtest.sbtest1 ."
time="2025-06-06T11:39:40-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest1 => sbtest.sbtest1 , tableCheckDuration=558.509111ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:40-07:00" level=info msg="End check table pair: sbtest.sbtest1 => sbtest.sbtest1 ."
time="2025-06-06T11:39:40-07:00" level=info msg="Starting check table pair: sbtest.sbtest10 => sbtest.sbtest10 ."
time="2025-06-06T11:39:41-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest10 => sbtest.sbtest10 , tableCheckDuration=1.072464832s, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:41-07:00" level=info msg="End check table pair: sbtest.sbtest10 => sbtest.sbtest10 ."
time="2025-06-06T11:39:41-07:00" level=info msg="Starting check table pair: sbtest.sbtest11 => sbtest.sbtest11 ."
time="2025-06-06T11:39:41-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest11 => sbtest.sbtest11 , tableCheckDuration=257.890423ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:41-07:00" level=info msg="End check table pair: sbtest.sbtest11 => sbtest.sbtest11 ."
time="2025-06-06T11:39:41-07:00" level=info msg="Starting check table pair: sbtest.sbtest12 => sbtest.sbtest12 ."
time="2025-06-06T11:39:41-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest12 => sbtest.sbtest12 , tableCheckDuration=253.987565ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:41-07:00" level=info msg="End check table pair: sbtest.sbtest12 => sbtest.sbtest12 ."
time="2025-06-06T11:39:41-07:00" level=info msg="Starting check table pair: sbtest.sbtest13 => sbtest.sbtest13 ."
time="2025-06-06T11:39:41-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest13 => sbtest.sbtest13 , tableCheckDuration=249.332772ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:41-07:00" level=info msg="End check table pair: sbtest.sbtest13 => sbtest.sbtest13 ."
time="2025-06-06T11:39:41-07:00" level=info msg="Starting check table pair: sbtest.sbtest14 => sbtest.sbtest14 ."
time="2025-06-06T11:39:42-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest14 => sbtest.sbtest14 , tableCheckDuration=275.567289ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:42-07:00" level=info msg="End check table pair: sbtest.sbtest14 => sbtest.sbtest14 ."
time="2025-06-06T11:39:42-07:00" level=info msg="Starting check table pair: sbtest.sbtest15 => sbtest.sbtest15 ."
time="2025-06-06T11:39:42-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest15 => sbtest.sbtest15 , tableCheckDuration=259.829183ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:42-07:00" level=info msg="End check table pair: sbtest.sbtest15 => sbtest.sbtest15 ."
time="2025-06-06T11:39:42-07:00" level=info msg="Starting check table pair: sbtest.sbtest16 => sbtest.sbtest16 ."
time="2025-06-06T11:39:42-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest16 => sbtest.sbtest16 , tableCheckDuration=251.448752ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:42-07:00" level=info msg="End check table pair: sbtest.sbtest16 => sbtest.sbtest16 ."
time="2025-06-06T11:39:42-07:00" level=info msg="Starting check table pair: sbtest.sbtest17 => sbtest.sbtest17 ."
time="2025-06-06T11:39:42-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest17 => sbtest.sbtest17 , tableCheckDuration=252.834589ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:42-07:00" level=info msg="End check table pair: sbtest.sbtest17 => sbtest.sbtest17 ."
time="2025-06-06T11:39:42-07:00" level=info msg="Starting check table pair: sbtest.sbtest18 => sbtest.sbtest18 ."
time="2025-06-06T11:39:43-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest18 => sbtest.sbtest18 , tableCheckDuration=254.886938ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:43-07:00" level=info msg="End check table pair: sbtest.sbtest18 => sbtest.sbtest18 ."
time="2025-06-06T11:39:43-07:00" level=info msg="Starting check table pair: sbtest.sbtest19 => sbtest.sbtest19 ."
time="2025-06-06T11:39:43-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest19 => sbtest.sbtest19 , tableCheckDuration=255.367792ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:43-07:00" level=info msg="End check table pair: sbtest.sbtest19 => sbtest.sbtest19 ."
time="2025-06-06T11:39:43-07:00" level=info msg="Starting check table pair: sbtest.sbtest2 => sbtest.sbtest2 ."
time="2025-06-06T11:39:43-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest2 => sbtest.sbtest2 , tableCheckDuration=254.412822ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:43-07:00" level=info msg="End check table pair: sbtest.sbtest2 => sbtest.sbtest2 ."
time="2025-06-06T11:39:43-07:00" level=info msg="Starting check table pair: sbtest.sbtest20 => sbtest.sbtest20 ."
time="2025-06-06T11:39:43-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest20 => sbtest.sbtest20 , tableCheckDuration=251.905263ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:43-07:00" level=info msg="End check table pair: sbtest.sbtest20 => sbtest.sbtest20 ."
time="2025-06-06T11:39:43-07:00" level=info msg="Starting check table pair: sbtest.sbtest21 => sbtest.sbtest21 ."
time="2025-06-06T11:39:44-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest21 => sbtest.sbtest21 , tableCheckDuration=294.660871ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:44-07:00" level=info msg="End check table pair: sbtest.sbtest21 => sbtest.sbtest21 ."
time="2025-06-06T11:39:44-07:00" level=info msg="Starting check table pair: sbtest.sbtest22 => sbtest.sbtest22 ."
time="2025-06-06T11:39:44-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest22 => sbtest.sbtest22 , tableCheckDuration=254.852414ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:44-07:00" level=info msg="End check table pair: sbtest.sbtest22 => sbtest.sbtest22 ."
time="2025-06-06T11:39:44-07:00" level=info msg="Starting check table pair: sbtest.sbtest23 => sbtest.sbtest23 ."
time="2025-06-06T11:39:44-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest23 => sbtest.sbtest23 , tableCheckDuration=255.28893ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:44-07:00" level=info msg="End check table pair: sbtest.sbtest23 => sbtest.sbtest23 ."
time="2025-06-06T11:39:44-07:00" level=info msg="Starting check table pair: sbtest.sbtest24 => sbtest.sbtest24 ."
time="2025-06-06T11:39:44-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest24 => sbtest.sbtest24 , tableCheckDuration=250.826814ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:44-07:00" level=info msg="End check table pair: sbtest.sbtest24 => sbtest.sbtest24 ."
time="2025-06-06T11:39:44-07:00" level=info msg="Starting check table pair: sbtest.sbtest3 => sbtest.sbtest3 ."
time="2025-06-06T11:39:47-07:00" level=error msg="Critical: record CRC32 checksum value is not equal of table pair: sbtest.sbtest3 => sbtest.sbtest3 , tableCheckDuration=2.133377276s"
time="2025-06-06T11:39:47-07:00" level=info msg="Running differential analysis for table pair: sbtest.sbtest3 => sbtest.sbtest3"
time="2025-06-06T11:39:47-07:00" level=info msg="Starting differential analysis for table pair: sbtest.sbtest3 => sbtest.sbtest3"
time="2025-06-06T11:39:47-07:00" level=info msg="=== DIFFERENTIAL ANALYSIS RESULTS ==="
time="2025-06-06T11:39:47-07:00" level=info msg="Table Pair: sbtest.sbtest3 => sbtest.sbtest3"
time="2025-06-06T11:39:47-07:00" level=error msg="- 5 records exist only in SOURCE"
time="2025-06-06T11:39:47-07:00" level=info msg="= 50045 records are identical"
time="2025-06-06T11:39:47-07:00" level=info msg="=== SAMPLE DIFFERENCES ==="
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=[53 48 48 48 51]) exists only in source"
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=[53 48 48 48 48]) exists only in source"
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=[53 48 48 48 49]) exists only in source"
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=[53 48 48 48 50]) exists only in source"
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=[53 48 48 48 52]) exists only in source"
time="2025-06-06T11:39:47-07:00" level=info msg="=== END DIFFERENTIAL ANALYSIS ==="
time="2025-06-06T11:39:47-07:00" level=info msg="Starting check table pair: sbtest.sbtest4 => sbtest.sbtest4 ."
time="2025-06-06T11:39:47-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest4 => sbtest.sbtest4 , tableCheckDuration=256.227545ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:47-07:00" level=info msg="End check table pair: sbtest.sbtest4 => sbtest.sbtest4 ."
time="2025-06-06T11:39:47-07:00" level=info msg="Starting check table pair: sbtest.sbtest5 => sbtest.sbtest5 ."
time="2025-06-06T11:39:47-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest5 => sbtest.sbtest5 , tableCheckDuration=254.200657ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:47-07:00" level=info msg="End check table pair: sbtest.sbtest5 => sbtest.sbtest5 ."
time="2025-06-06T11:39:47-07:00" level=info msg="Starting check table pair: sbtest.sbtest6 => sbtest.sbtest6 ."
time="2025-06-06T11:39:48-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest6 => sbtest.sbtest6 , tableCheckDuration=263.125136ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:48-07:00" level=info msg="End check table pair: sbtest.sbtest6 => sbtest.sbtest6 ."
time="2025-06-06T11:39:48-07:00" level=info msg="Starting check table pair: sbtest.sbtest7 => sbtest.sbtest7 ."
time="2025-06-06T11:39:48-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest7 => sbtest.sbtest7 , tableCheckDuration=248.430711ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:48-07:00" level=info msg="End check table pair: sbtest.sbtest7 => sbtest.sbtest7 ."
time="2025-06-06T11:39:48-07:00" level=info msg="Starting check table pair: sbtest.sbtest8 => sbtest.sbtest8 ."
time="2025-06-06T11:39:48-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest8 => sbtest.sbtest8 , tableCheckDuration=251.678157ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:48-07:00" level=info msg="End check table pair: sbtest.sbtest8 => sbtest.sbtest8 ."
time="2025-06-06T11:39:48-07:00" level=info msg="Starting check table pair: sbtest.sbtest9 => sbtest.sbtest9 ."
time="2025-06-06T11:39:49-07:00" level=info msg="Info: record CRC32 checksum value is equal of table pair: sbtest.sbtest9 => sbtest.sbtest9 , tableCheckDuration=253.057682ms, tableCheckSpeed= 100000 rows/second."
time="2025-06-06T11:39:49-07:00" level=info msg="End check table pair: sbtest.sbtest9 => sbtest.sbtest9 ."
time="2025-06-06T11:39:49-07:00" level=error msg="Table records check result 23 equal, 1 not equal."
time="2025-06-06T11:39:49-07:00" level=info msg="Finished go-data-checksum. TotalDuration=9.566251379s"
```

## DETAILED FUNCTIONALITY EXPLANATION

### Core Verification Technology
go-data-checksum is a high-performance MySQL database/table data verification tool that provides comprehensive data integrity checking capabilities between MySQL instances. The tool operates by calculating and comparing CRC32 values of data being verified, ensuring accurate and efficient data comparison.

### Key Capabilities

#### 1. Comprehensive Data Verification Methods
- **Primary Key-based Full Data Verification (按照主键的全量数据核对)**: Complete table comparison using primary keys for chunking and ordering
- **Time Field-based Incremental Verification (按照时间字段的增量数据核对)**: Incremental data checking based on specified timestamp columns for efficient delta comparisons
- **Full Field Verification (全字段核对)**: Compare all columns in the table for complete data integrity
- **Selective Field Verification (指定字段核对)**: Compare only specified columns for targeted verification

#### 2. Advanced Cross-Instance Support
- **Multi-Table Parallel Verification (跨MySQL实例的多表并行核对)**: Supports parallel verification of multiple tables across different MySQL instances for improved performance
- **Superset Data Scenarios (目标表的数据是源表的超集的场景)**: Handles scenarios where target table data is a superset of source table data, allowing for flexible replication validation

#### 3. Technical Implementation
- **CRC32 Checksum Algorithm (计算并比较待核对数据的CRC32值)**: Uses CRC32 checksums to calculate and compare data integrity values, providing fast and reliable data comparison
- **Chunk-based Processing**: Processes data in configurable chunks to handle large tables efficiently
- **Automatic Primary Key Detection**: Intelligently selects the best unique key for data chunking and comparison
- **Retry Mechanisms**: Built-in retry logic for handling transient network or database issues

#### 4. Performance Optimization
- **Parallel Processing**: Multi-threaded execution for concurrent table comparisons
- **Memory Efficient**: Processes data in chunks to minimize memory usage
- **Connection Pooling**: Efficient database connection management
- **Optimized Query Execution**: Smart query building for maximum performance

This tool is designed for database administrators, DevOps engineers, and data engineers who need to ensure data consistency across MySQL environments, validate replication accuracy, and maintain data integrity in distributed database systems.
