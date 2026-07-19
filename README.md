# go-data-checksum

## DESCRIPTION
go-data-checksum is a high-performance data check tool to verify data integrity between MySQL databases/tables. go-data-checksum supports full data check via primary key and incremental data check via specified time field; supports full field check or specified field check also.


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
make build     # builds bin/go-data-checksum and bin/go-data-sync
make test      # runs the unit tests
```

Or build the binaries directly:
```
go build -o bin/go-data-checksum cmd/checksum/main.go
go build -o bin/go-data-sync cmd/sync/main.go
```

## USAGE
```
# Usage help
./bin/go-data-checksum --help

  -check-column-names string
        Column names to check,eg: col1,col2,col3. By default, all columns are used.
  -chunk-size int
        amount of rows to handle in each iteration (allowed range: 10-100,000) (default 1000)
  -conn-db-timeout int
        connect db timeout (default 60)
  -debug
        debug mode (very verbose)
  -default-retries int
        Default number of retries for various operations before panicking (default 10)
  -enable-differential-reporting
        Enable detailed differential reporting showing which records differ by primary key (default false)
  -enable-tracking
        Persist job/table/chunk results to a tracking database (pt-table-checksum style).
  -generate-sync-sql
        Generate REPLACE INTO statements for synchronizing differences to a file
  -ignore-row-count-check
        Shall we ignore check by counting rows? Default: false
  -is-superset-as-equal
        Shall we think that the records in target table is the superset of the source as equal? By default, we think the records are exactly equal as equal.
  -logfile string
        Log file name.
  -max-display-differences int
        Maximum number of differences to display in output (default: 10) (default 10)
  -max-sample-differences int
        Maximum number of sample differences to collect during analysis (default: 100) (default 100)
  -resume-job-id string
        Resume a previous tracked job by job_id (implies --enable-tracking).
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
  -sync-sql-file string
        Output file for sync SQL statements (default: stdout if not specified)
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
  -tracking-db-host string
        Tracking MySQL hostname (default: target-db-host).
  -tracking-db-name string
        Tracking database name; auto-created if missing. (default "data_checksum_tracking")
  -tracking-db-password string
        Tracking MySQL password (default: target-db-password).
  -tracking-db-port int
        Tracking MySQL port (default: target-db-port).
  -tracking-db-user string
        Tracking MySQL user (default: target-db-user).
  -version
        Print version & exit
```

## EXAMPLES

### 1. Quick start: compare one database between two instances
```bash
# Checks every table in app_db on the source against the same-named tables
# on the target. Fails fast per table on a row-count mismatch, then verifies
# contents chunk by chunk with CRC32.
./bin/go-data-checksum \
  --source-db-host="prod.example.com"    --source-db-port=3306 \
  --source-db-user="checker"             --source-db-password="xxxx" \
  --target-db-host="replica.example.com" --target-db-port=3306 \
  --target-db-user="checker"             --target-db-password="xxxx" \
  --source-db-name="app_db" \
  --threads=4
```
Exit summary: `All N pairs of tables check result is equal.` or
`Table records check result X equal, Y not equal.`

### 2. Verify a replica and see exactly which records drifted
```bash
# --enable-differential-reporting rescans any unequal table and reports
# per-record differences by primary key:
#   -  record exists only on the SOURCE   (missing on the replica)
#   +  record exists only on the TARGET   (extra on the replica)
#   ~  record exists on both but differs  (modified on the replica)
./bin/go-data-checksum \
  --source-db-host="192.168.50.75" --source-db-port=3306 \
  --source-db-user="root"          --source-db-password="xxxx" \
  --target-db-host="192.168.50.75" --target-db-port=3307 \
  --target-db-user="root"          --target-db-password="xxxx" \
  --source-db-name="sbtest" \
  --enable-differential-reporting \
  --max-display-differences=25 \
  --logfile="comparison.log"
```

### 3. Full repair workflow: find, review, sync, re-verify
```bash
# Step 1 — find differences and write REPLACE INTO statements to a file.
# Raise --max-sample-differences to cover every difference; the tool warns
# (in the log and inside the file) if the file is incomplete.
./bin/go-data-checksum \
  --source-db-host="prod.example.com"    --source-db-user="checker" --source-db-password="xxxx" \
  --target-db-host="replica.example.com" --target-db-user="checker" --target-db-password="xxxx" \
  --source-db-name="app_db" \
  --source-table-name="orders" \
  --enable-differential-reporting \
  --generate-sync-sql \
  --sync-sql-file="sync_orders.sql" \
  --max-sample-differences=10000

# Step 2 — review what would be applied (go-data-sync is dry-run by default)
./bin/go-data-sync --sql-file="sync_orders.sql" \
  --target-db-host="replica.example.com" --target-db-user="admin" --target-db-password="xxxx"

# Step 3 — apply in transactional batches
./bin/go-data-sync --sql-file="sync_orders.sql" \
  --target-db-host="replica.example.com" --target-db-user="admin" --target-db-password="xxxx" \
  --execute

# Step 4 — re-verify: the table pair should now report equal
./bin/go-data-checksum \
  --source-db-host="prod.example.com"    --source-db-user="checker" --source-db-password="xxxx" \
  --target-db-host="replica.example.com" --target-db-user="checker" --target-db-password="xxxx" \
  --source-db-name="app_db" --source-table-name="orders" \
  --enable-differential-reporting
```
Note: target-only records (`+`) are *never* included in sync SQL — removing
rows from the target requires a manual, reviewed DELETE.

### 4. Select tables by regular expression
```bash
# Check every table matching sbtest.sbtest* across instances
./bin/go-data-checksum \
  --source-db-host="1.1.1.1" --source-db-port=3307 --source-db-user="test" --source-db-password="xxxx" \
  --target-db-host="8.8.8.8" --target-db-port=3306 --target-db-user="test" --target-db-password="xxxx" \
  --source-table-regexp="sbtest\.sbtest.*" \
  --threads=4
```

### 5. Incremental check on a time column (large tables)
```bash
# Only verify rows whose updated_at falls inside the window, walking it in
# 30-minute steps. Ideal for hourly/daily incremental verification jobs
# where a full-table pass is too expensive.
./bin/go-data-checksum \
  --source-db-host="prod.example.com"    --source-db-user="checker" --source-db-password="xxxx" \
  --target-db-host="replica.example.com" --target-db-user="checker" --target-db-password="xxxx" \
  --source-db-name="app_db" \
  --source-table-name="events" \
  --specified-time-column="updated_at" \
  --specified-time-begin="2026-07-04 00:00:00" \
  --specified-time-end="2026-07-05 00:00:00" \
  --time-range-per-step=30m \
  --enable-differential-reporting
```

### 6. Compare differently-named targets (migration / shadow tables)
```bash
# Source app_db.users vs target app_db_new.users_v2 — explicit mapping
./bin/go-data-checksum \
  ... connection flags ... \
  --source-db-name="app_db"      --source-table-name="users" \
  --target-db-name="app_db_new"  --target-table-name="users_v2"

# Or by suffix: app_db.users vs app_db.users_new for every source table
./bin/go-data-checksum \
  ... connection flags ... \
  --source-db-name="app_db" \
  --target-table-add-suffix="_new"
```

### 7. Check only specific columns / tolerate a superset target
```bash
# Compare only the business columns (skip audit/metadata columns), and
# accept the target having extra rows (e.g. target keeps soft-deleted rows).
# Note: sync SQL still always writes FULL rows, regardless of this flag.
./bin/go-data-checksum \
  ... connection flags ... \
  --source-db-name="app_db" --source-table-name="accounts" \
  --check-column-names="id,email,balance,status" \
  --is-superset-as-equal \
  --ignore-row-count-check
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

Notes:
- The differential analysis always rescans the whole table pair, so the counts
  cover every record — including target-only records whose keys fall outside
  the source table's key range.
- The analysis runs whenever a table pair is found unequal, including when the
  row-count pre-check already shows a mismatch.

### Explanation of Symbols
- **`-` (minus)**: Records that exist in the source database but are missing in the target
- **`+` (plus)**: Records that exist in the target database but are missing in the source  
- **`~` (tilde)**: Records that exist in both databases but have different data (checksum mismatch)
- **`=` (equals)**: Records that are identical in both databases

**Now you can use differential reporting by adding `--enable-differential-reporting` to your existing command!**

## SYNC SQL GENERATION

### Overview
The `--generate-sync-sql` feature generates `REPLACE INTO` statements for synchronizing table differences, inspired by Percona's pt-table-sync tool. This allows you to:

1. Identify differences between source and target tables
2. Generate SQL statements to fix those differences
3. Apply the changes to bring tables into sync

### How It Works

**REPLACE INTO Behavior:**
- If a row with the same unique key doesn't exist in target → **INSERT** the row
- If a row with the same unique key exists in target → **DELETE** old row and **INSERT** new row
- Atomic operation that handles both insert and update scenarios

**What Gets Synchronized:**
- **Source-only records** (`-`): Generated as REPLACE INTO to add missing rows
- **Modified records** (`~`): Generated as REPLACE INTO to update changed data
- **Target-only records** (`+`): NOT generated (safety: requires explicit DELETE)

### Sample Sync SQL Output

```sql
-- Sync SQL for source_db.users => target_db.users
-- Generated at: 2025-06-06 14:30:22
-- Total differences: source_only=5, target_only=0, modified=2

REPLACE INTO `target_db`.`users` (`id`, `name`, `email`, `created_at`) VALUES (123, 'John Doe', 'john@example.com', '2025-01-15 10:00:00');
REPLACE INTO `target_db`.`users` (`id`, `name`, `email`, `created_at`) VALUES (456, 'Jane Smith', 'jane@example.com', '2025-02-20 15:30:00');
REPLACE INTO `target_db`.`users` (`id`, `name`, `email`, `created_at`) VALUES (789, 'Bob Wilson', 'bob@example.com', NULL);

-- Total REPLACE INTO statements generated: 3
```

### Safety Features

1. **SQL Escaping**: Single quotes, backslashes, and control characters are
   properly escaped (`'` → `''`, `\` → `\\`, newlines → `\n`)
2. **NULL Handling**: NULL values are written as `NULL` (not quoted)
3. **Datetime Handling**: DATETIME/TIMESTAMP values are written as proper MySQL
   literals (`'2026-01-05 14:00:00'`)
4. **Full-Row Statements**: sync SQL always covers *every* column of the table,
   even when `--check-column-names` restricted the checksum to a subset —
   REPLACE INTO deletes and re-inserts the row, so partial statements would
   silently reset unlisted columns
5. **Atomic Operations**: REPLACE INTO is atomic within InnoDB
6. **Sample Limits**: Use `--max-sample-differences` to control statement
   count; if the file does not cover every difference found, a warning is
   logged and written into the file
7. **Fresh Output**: the sync SQL file is truncated at the start of each run so
   it never contains stale statements from a previous run

### Best Practices

1. **Review Before Applying**: Always review generated SQL before execution —
   `go-data-sync` runs in dry-run mode by default for exactly this reason
2. **Test on Staging**: Test sync SQL on non-production environments first
3. **Backup Target**: Take a backup of target database before applying changes
4. **Handle Target-Only**: Manually review and handle target-only records —
   they are never included in the generated SQL
5. **Apply with go-data-sync**: it validates that the file contains only
   REPLACE INTO statements and applies them in transactional batches
   (see "COMPANION CLI: go-data-sync" below and EXAMPLES #3 for the full
   find → review → sync → re-verify workflow)


## RESULT TRACKING (pt-table-checksum style)

### Overview

With `--enable-tracking`, every run persists its results to a MySQL tracking
database — the same idea as pt-table-checksum's `percona.checksums` table, but
without the replication-replay mechanism (this tool always connects to both
servers directly). You get:

- **History**: "when did these servers last verify clean?" is a SQL query
- **Per-chunk detail**: which key ranges differed, with both checksums
- **Per-record diffs**: with `--enable-differential-reporting`, each sampled
  differing record's primary key and diff type is stored permanently
- **Resume**: a killed run can be resumed by job id, re-checking only the
  tables that never finished

Tracking is fully opt-in and fail-safe: without the flag no tracking
connection is ever opened, and if a tracking write fails mid-run the checksum
continues and only logs a warning.

### Where results are stored

By default the tracking database `data_checksum_tracking` is auto-created on
the **target** server (writing to a production source is deliberately
avoided). Use the `--tracking-db-*` flags to store it anywhere else, e.g. a
dedicated admin instance. The user needs `CREATE` on the first run only;
afterwards `INSERT`/`UPDATE`/`SELECT` on the tracking database suffices.
A reference copy of the DDL is in `schema/tracking_schema.sql` for manual
installs or permission-restricted environments.

### Schema

| Table | One row per | Notable columns |
|---|---|---|
| `checksum_jobs` | run (job) | `job_id`, source/target host, status, table tallies |
| `table_comparisons` | table pair | status, row counts, chunk tallies, error message |
| `chunk_comparisons` | chunk checked | key range (JSON), both checksums, status, duration |
| `difference_details` | sampled differing record | primary key (JSON), diff type, both checksums |

Status mapping: a table or chunk is `equal`, `different`, or `error` (an error
takes precedence over the comparison result). A job flips from `running` to
`completed` when the run finishes; a job that stays `running` was interrupted
and can be resumed.

Note: like the normal check, the chunk loop stops at the **first unequal
chunk** of a table (unlike pt-table-checksum, which always scans all chunks).
Chunk rows therefore cover chunks up to and including the first difference.
In time-column mode row counts are not collected (stored as NULL).

### Example: tracked run

```bash
./bin/go-data-checksum \
  --source-db-host="127.0.0.1" --source-db-port=3306 \
  --source-db-user="checker"   --source-db-password="xxxx" \
  --target-db-host="127.0.0.1" --target-db-port=3307 \
  --target-db-user="checker"   --target-db-password="xxxx" \
  --source-db-name="app_db" \
  --enable-tracking --enable-differential-reporting
# log line to note:
#   Tracking enabled, job_id=127.0.0.1:3306_127.0.0.1:3307_1752940000_a1b2c3d4 ...
```

Then query the results (on the target, 3307 in this example):

```sql
-- Recent jobs
SELECT job_id, status, total_tables, tables_equal, tables_different
  FROM data_checksum_tracking.checksum_jobs ORDER BY start_time DESC LIMIT 10;

-- Which tables of a job were not clean?
SELECT source_database, source_table, status, source_row_count, target_row_count
  FROM data_checksum_tracking.table_comparisons
 WHERE job_id = '<job_id>' AND status <> 'equal';

-- Exactly which records drifted (needs --enable-differential-reporting)?
SELECT d.difference_type, d.primary_key_values
  FROM data_checksum_tracking.difference_details d
  JOIN data_checksum_tracking.chunk_comparisons  c USING (chunk_id)
  JOIN data_checksum_tracking.table_comparisons  t USING (comparison_id)
 WHERE t.job_id = '<job_id>';
```

### Resuming an interrupted job

If a run dies (Ctrl-C, network, crash), its job stays `running` and the
unfinished tables stay `pending`/`running`. Re-run with the same connection
flags plus the job id:

```bash
./bin/go-data-checksum \
  --source-db-host="127.0.0.1" --source-db-port=3306 ... \
  --target-db-host="127.0.0.1" --target-db-port=3307 ... \
  --resume-job-id="<job_id>"
```

Only the tables that never completed are re-checked (each from its beginning —
resume is per table, not per chunk), their existing rows are updated in place,
and the job is finalized from the aggregated table results.


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

## COMPANION CLI: go-data-sync

`go-data-sync` (built alongside the checksum binary by `make build`) applies a
sync SQL file generated with `--generate-sync-sql` to the target instance.

```
  -sql-file string
        Sync SQL file generated by go-data-checksum --generate-sync-sql (required)
  -target-db-host string
        Target MySQL hostname (default "127.0.0.1")
  -target-db-port int
        Target MySQL port (default 3306)
  -target-db-user string
        Target MySQL user
  -target-db-password string
        Target MySQL password
  -execute
        Actually apply the statements. Without this flag the tool runs in dry-run mode.
  -batch-size int
        Number of statements per transaction (default 100)
  -conn-db-timeout int
        connect db timeout in seconds (default 60)
  -version
        Print version & exit
```

Safety model:

- **Dry-run by default** — shows a per-table statement summary and a preview;
  nothing is executed until you pass `--execute`.
- **REPLACE INTO only** — any other statement in the file aborts the run before
  anything is executed.
- **Transactional batches** — a failed batch is rolled back and the run stops,
  reporting the exact line number of the failing statement.
- Reports rows *inserted* (missing on target) vs *replaced* (modified on target).

```bash
# 1. Find differences and generate sync SQL
./bin/go-data-checksum ... --enable-differential-reporting \
  --generate-sync-sql --sync-sql-file=sync_orders.sql --max-sample-differences=10000

# 2. Review, then dry-run
./bin/go-data-sync --sql-file=sync_orders.sql \
  --target-db-host=replica.example.com --target-db-user=admin --target-db-password=xxx

# 3. Apply
./bin/go-data-sync --sql-file=sync_orders.sql \
  --target-db-host=replica.example.com --target-db-user=admin --target-db-password=xxx \
  --execute

# 4. Re-verify
./bin/go-data-checksum ... --enable-differential-reporting
```

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
./bin/go-data-checksum \
  --source-db-host="192.168.50.75" \
  --source-db-port=3306 \
  --source-db-user="root" \
  --source-db-password="s3cr3t" \
  --target-db-host="192.168.50.75" \
  --target-db-port=3307 \
  --target-db-user="root" \
  --target-db-password="s3cr3t" \
  --source-db-name="sbtest" \
  --default-retries=3 \
  --enable-differential-reporting \
  --ignore-row-count-check \
  --logfile="comparison.log"

  
time="2025-06-06T11:39:39-07:00" level=info msg="Starting go-data-checksum dev..."
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
time="2025-06-06T11:39:47-07:00" level=info msg="= 99995 records are identical"
time="2025-06-06T11:39:47-07:00" level=info msg="=== SAMPLE DIFFERENCES ==="
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=50000) exists only in source"
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=50001) exists only in source"
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=50002) exists only in source"
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=50003) exists only in source"
time="2025-06-06T11:39:47-07:00" level=error msg="- Record (id=50004) exists only in source"
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
- **Primary Key-based Full Data Verification**: Complete table comparison using primary keys for chunking and ordering
- **Time Field-based Incremental Verification**: Incremental data checking based on specified timestamp columns for efficient delta comparisons
- **Full Field Verification**: Compare all columns in the table for complete data integrity
- **Selective Field Verification**: Compare only specified columns for targeted verification

#### 2. Advanced Cross-Instance Support
- **Multi-Table Parallel Verification**: Supports parallel verification of multiple tables across different MySQL instances for improved performance
- **Superset Data Scenarios**: Handles scenarios where target table data is a superset of source table data, allowing for flexible replication validation

#### 3. Technical Implementation
- **CRC32 Checksum Algorithm**: Uses CRC32 checksums to calculate and compare data integrity values, providing fast and reliable data comparison
- **Chunk-based Processing**: Processes data in configurable chunks to handle large tables efficiently
- **Automatic Primary Key Detection**: Intelligently selects the best unique key for data chunking and comparison
- **Retry Mechanisms**: Built-in retry logic for handling transient network or database issues

#### 4. Performance Optimization
- **Parallel Processing**: Multi-threaded execution for concurrent table comparisons
- **Memory Efficient**: Processes data in chunks to minimize memory usage
- **Connection Pooling**: Efficient database connection management
- **Optimized Query Execution**: Smart query building for maximum performance

This tool is designed for database administrators, DevOps engineers, and data engineers who need to ensure data consistency across MySQL environments, validate replication accuracy, and maintain data integrity in distributed database systems.
