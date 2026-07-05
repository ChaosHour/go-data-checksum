# Production-Readiness Issue Tracker

Branch: `production-ready-fixes`
Goal: reliably find records that are out of sync between source and target MySQL
instances, show the user record counts and the actual out-of-sync records, and
provide a safe way to sync them (companion CLI in this repo).

Status legend: `[ ]` open · `[x]` fixed · `[~]` documented / deferred

---

## Critical logic bugs

- [x] **L1. Time-column check mode is a stub that always reports "equal"**
  `pkg/checksum/timecolumn.go` — `IterationTimeRangeQueryChecksum()` returns
  `true` unconditionally and `EstimateTableRowsViaExplain()` returns a hardcoded
  `1000`. Anyone running with `--specified-time-column` gets a false "data is
  equal" result with no query ever executed. **Fix:** implement real time-range
  chunked CRC32 checksum queries against both source and target, and a real row
  estimate via `EXPLAIN`.

- [x] **L2. Differential analysis starts mid-table, missing differences**
  `pkg/checksum/differ.go` — `AnalyzeAndReportDifferences()` reuses the
  `ChecksumContext` left over from the checksum loop. `ChecksumIterationRangeMinValues`
  still points at the failed chunk's end and `Iteration > 0`, so the differ
  resumes *after* the first mismatched chunk: differences inside that chunk and
  everything before it are never analyzed, and the "records are identical" count
  only covers the tail of the table. **Fix:** reset iteration state (iteration
  counter + range values) before the analysis loop so the whole table is scanned.

- [x] **L3. Target-only rows outside the source key range are invisible**
  Chunk boundaries are driven from the *source* table's unique-key min/max. Rows
  that exist only in the target with keys below the source min or above the
  source max are never fetched, so they are never reported as `+` target-only.
  An empty source table reports nothing at all. **Fix:** after the main chunk
  loop, scan the target for rows outside the source key range (and handle the
  empty-source case).

- [x] **L4. Nil-pointer crash when differential reporting runs in time-column mode**
  `cmd/checksum/main.go` — `ChecksumPerTableViaTimeColumn()` never calls
  `GetUniqueKeys()`, but `AnalyzeAndReportDifferences()` dereferences
  `ctx.UniqueKey`. As soon as L1 is fixed and a real mismatch is found, this
  panics. **Fix:** resolve the unique key before running the differ.

- [x] **L5. Sync SQL can destroy data when `--check-column-names` is a subset**
  `pkg/checksum/differ.go` — `fetchFullRowData()` / `buildReplaceIntoStatement()`
  use `CheckColumns`. `REPLACE INTO` deletes the existing row and inserts the new
  one, so any column not listed is reset to its default → silent data loss on the
  target. **Fix:** always fetch and write the *full* column list for sync SQL,
  regardless of which columns were checked.

- [x] **L6. Sync SQL emits invalid literals for DATETIME/TIMESTAMP columns**
  The DSN uses `parseTime=true`, so datetime columns scan as `time.Time`;
  `formatValueForSQL()` falls through to `%v` producing
  `'2025-01-15 10:00:00 +0000 UTC'`, which MySQL rejects (or truncates).
  **Fix:** format `time.Time` as `'2006-01-02 15:04:05'`.

- [x] **L7. Backslashes not escaped in generated SQL string literals**
  Only `'` is escaped. A value containing `\` (Windows paths, JSON, etc.)
  corrupts the generated statement under default `sql_mode`. **Fix:** escape
  `\` and `'`.

- [x] **L8. Result-channel bookkeeping is wrong → wrong summary, possible hang**
  `cmd/checksum/main.go` — the count-check error path pushes to `ChecksumErrChan`
  and then *falls through* and pushes again; the `GetCheckColumns` error path
  pushes err+result but does not `return`, so the function keeps running and
  pushes a second (or third) result for the same table. The collector reads
  exactly one result+error per table, so counts shift between tables and extra
  sends can fill the buffer and deadlock `wg.Wait()`. **Fix:** each table sends
  exactly one (result, error) pair — moved the sends into the spawning goroutine
  and made the worker functions return `(bool, error)`.

- [x] **L9. Connection charset hardcoded to `latin1`**
  `pkg/types/types.go` `GetDBUri()` — checksums are computed server-side with
  `hex()` so they survive, but `fetchFullRowData()` reads real string data over
  a latin1 connection: any non-latin1 text is mangled before it is written into
  sync SQL. **Fix:** use `charset=utf8mb4`.

- [x] **L10. `ColumnList.Equals()` compares method values, not columns**
  `pkg/types/types.go` — `reflect.DeepEqual(cl.Columns, other.Columns)` compares
  the *method* `Columns`, which is always false/meaningless. **Fix:** compare
  `cl.columns` / `other.columns`.

- [x] **L11. Fatal-error flow races and continues running after failure**
  `cmd/checksum/main.go` — errors are pushed to `PanicAbort` (handled by a
  goroutine calling `log.Fatalf`) but the calling code keeps executing —
  e.g. after `InitDB()` fails the program still enters the checksum run with nil
  DB handles until the listener goroutine wins the race. **Fix:** fail fast at
  the call sites in `main()`; `GenerateTableList` errors return before any tables
  are processed.

## Display / output bugs

- [x] **D1. Primary keys and checksums printed as raw byte arrays**
  `pkg/checksum/differ.go` `getChunkRecords()` — `fmt.Sprintf("%v", …)` on the
  `[]byte` values the MySQL driver returns produces output like
  `Record (id=[53 48 48 48 51])` and `source_checksum=[97 98 99 …]` (visible in
  the README's own sample log). **Fix:** convert `[]byte` to string when building
  the PK map key and the checksum string.

- [x] **D2. "Staring go-data-checksum" typo** in the startup banner.

- [x] **D3. Confusing summary when no tables matched**
  "Table records check result 0 equal, 0 not equal." is printed as an *error*
  when the table list is empty (bad regexp, mismatched source/target lists).
  **Fix:** explicit error message when no table pairs are found; also error out
  when source/target table list lengths don't match instead of silently
  producing an empty pair map.

- [x] **D4. Sample differences can exceed `--max-sample-differences`**
  Whole chunk sample slices are appended once the total is below the limit, so
  the collection can overshoot. **Fix:** trim to the limit after appending.

- [x] **D5. Sync SQL silently incomplete**
  Statements are only generated for *sampled* differences; if the table has more
  differences than `--max-sample-differences` the file is partial with no
  warning. **Fix:** compare generated count vs. total syncable differences and
  log a prominent warning with the flag to raise.

- [x] **D6. Sync SQL file grows across reruns**
  Opened with `O_APPEND`, so re-running the tool appends stale statements to the
  old file. **Fix:** truncate the file once at program start; per-table writes
  within one run still append.

- [x] **D7. Documentation drift in README**
  - `-default-retries` README says default 10, flag default was 5 (aligned to 10).
  - `-conn-db-timeout` README says 30, code is 60.
  - A whole "TRACKING FUNCTIONALITY" section documents flags
    (`-enable-tracking`, `-tracking-db-*`, …) that do not exist in the CLI.
  - Sample differential output shows the byte-array display bug as expected
    output.
  **Fix:** README rewritten to match reality, incl. companion sync CLI docs.

## Deferred / known limitations (documented, not fixed here)

- [~] **K1. Stub/unwired packages**: `pkg/tracking`, `pkg/monitoring`,
  `pkg/resume`, `pkg/timerange`, `pkg/logic` (compatibility shims) compile but
  are not reachable from the CLI. Left in place for future work; README no
  longer advertises them.
- [~] **K2. Mismatch retry cost**: a genuinely different chunk is retried
  `--default-retries` times with 1s sleeps before being reported (intended to
  ride out replication lag). Consider a dedicated `--recheck-interval` later.
- [~] **K3. `tableCheckSpeed` is an estimate** (`iterations × chunk-size`), the
  last partial chunk inflates it slightly.
- [~] **K4. Table names containing a literal dot** in db or table name break the
  `strings.Split(name, ".")` pairing logic.
- [~] **K5. Time-column mode + `--is-superset-as-equal`**: per-row checksum
  ordering within a time chunk is by the time column only; rows with identical
  timestamps may compare in different orders across instances (aggregate mode
  is order-independent and unaffected).

## New functionality

- [x] **N1. Companion sync CLI** — `cmd/sync` (`go-data-sync` binary): applies a
  reviewed sync SQL file to the target with `--dry-run` (default) / `--execute`,
  transactional batches, per-statement error reporting, and a rows-affected
  summary. Built via `make build` alongside the checksum binary.
