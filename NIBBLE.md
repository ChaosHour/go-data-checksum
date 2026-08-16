# go-data-nibble: how it works, and how to test it

This is a deep dive on `go-data-nibble` — the internals, the exact
semantics of what it reports, and a step-by-step guide to testing and
verifying it before trusting it against a real table. For flags and quick
usage examples, see `README.md`'s "COMPANION CLI: go-data-nibble" section;
this document explains the *why* behind them.

## 1. Why it exists

`go-data-nibble` was built for one specific situation: a replica fell behind
or was down from a known point in time, only a handful of tables actually
drifted, and at least one of those tables is large enough (hundreds of GB to
low TB) that a full-table checksum/rebuild is impractical, but a full
primary-key differential pass is *also* too slow.

The reason a plain PK-based differential pass is too slow here: rows that
changed during the drift window are scattered across the *entire* primary
key range, not clustered at the end of it. Walking chunk boundaries by
`LIMIT 1 OFFSET N` (the way `go-data-checksum`'s default differential path
works) still touches most of the table per chunk, no matter how tightly you
scope the comparison — the offset walk itself doesn't know anything about
time. On a huge table this is expensive; on a MyISAM table it's worse,
because MyISAM takes table-level locks for large scans, which stalls the
single-threaded replication SQL thread (the operational trigger for building
this tool in the first place — see the "MyISAM tables are not fun" commit).

The fix that makes `go-data-nibble` viable: nibble by the time column
*directly*. Every window is a plain indexed range scan on
`--specified-time-column`, independent of table size. This is the same
approach `go-data-checksum --differential-use-time-range` uses for the
primary checksum loop — `go-data-nibble` reuses that exact machinery from
`pkg/checksum`, plus a convergence loop that keeps applying and re-checking
until nothing is left.

## 2. How it works internally

### 2.1 The collect step: `TableDiffer.CollectDifferences()`

(`pkg/checksum/differ.go:79`)

When `ctx.DifferentialUseTimeRange` is set (always true for `go-data-nibble`
— see `nibbleTable()` in `cmd/nibble/main.go`), `CollectDifferences()` calls
`analyzeDifferencesByTimeRange()`, which loops
`ctx.CalculateNextIterationTimeRange()` (`pkg/checksum/timecolumn.go`) —
the same window-walking function the primary checksum loop uses — advancing
through `[--specified-time-begin, --specified-time-end)` in
`--time-range-per-step`-sized windows.

For each window, `analyzeTimeChunkDifferences()`
(`pkg/checksum/differ.go:179`) builds a `WHERE` clause directly on the time
column:

```sql
WHERE last_updated >= ? AND last_updated < ?   -- most windows
WHERE last_updated >= ? AND last_updated <= ?  -- final window only (isFinalTimeChunk)
```

and runs it **identically against both source and target** (`getChunkRecords`,
same `whereClause`/`args` on each side). No primary-key bounds are involved
at all — this is the whole reason it's cheap on a huge table.

### 2.2 Classifying differences: `compareRecordSets()`

(`pkg/checksum/differ.go:434`)

Both result sets are keyed by primary key. For each source row:

- not present in the target result set → **`source_only`**
- present, but checksum differs → **`modified`**
- present, checksum matches → **identical**

Then, for each target row not matched above → **`target_only`**.

**Important nuance, confirmed by load testing:** because the target query
uses the *same* time-window `WHERE` clause as the source query, a target row
whose own `last_updated` falls **outside** the window is invisible to that
window's target query — it simply isn't in `targetRecords`. That makes it
look like `source_only` (missing from target) rather than `modified` (present
but different), even if the row does exist on the target with stale data.

Concretely: during our 2M-row / 55k-row-drift load test, 50,000 rows were
updated on the primary only (existing rows, new `last_updated`) and 5,000
were brand-new inserts. The tool reported all 55,000 as `source_only`, 0 as
`modified` — because every target-side copy of those 50,000 updated rows
still carried its *old* `last_updated`, outside the window being scanned.

**This has no effect on correctness.** `BuildReplaceIntoStatements` (below)
treats `source_only` and `modified` identically — both get synced. It only
affects the *label* you see in the log line, not what gets fixed. Don't be
surprised if real drifted tables show mostly `source_only` even for rows
that technically already exist on the target.

### 2.3 Building the fix: `BuildReplaceIntoStatements()`

(`pkg/checksum/differ.go:641`)

Takes the report's sample differences, drops anything typed `target_only`
(never synced — see §4), fetches full row data for the remaining primary
keys in batches of 1000, and builds one `REPLACE INTO` per row. A row whose
full-data fetch fails is logged as a warning and skipped, not fatal to the
whole batch.

### 2.4 Applying: `applyStatements()`

(`cmd/nibble/main.go:184`)

Straightforward: batches of `--apply-batch-size` statements, each batch in
its own transaction (`Begin`/`Exec`.../`Commit`). Unlike `go-data-sync`,
there's **no retry loop** — a nibble run is short and interactive, so a
failure surfaces immediately with how many statements were applied before it
hit the error, rather than being silently retried.

### 2.5 The convergence loop: `nibbleTable()`

(`cmd/nibble/main.go:217`)

```bash
for iteration := 1; ; iteration++ {
    report := differ.CollectDifferences()          // full rescan of the window
    if report.SourceOnlyRecords + report.ModifiedRecords == 0 {
        converged; return
    }
    statements := differ.BuildReplaceIntoStatements(report.SampleDifferences, ...)
    if !execute { print preview; return }           // dry-run: one pass only
    applyStatements(statements)
    // loop: re-collect, see what's left
}
```

Two things worth understanding here:

- **`--batch-diffs` caps the sample list, which caps what gets synced per
  pass.** `report.SourceOnlyRecords`/`ModifiedRecords` are true totals for
  the window, but `report.SampleDifferences` — the thing actually handed to
  `BuildReplaceIntoStatements` — is capped at `--batch-diffs`
  (`baseContext.MaxSampleDifferences`). If a window has 55,000 differences
  and `--batch-diffs=5000`, only 5,000 get applied per iteration; it takes
  11 iterations to clear the backlog (this is exactly what the load test
  showed). Size `--batch-diffs` generously relative to expected drift, or
  budget for more iterations.
- **Each iteration rescans the whole window from scratch.** There's no
  incremental "only re-check what changed" — already-converged sub-windows
  are cheap re-checks (identical rows just get counted, not re-synced) but
  not free. This is why `--time-range-per-step` and `--batch-diffs` both
  matter for total runtime on a large drift.

### 2.6 Target-only rows are never touched

`report.TargetOnlyRecords > 0` triggers a loud warning
(`nibbleTable()`), but `BuildReplaceIntoStatements` explicitly filters out
`target_only` diffs — there is no code path anywhere in `go-data-nibble` that
issues a `DELETE`. If your workload does hard deletes during the drift
window, this tool alone will not fully converge the tables; those rows need
manual review.

## 3. Session-level target flags

`--skip-binlog`/`--skip-fk-checks`/`--skip-unique-checks`/`--no-auto-value-on-zero`
mirror `go-data-sync`'s flags of the same name (`cmd/nibble/main.go`'s
`initDB()`, appending DSN params exactly like `cmd/sync/main.go` does). All
default `false`, and only ever affect the **target** connection — the source
connection (used only for reads) is never touched.

`--skip-binlog` is the one worth turning on deliberately when repairing a
replica directly, especially if `GTID_MODE=ON` (check with
`SELECT @@GLOBAL.GTID_MODE;`): a `REPLACE INTO` applied without
`sql_log_bin=0` gets binlogged and GTID-tagged on the replica itself. Since
it didn't come through the replication SQL thread, that's an errant
transaction — exactly the kind of thing that causes `Duplicate GTID` errors
later if that replica is ever promoted or re-pointed at a different source.

## 4. How to test and verify

### 4.1 Unit tests

`cmd/nibble/main_test.go` covers `parseTables()` (the `--tables` spec
parser) and `tablePair.String()` — pure functions with no DB dependency.
`applyStatements()`/`nibbleTable()`/`initDB()` need a real MySQL connection
and aren't unit tested, same reasoning as why `go-data-sync`'s apply path
isn't either. Run with:

```bash
go test ./cmd/nibble/... -v
```

### 4.2 End-to-end: `scripts/nibble-loadtest.sh`

This is the real verification path — it exercises the entire tool
(collect → apply → re-collect → converge) against a synthetic large table,
using your **existing** primary/replica containers rather than spinning up
throwaway ones.

**What it proves, each run:**

1. The tool correctly finds injected drift (it asserts this explicitly —
   see §5, "zero-width window" — a run that reports 0 differences when
   drift was just injected is treated as a script failure, not a pass).
2. `--execute` actually converges the table (`REPLACE INTO` applied, verified
   with a follow-up dry run that must show `converged after N iteration(s)`).
3. Real timing at the scale you choose — dry-run pass time and total execute
   time are printed, so you can size `--time-range-per-step`/`--batch-diffs`
   for your actual table before running against it for real.
4. Your real replication is never disrupted — it stops only the replica's
   SQL thread (not IO), scopes all writes to a throwaway `nibbletest` schema
   via a `REPLICATE_IGNORE_DB` filter, and unconditionally restores that
   filter and drops the test schema on exit, success or failure.

**Run it:**

```bash
# Quick smoke test (seconds)
scripts/nibble-loadtest.sh --table-size 50000 --drift-updates 2000 --drift-inserts 200

# Realistic scale -- this exact invocation is what we validated:
# 2M rows, 55k drifted, converged in 12 iterations, ~153s to execute
scripts/nibble-loadtest.sh \
  --table-size 2000000 --drift-updates 50000 --drift-inserts 5000 \
  --time-range-per-step 15m

```bash
Full flag reference: `scripts/nibble-loadtest.sh --help`. See `README.md`'s
"Load-testing go-data-nibble at scale" section for requirements and
troubleshooting specific to the script.

### 4.3 Manual walkthrough (no script, to build intuition)

If you want to see each piece work without the script's automation:

```bash
# 1. Point --specified-time-begin at a window you know has drift, dry-run first
./bin/go-data-nibble \
  --source-db-host=... --target-db-host=... --tables=mydb.mytable \
  --specified-time-column=last_updated \
  --specified-time-begin='2026-07-16 00:00:00' \
  --time-range-per-step=1h
# Read the log line: "N source-only, M target-only, K modified, J identical"

# 2. If N+K > 0, re-run with --execute
./bin/go-data-nibble ... --execute

# 3. Independently verify via raw SQL -- row counts should now match for the window
mysql -h<source> -e "SELECT COUNT(*) FROM mydb.mytable WHERE last_updated BETWEEN '...' AND '...';"
mysql -h<target> -e "SELECT COUNT(*) FROM mydb.mytable WHERE last_updated BETWEEN '...' AND '...';"

# 4. Re-run the dry run from step 1 -- it should now report "converged after 1 iteration(s)"
```bash
Step 3 is the important one: don't just trust the tool's own "converged"
claim — a raw `COUNT(*)`/checksum comparison outside the tool is the
independent check. For a stronger check than row counts, compare a
`CHECKSUM TABLE` or a manual `SUM(CRC32(...))` over the same window on both
sides.

## 5. Known nuances / troubleshooting

- **The time column must be indexed on both sides.** Without an index, every
  `--time-range-per-step` window is a full table scan — worse than the
  PK-offset approach this tool exists to avoid. This is not optional for
  large tables.
- **Host/DB-server timezone mismatch.** If the machine running
  `go-data-nibble` is in a different timezone than the MySQL server(s), and
  you leave `--specified-time-end` unset, its default (`time.Now()` on the
  *host*) can be compared against `--specified-time-begin` values or data
  timestamped by the *server's* clock, producing "illegal time range" errors
  or silently wrong windows. Always pass `--specified-time-end` explicitly,
  sourced from a query against the source DB (`SELECT NOW()`), when the two
  clocks might differ.
- **A zero-width or near-zero window can silently report false
  convergence.** If `--specified-time-begin` and `--specified-time-end` end
  up equal or extremely close, the tool finds nothing to compare and reports
  "converged" even with real drift present. If a dry run against a table you
  *know* has drifted reports 0 differences, verify the window bounds before
  trusting the result — don't assume the table is actually in sync.
- **`GTID_PURGED` errors when cloning a table into an already-replicating
  server via `mysqldump`.** Not a `go-data-nibble` issue directly, but comes
  up constantly when setting up test scenarios: dumping a single
  schema/table from a GTID-enabled server and loading it into a target that
  already has its own `GTID_EXECUTED` fails unless the dump uses
  `--set-gtid-purged=OFF`.
- **`--batch-diffs` sizing.** Too small relative to actual drift means many
  iterations to converge (correct, just slow); too large means each
  iteration's `REPLACE INTO` batch and full-row fetch is bigger. Start
  conservative (as in `nibble_config.example.json`) and raise it once you've
  seen real drift volume from a dry run.

## 6. Reference

- Flags and JSON config quick-reference: `README.md` → "COMPANION CLI:
  go-data-nibble"
- Example configs: `nibble_config.example.json` (dry-run),
  `nibble_execute.example.json` (execute, with the session flags from §3
  turned on)
- Load-test script: `scripts/nibble-loadtest.sh`
- Core implementation: `pkg/checksum/differ.go` (diff logic, shared with
  `go-data-checksum`), `cmd/nibble/main.go` (convergence loop, CLI, target
  session flags)
