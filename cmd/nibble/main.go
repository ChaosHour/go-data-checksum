// go-data-nibble converges a target table onto its source by repeatedly
// running a time-scoped differential analysis (the same nibbling strategy as
// go-data-checksum's --differential-use-time-range) and applying the
// resulting REPLACE INTO statements, until no differences remain in the
// window or --max-iterations is hit.
//
// It exists for recovering a small number of tables after a known drift
// window -- e.g. a replica that was down/lagging from a known start time --
// where a full checksum + rebuild of a very large table is impractical, but
// a full primary-key differential pass is still too slow because the
// changed rows are scattered across the whole key range. See
// pkg/checksum/differ.go's analyzeDifferencesByTimeRange for why PK-offset
// pagination doesn't work for that case.
//
// Safety model mirrors go-data-sync: dry-run by default (one analysis pass
// per table, prints what would be applied, nothing executed); pass --execute
// to actually converge. This tool never deletes rows -- target-only
// differences are reported and logged loudly but never touched, same as
// go-data-checksum's sync SQL generation.
package main

import (
	gosql "database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ChaosHour/go-data-checksum/pkg/checksum"
	"github.com/ChaosHour/go-data-checksum/pkg/types"
)

var AppVersion string

type NibbleJSONConfig struct {
	SourceDBHost        *string `json:"source-db-host,omitempty"`
	SourceDBPort        *int    `json:"source-db-port,omitempty"`
	SourceDBUser        *string `json:"source-db-user,omitempty"`
	SourceDBPassword    *string `json:"source-db-password,omitempty"`
	TargetDBHost        *string `json:"target-db-host,omitempty"`
	TargetDBPort        *int    `json:"target-db-port,omitempty"`
	TargetDBUser        *string `json:"target-db-user,omitempty"`
	TargetDBPassword    *string `json:"target-db-password,omitempty"`
	ConnDBTimeout       *int    `json:"conn-db-timeout,omitempty"`
	Tables              *string `json:"tables,omitempty"`
	CheckColumnNames    *string `json:"check-column-names,omitempty"`
	SpecifiedTimeColumn *string `json:"specified-time-column,omitempty"`
	SpecifiedTimeBegin  *string `json:"specified-time-begin,omitempty"`
	SpecifiedTimeEnd    *string `json:"specified-time-end,omitempty"`
	TimeRangePerStep    *string `json:"time-range-per-step,omitempty"`
	BatchDiffs          *int    `json:"batch-diffs,omitempty"`
	MaxIterations       *int    `json:"max-iterations,omitempty"`
	Execute             *bool   `json:"execute,omitempty"`
	ApplyBatchSize      *int    `json:"apply-batch-size,omitempty"`
	SkipBinlog          *bool   `json:"skip-binlog,omitempty"`
	SkipFK              *bool   `json:"skip-fk-checks,omitempty"`
	SkipUnique          *bool   `json:"skip-unique-checks,omitempty"`
	NoAutoValueOnZero   *bool   `json:"no-auto-value-on-zero,omitempty"`
	Debug               *bool   `json:"debug,omitempty"`
	LogFile             *string `json:"logfile,omitempty"`
}

func loadNibbleConfig(path string, baseContext *types.BaseContext, tables, specifiedTimeBegin, specifiedTimeEnd *string, batchDiffs, maxIterations, applyBatchSize *int, execute, skipBinlog, skipFK, skipUnique, noAutoValueOnZero, debug *bool, logFile *string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	var cfg NibbleJSONConfig
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return err
	}

	seen := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		seen[f.Name] = true
	})

	if !seen["source-db-host"] && cfg.SourceDBHost != nil {
		baseContext.SourceDBHost = *cfg.SourceDBHost
	}
	if !seen["source-db-port"] && cfg.SourceDBPort != nil {
		baseContext.SourceDBPort = *cfg.SourceDBPort
	}
	if !seen["source-db-user"] && cfg.SourceDBUser != nil {
		baseContext.SourceDBUser = *cfg.SourceDBUser
	}
	if !seen["source-db-password"] && cfg.SourceDBPassword != nil {
		baseContext.SourceDBPass = *cfg.SourceDBPassword
	}
	if !seen["target-db-host"] && cfg.TargetDBHost != nil {
		baseContext.TargetDBHost = *cfg.TargetDBHost
	}
	if !seen["target-db-port"] && cfg.TargetDBPort != nil {
		baseContext.TargetDBPort = *cfg.TargetDBPort
	}
	if !seen["target-db-user"] && cfg.TargetDBUser != nil {
		baseContext.TargetDBUser = *cfg.TargetDBUser
	}
	if !seen["target-db-password"] && cfg.TargetDBPassword != nil {
		baseContext.TargetDBPass = *cfg.TargetDBPassword
	}
	if !seen["conn-db-timeout"] && cfg.ConnDBTimeout != nil {
		baseContext.Timeout = *cfg.ConnDBTimeout
	}
	if !seen["tables"] && cfg.Tables != nil {
		*tables = *cfg.Tables
	}
	if !seen["check-column-names"] && cfg.CheckColumnNames != nil {
		baseContext.RequestedColumnNames = *cfg.CheckColumnNames
	}
	if !seen["specified-time-column"] && cfg.SpecifiedTimeColumn != nil {
		baseContext.SpecifiedDatetimeColumn = *cfg.SpecifiedTimeColumn
	}
	if !seen["specified-time-begin"] && cfg.SpecifiedTimeBegin != nil {
		*specifiedTimeBegin = *cfg.SpecifiedTimeBegin
	}
	if !seen["specified-time-end"] && cfg.SpecifiedTimeEnd != nil {
		*specifiedTimeEnd = *cfg.SpecifiedTimeEnd
	}
	if !seen["time-range-per-step"] && cfg.TimeRangePerStep != nil {
		d, err := time.ParseDuration(*cfg.TimeRangePerStep)
		if err != nil {
			return fmt.Errorf("invalid time-range-per-step %q: %v", *cfg.TimeRangePerStep, err)
		}
		baseContext.SpecifiedTimeRangePerStep = d
	}
	if !seen["batch-diffs"] && cfg.BatchDiffs != nil {
		*batchDiffs = *cfg.BatchDiffs
	}
	if !seen["max-iterations"] && cfg.MaxIterations != nil {
		*maxIterations = *cfg.MaxIterations
	}
	if !seen["execute"] && cfg.Execute != nil {
		*execute = *cfg.Execute
	}
	if !seen["apply-batch-size"] && cfg.ApplyBatchSize != nil {
		*applyBatchSize = *cfg.ApplyBatchSize
	}
	if !seen["skip-binlog"] && cfg.SkipBinlog != nil {
		*skipBinlog = *cfg.SkipBinlog
	}
	if !seen["skip-fk-checks"] && cfg.SkipFK != nil {
		*skipFK = *cfg.SkipFK
	}
	if !seen["skip-unique-checks"] && cfg.SkipUnique != nil {
		*skipUnique = *cfg.SkipUnique
	}
	if !seen["no-auto-value-on-zero"] && cfg.NoAutoValueOnZero != nil {
		*noAutoValueOnZero = *cfg.NoAutoValueOnZero
	}
	if !seen["debug"] && cfg.Debug != nil {
		*debug = *cfg.Debug
	}
	if !seen["logfile"] && cfg.LogFile != nil {
		*logFile = *cfg.LogFile
	}

	return nil
}

// tablePair is a resolved source/target db.table pair to converge. Target
// db/table names are assumed identical to source -- true for a plain
// replica, which is the scenario this tool targets.
type tablePair struct {
	sourceDB, sourceTable string
	targetDB, targetTable string
}

func (p tablePair) String() string {
	return fmt.Sprintf("%s.%s => %s.%s", p.sourceDB, p.sourceTable, p.targetDB, p.targetTable)
}

func parseTables(spec string) ([]tablePair, error) {
	var pairs []tablePair
	for _, entry := range strings.Split(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ".", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return nil, fmt.Errorf("invalid entry %q in --tables, expected db.table", entry)
		}
		pairs = append(pairs, tablePair{sourceDB: parts[0], sourceTable: parts[1], targetDB: parts[0], targetTable: parts[1]})
	}
	if len(pairs) == 0 {
		return nil, fmt.Errorf("--tables must list at least one db.table")
	}
	return pairs, nil
}

// applyStatements executes REPLACE INTO statements against db in
// transactional batches. Unlike go-data-sync, there is no retry loop: a
// nibble run is short and interactive, so a failure should surface
// immediately instead of being silently retried.
func applyStatements(db *gosql.DB, statements []string, batchSize int) (int, error) {
	applied := 0
	for i := 0; i < len(statements); i += batchSize {
		end := i + batchSize
		if end > len(statements) {
			end = len(statements)
		}
		batch := statements[i:end]

		trx, err := db.Begin()
		if err != nil {
			return applied, fmt.Errorf("begin transaction: %v", err)
		}
		for _, stmt := range batch {
			if _, err := trx.Exec(stmt); err != nil {
				trx.Rollback()
				return applied, fmt.Errorf("statement failed (batch rolled back): %v\n  %.200s", err, stmt)
			}
		}
		if err := trx.Commit(); err != nil {
			return applied, fmt.Errorf("commit transaction: %v", err)
		}
		applied += len(batch)
	}
	return applied, nil
}

// nibbleTable converges a single table pair: collect differences within the
// configured time window, apply up to --batch-diffs of them, and repeat.
// Each pass rescans the whole window (not just the tail), so already-equal
// chunks are cheap re-checks but not free -- size --batch-diffs generously
// enough that most tables converge in one or two passes rather than relying
// on --max-iterations to grind through the backlog.
func nibbleTable(baseContext *types.BaseContext, pair tablePair, execute bool, maxIterations, applyBatchSize int) error {
	tableContext := types.NewTableContext(pair.sourceDB, pair.sourceTable, pair.targetDB, pair.targetTable)
	ctx := checksum.NewChecksumContext(baseContext, tableContext)
	ctx.DifferentialUseTimeRange = true

	if err := ctx.GetCheckColumns(); err != nil {
		return err
	}
	if err := ctx.GetUniqueKeys(); err != nil {
		return err
	}
	if err := ctx.GetTimeColumn(); err != nil {
		return err
	}

	differ := &checksum.TableDiffer{Context: ctx}

	for iteration := 1; ; iteration++ {
		if iteration > maxIterations {
			return fmt.Errorf("gave up after %d iteration(s), still not converged (raise --max-iterations or --batch-diffs)", maxIterations)
		}

		report, err := differ.CollectDifferences()
		if err != nil {
			return fmt.Errorf("iteration %d: %v", iteration, err)
		}

		totalSyncable := report.SourceOnlyRecords + report.ModifiedRecords
		baseContext.Log.Infof("%s: iteration %d: %d source-only, %d target-only, %d modified, %d identical",
			pair, iteration, report.SourceOnlyRecords, report.TargetOnlyRecords, report.ModifiedRecords, report.IdenticalRecords)

		if report.TargetOnlyRecords > 0 {
			baseContext.Log.Warnf("%s: %d target-only record(s) in this window -- rows that exist on the target with no source match are never touched by this tool (it only backfills/repairs, never deletes); review manually",
				pair, report.TargetOnlyRecords)
		}

		if totalSyncable == 0 {
			baseContext.Log.Infof("%s: converged after %d iteration(s)", pair, iteration)
			return nil
		}

		allColumns, err := ctx.GetAllColumns()
		if err != nil {
			return err
		}
		statements := differ.BuildReplaceIntoStatements(report.SampleDifferences, allColumns)

		if !execute {
			baseContext.Log.Infof("%s: DRY-RUN, %d statement(s) would be applied this pass (%d syncable difference(s) found in the window); re-run with --execute",
				pair, len(statements), totalSyncable)
			preview := statements
			if len(preview) > 5 {
				preview = preview[:5]
			}
			for _, stmt := range preview {
				fmt.Printf("  %.160s\n", stmt)
			}
			return nil
		}

		applied, err := applyStatements(baseContext.TargetDB, statements, applyBatchSize)
		if err != nil {
			return fmt.Errorf("iteration %d: applied %d/%d statement(s) before failing: %v", iteration, applied, len(statements), err)
		}
		baseContext.Log.Infof("%s: iteration %d: applied %d statement(s)", pair, iteration, applied)
	}
}

// initDB opens the source and target connections. It mirrors
// BaseContext.InitDB, except the target DSN can optionally carry the same
// session-level overrides go-data-sync supports (--skip-binlog etc.) --
// needed here because, unlike go-data-checksum, this tool writes to the
// target directly. The source connection is never affected.
func initDB(baseContext *types.BaseContext, skipBinlog, skipFK, skipUnique, noAutoValueOnZero bool) error {
	databaseName := "information_schema"
	sourceDBUri := types.BuildDBUri(baseContext.SourceDBUser, baseContext.SourceDBPass, baseContext.SourceDBHost, baseContext.SourceDBPort, databaseName, baseContext.Timeout)
	targetDBUri := types.BuildDBUri(baseContext.TargetDBUser, baseContext.TargetDBPass, baseContext.TargetDBHost, baseContext.TargetDBPort, databaseName, baseContext.Timeout)
	if skipFK {
		targetDBUri += "&foreign_key_checks=0"
	}
	if skipUnique {
		targetDBUri += "&unique_checks=0"
	}
	if skipBinlog {
		targetDBUri += "&sql_log_bin=0"
	}
	if noAutoValueOnZero {
		targetDBUri += "&sql_mode=%27NO_AUTO_VALUE_ON_ZERO%27"
	}

	connect := func(dbUri string) (*gosql.DB, error) {
		db, err := gosql.Open("mysql", dbUri)
		if err != nil {
			return nil, err
		}
		if err := db.Ping(); err != nil {
			return nil, err
		}
		db.SetConnMaxLifetime(3 * time.Minute)
		db.SetMaxIdleConns(30)
		return db, nil
	}

	var err error
	if baseContext.SourceDB, err = connect(sourceDBUri); err != nil {
		return err
	}
	if baseContext.TargetDB, err = connect(targetDBUri); err != nil {
		return err
	}
	return nil
}

func main() {
	baseContext := types.NewBaseContext()

	flag.StringVar(&baseContext.SourceDBHost, "source-db-host", "127.0.0.1", "Source MySQL hostname")
	flag.IntVar(&baseContext.SourceDBPort, "source-db-port", 3306, "Source MySQL port")
	flag.StringVar(&baseContext.SourceDBUser, "source-db-user", "", "Source MySQL user")
	flag.StringVar(&baseContext.SourceDBPass, "source-db-password", "", "Source MySQL password")
	flag.StringVar(&baseContext.TargetDBHost, "target-db-host", "127.0.0.1", "Target MySQL hostname")
	flag.IntVar(&baseContext.TargetDBPort, "target-db-port", 3306, "Target MySQL port")
	flag.StringVar(&baseContext.TargetDBUser, "target-db-user", "", "Target MySQL user")
	flag.StringVar(&baseContext.TargetDBPass, "target-db-password", "", "Target MySQL password")
	flag.IntVar(&baseContext.Timeout, "conn-db-timeout", 60, "connect db timeout")

	tables := flag.String("tables", "", "Comma-separated db.table list to converge, eg: sbtest.orders,sbtest.users (required). Target db/table names are assumed identical to source.")
	flag.StringVar(&baseContext.RequestedColumnNames, "check-column-names", "", "Column names to check, eg: col1,col2,col3. By default, all columns are used.")
	flag.StringVar(&baseContext.SpecifiedDatetimeColumn, "specified-time-column", "", "Time column to nibble by, eg: last_updated (required).")
	specifiedDatetimeRangeBegin := flag.String("specified-time-begin", "", "Start of the drift window, eg. when the replica fell behind (required).")
	specifiedDatetimeRangeEnd := flag.String("specified-time-end", "", "End of the drift window. Defaults to now, captured once at startup so a table still taking live writes converges instead of chasing a moving target.")
	flag.DurationVar(&baseContext.SpecifiedTimeRangePerStep, "time-range-per-step", 5*time.Minute, "Time window size per nibble step, eg: 1h/15m/30s. Use a smaller step for very large/high-write tables to bound memory per pass.")
	batchDiffs := flag.Int("batch-diffs", 500, "Max differences collected (and applied) per iteration. Size this generously relative to the expected drift so tables converge in a handful of passes instead of relying on --max-iterations.")
	maxIterations := flag.Int("max-iterations", 50, "Give up on a table after this many iterations without converging.")
	execute := flag.Bool("execute", false, "Actually apply the statements. Without this flag, runs one dry-run analysis pass per table and exits (like go-data-sync).")
	applyBatchSize := flag.Int("apply-batch-size", 200, "Number of REPLACE INTO statements per transaction when applying.")
	skipBinlog := flag.Bool("skip-binlog", false, "Set @@session.sql_log_bin = 0 on the target connection. Recommended when repairing a replica directly, so applied REPLACE INTOs don't become errant GTID transactions on a GTID-enabled replica.")
	skipFK := flag.Bool("skip-fk-checks", false, "Set @@session.foreign_key_checks = 0 on the target connection.")
	skipUnique := flag.Bool("skip-unique-checks", false, "Set @@session.unique_checks = 0 on the target connection.")
	noAutoValueOnZero := flag.Bool("no-auto-value-on-zero", false, "Set @@session.sql_mode = 'NO_AUTO_VALUE_ON_ZERO' on the target connection.")
	debug := flag.Bool("debug", false, "debug mode (very verbose)")
	logFile := flag.String("logfile", "", "Log file name.")
	configFile := flag.String("config", "", "Path to a JSON configuration file to load arguments from")
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

	if *configFile != "" {
		if err := loadNibbleConfig(*configFile, baseContext, tables, specifiedDatetimeRangeBegin, specifiedDatetimeRangeEnd, batchDiffs, maxIterations, applyBatchSize, execute, skipBinlog, skipFK, skipUnique, noAutoValueOnZero, debug, logFile); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to load config file: %v\n", err)
			os.Exit(1)
		}
	}

	baseContext.SetLogLevel(*debug, *logFile)

	if *tables == "" {
		baseContext.Log.Fatalf("--tables is required, eg: --tables sbtest.orders,sbtest.users")
	}
	if baseContext.SpecifiedDatetimeColumn == "" {
		baseContext.Log.Fatalf("--specified-time-column is required")
	}
	if *specifiedDatetimeRangeBegin == "" {
		baseContext.Log.Fatalf("--specified-time-begin is required")
	}
	// Fix the end of the window once at startup: if it tracked time.Now() on
	// every iteration instead, a table still taking live writes would never
	// converge -- CollectDifferences would keep finding "new" differences in
	// the ever-growing tail of the window.
	if *specifiedDatetimeRangeEnd == "" {
		*specifiedDatetimeRangeEnd = time.Now().Format("2006-01-02 15:04:05")
	}
	if err := baseContext.SetSpecifiedDatetimeRange(*specifiedDatetimeRangeBegin, *specifiedDatetimeRangeEnd); err != nil {
		baseContext.Log.Fatalf("Illegal time range (%v), please check!", err)
	}
	baseContext.MaxSampleDifferences = *batchDiffs

	pairs, err := parseTables(*tables)
	if err != nil {
		baseContext.Log.Fatalf("%v", err)
	}

	if !*execute {
		baseContext.Log.Infof("DRY-RUN mode: one analysis pass per table, nothing will be applied. Re-run with --execute to converge.")
	}

	startTime := time.Now()
	defer func() {
		baseContext.CloseDB()
		baseContext.Log.Infof("Finished go-data-nibble. TotalDuration=%+v", time.Since(startTime))
	}()

	if err := initDB(baseContext, *skipBinlog, *skipFK, *skipUnique, *noAutoValueOnZero); err != nil {
		baseContext.Log.Fatalf("DB connection initiate failed: %v", err)
	}

	failed := 0
	for _, pair := range pairs {
		if err := nibbleTable(baseContext, pair, *execute, *maxIterations, *applyBatchSize); err != nil {
			failed++
			baseContext.Log.Errorf("%s: %v", pair, err)
		}
	}

	if failed > 0 {
		baseContext.Log.Errorf("%d/%d table(s) failed to converge.", failed, len(pairs))
		os.Exit(1)
	}
	baseContext.Log.Infof("All %d table(s) converged.", len(pairs))
}
