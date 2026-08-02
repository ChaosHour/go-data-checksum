// Command go-data-nibble-loadtest is a Go rewrite of scripts/nibble-loadtest.sh.
//
// It creates a synthetic large-table drift scenario against your existing
// primary/replica MySQL containers and runs go-data-nibble against it, so
// its behavior at scale (timing, chunking, convergence) can be observed
// before pointing it at a real multi-hundred-GB MyISAM table.
//
// This reuses your already-running primary/replica containers (defaults:
// 127.0.0.1:3306 primary, 127.0.0.1:3307 replica) instead of spinning up
// throwaway ones. It does NOT touch any of your existing schemas or their
// replication:
//
//  1. Checks the replica is healthy (IO+SQL threads running) before
//     touching anything, and records the current replication filter state
//     so it can be restored exactly afterward.
//  2. Stops only the replica SQL thread (not IO -- no relay log gap) and
//     adds a REPLICATE_IGNORE_DB filter for a dedicated `nibbletest`
//     schema, so writes to that schema on the primary are never
//     auto-applied to the replica. Restarts the SQL thread.
//  3. Creates `nibbletest` independently on both sides, seeds a large
//     table on the primary via containerized sysbench (severalnines/sysbench
//     -- nothing installed on the host), clones it to the replica as
//     MyISAM via mysqldump.
//  4. Updates/inserts a batch of rows on the primary only, inside a known
//     time window -- because of the filter from step 2, these never reach
//     the replica, simulating a replica that fell behind.
//  5. Runs go-data-nibble dry-run, then --execute, timing each pass.
//  6. Re-runs dry-run once more and confirms convergence.
//  7. Cleanup: always drops `nibbletest` on both sides and the scoped
//     sysbench DB user, and ALWAYS restores the replication filter to
//     exactly what it was before (even on failure/Ctrl-C).
//
// Requirements: docker (for containerized sysbench), and local `mysql` /
// `mysqldump` clients on PATH. A bin/go-data-nibble build (built
// automatically via `make build` if missing). Run from the repository root
// (or pass -repo-root).
//
// DB credentials: reads PRIMARY_DB_PASSWORD from the environment, or falls
// back to the `password=` line under [client] in ~/.my.cnf. Never printed.
package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	testDB       = "nibbletest"
	tableName    = "sbtest1"
	sysbenchUser = "sysbench_lt"
)

type options struct {
	primaryHost      string
	primaryPort      int
	replicaHost      string
	replicaPort      int
	dbUser           string
	tableSize        int
	driftUpdates     int
	driftInserts     int
	timeRangePerStep string
	batchDiffs       int
	maxIterations    int
	keep             bool
	repoRoot         string
}

func logf(format string, a ...any) {
	fmt.Printf("\n\033[1;34m==>\033[0m %s\n", fmt.Sprintf(format, a...))
}

// failure is a sentinel error type so run() can distinguish "print FAIL and
// exit 1" from an unexpected Go error -- in practice both are handled the
// same way by main(), but keeping them distinct documents intent at call
// sites (mirrors the bash script's fail() helper).
type failure struct{ msg string }

func (f *failure) Error() string { return f.msg }

func fail(format string, a ...any) error {
	return &failure{msg: fmt.Sprintf(format, a...)}
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\n\033[1;31mFAIL:\033[0m %s\n", err)
		os.Exit(1)
	}
}

func run() error {
	opts := options{}
	flag.StringVar(&opts.primaryHost, "primary-host", "127.0.0.1", "Primary MySQL host")
	flag.IntVar(&opts.primaryPort, "primary-port", 3306, "Primary MySQL port")
	flag.StringVar(&opts.replicaHost, "replica-host", "127.0.0.1", "Replica MySQL host")
	flag.IntVar(&opts.replicaPort, "replica-port", 3307, "Replica MySQL port")
	flag.StringVar(&opts.dbUser, "db-user", "root", "MySQL user (same on both sides)")
	flag.IntVar(&opts.tableSize, "table-size", 2_000_000, "Rows to seed via sysbench")
	flag.IntVar(&opts.driftUpdates, "drift-updates", 50_000, "Existing rows to update only on primary")
	flag.IntVar(&opts.driftInserts, "drift-inserts", 5_000, "New rows to insert only on primary")
	flag.StringVar(&opts.timeRangePerStep, "time-range-per-step", "15m", "Nibble step duration, eg 15m/1h")
	flag.IntVar(&opts.batchDiffs, "batch-diffs", 5_000, "--batch-diffs passed to go-data-nibble")
	flag.IntVar(&opts.maxIterations, "max-iterations", 50, "--max-iterations passed to go-data-nibble")
	flag.BoolVar(&opts.keep, "keep", false, "Don't drop the nibbletest schema/user on success (replication filter is ALWAYS restored regardless)")
	flag.StringVar(&opts.repoRoot, "repo-root", ".", "Path to the go-data-checksum repository root")
	flag.Parse()

	repoRoot, err := filepath.Abs(opts.repoRoot)
	if err != nil {
		return err
	}
	opts.repoRoot = repoRoot

	primaryPassword, err := readPassword()
	if err != nil {
		return err
	}
	replicaPassword := primaryPassword
	sysbenchPassword := fmt.Sprintf("sysbench_lt_%d", os.Getpid())

	for _, tool := range []string{"docker", "mysql", "mysqldump"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fail("%s is required but not found in PATH", tool)
		}
	}

	nibbleBin := filepath.Join(repoRoot, "bin", "go-data-nibble")
	if info, statErr := os.Stat(nibbleBin); statErr != nil || info.Mode()&0o111 == 0 {
		logf("bin/go-data-nibble not found, building")
		buildCmd := exec.Command("make", "build")
		buildCmd.Dir = repoRoot
		buildCmd.Stdout = os.Stdout
		buildCmd.Stderr = os.Stderr
		if err := buildCmd.Run(); err != nil {
			return fail("make build failed: %s", err)
		}
	}

	logf("Checking connectivity to primary (%s:%d) and replica (%s:%d)", opts.primaryHost, opts.primaryPort, opts.replicaHost, opts.replicaPort)
	primaryDB, err := openDB(opts.primaryHost, opts.primaryPort, opts.dbUser, primaryPassword, "")
	if err != nil {
		return fail("cannot connect to primary: %s", err)
	}
	defer primaryDB.Close()
	replicaDB, err := openDB(opts.replicaHost, opts.replicaPort, opts.dbUser, replicaPassword, "")
	if err != nil {
		return fail("cannot connect to replica: %s", err)
	}
	defer replicaDB.Close()

	logf("Checking replica health before touching anything")
	status, err := queryRow(replicaDB, "SHOW REPLICA STATUS")
	if err != nil {
		return fail("SHOW REPLICA STATUS failed: %s", err)
	}
	if status == nil {
		return fail("SHOW REPLICA STATUS returned no rows -- is this host actually a replica?")
	}
	ioRunning, sqlRunning := status["Replica_IO_Running"], status["Replica_SQL_Running"]
	if ioRunning != "Yes" || sqlRunning != "Yes" {
		return fail("replica is not healthy (IO=%s SQL=%s) -- refusing to touch replication filters on an already-unhealthy replica", ioRunning, sqlRunning)
	}
	originalIgnoreDB := status["Replicate_Ignore_DB"]
	logf("Replica healthy. Current Replicate_Ignore_DB='%s' (will be restored on exit)", originalIgnoreDB)

	var (
		filterChanged bool
		succeeded     bool
		cleanupOnce   sync.Once
	)

	cleanup := func() {
		if filterChanged {
			logf("Restoring replica replication filter to original state")
			mustExecWarn(replicaDB, "STOP REPLICA SQL_THREAD;")
			if originalIgnoreDB != "" {
				mustExecWarn(replicaDB, fmt.Sprintf("CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = (%s);", originalIgnoreDB))
			} else {
				mustExecWarn(replicaDB, "CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = ();")
			}
			mustExecWarn(replicaDB, "START REPLICA SQL_THREAD;")
		}

		if opts.keep {
			logf("Leaving %s schema and %s user in place (-keep). Replication filter has still been restored.", testDB, sysbenchUser)
			return
		}

		logf("Dropping %s on both sides and the %s user", testDB, sysbenchUser)
		mustExecWarn(primaryDB, fmt.Sprintf("DROP DATABASE IF EXISTS %s;", testDB))
		mustExecWarn(primaryDB, fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%';", sysbenchUser))
		mustExecWarn(replicaDB, fmt.Sprintf("DROP DATABASE IF EXISTS %s;", testDB))

		if !succeeded {
			logf("Run failed. Cleaned up test schema/filter, but see output above for what went wrong.")
		}
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cleanupOnce.Do(cleanup)
		os.Exit(1)
	}()
	defer cleanupOnce.Do(cleanup)

	logf("Stopping replica SQL thread and adding REPLICATE_IGNORE_DB filter for '%s' (IO thread keeps running -- no relay log gap)", testDB)
	if _, err := replicaDB.Exec("STOP REPLICA SQL_THREAD;"); err != nil {
		return fail("STOP REPLICA SQL_THREAD failed: %s", err)
	}
	combinedIgnoreDB := testDB
	if originalIgnoreDB != "" {
		combinedIgnoreDB = originalIgnoreDB + "," + testDB
	}
	if _, err := replicaDB.Exec(fmt.Sprintf("CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = (%s);", combinedIgnoreDB)); err != nil {
		return fail("CHANGE REPLICATION FILTER failed: %s", err)
	}
	if _, err := replicaDB.Exec("START REPLICA SQL_THREAD;"); err != nil {
		return fail("START REPLICA SQL_THREAD failed: %s", err)
	}
	filterChanged = true

	logf("Creating %s independently on primary and replica (filter means primary's copy of the DDL is never applied on replica)", testDB)
	if _, err := primaryDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", testDB)); err != nil {
		return fail("create database on primary failed: %s", err)
	}
	if _, err := replicaDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s;", testDB)); err != nil {
		return fail("create database on replica failed: %s", err)
	}

	// REPLICATE_IGNORE_DB filters DDL (which is always statement-based, even
	// under binlog_format=ROW) by the connection's *default database* (as
	// set by USE), not by a schema-qualified table name inside the
	// statement -- so "ALTER TABLE nibbletest.sbtest1 ..." issued on a
	// connection with no default database is NOT filtered and replicates
	// straight to the replica, colliding with the column already present
	// from the mysqldump clone. DML (INSERT/UPDATE) is filtered correctly
	// by the row event's actual table, but a dedicated database=testDB
	// connection is used for those too, to not depend on binlog_format
	// being ROW. This mirrors the bash/zsh scripts' `mysql_primary
	// "$TEST_DB" -e ...` (positional db arg sets the default database).
	primaryTestDB, err := openDB(opts.primaryHost, opts.primaryPort, opts.dbUser, primaryPassword, testDB)
	if err != nil {
		return fail("cannot open %s-scoped connection to primary: %s", testDB, err)
	}
	defer primaryTestDB.Close()

	logf("Creating scoped sysbench user (mysql_native_password -- the sysbench container's old client can't do caching_sha2_password)")
	// Drop first: a prior run (esp. one left behind via -keep, or interrupted
	// before cleanup) may have left this user with a different password, and
	// CREATE USER IF NOT EXISTS is a no-op on the password when the user
	// already exists, which then makes the new sysbench call fail auth.
	for _, stmt := range []string{
		fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%';", sysbenchUser),
		fmt.Sprintf("CREATE USER '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s';", sysbenchUser, sysbenchPassword),
		fmt.Sprintf("GRANT ALL PRIVILEGES ON %s.* TO '%s'@'%%';", testDB, sysbenchUser),
		"FLUSH PRIVILEGES;",
	} {
		if _, err := primaryDB.Exec(stmt); err != nil {
			return fail("failed to create sysbench user: %s", err)
		}
	}

	logf("Seeding %d rows into %s.%s on primary via containerized sysbench", opts.tableSize, testDB, tableName)
	sysbenchCmd := exec.Command("docker", "run", "--rm", "severalnines/sysbench", "sysbench", "oltp_read_write",
		"--mysql-host=host.docker.internal", fmt.Sprintf("--mysql-port=%d", opts.primaryPort),
		fmt.Sprintf("--mysql-user=%s", sysbenchUser), fmt.Sprintf("--mysql-password=%s", sysbenchPassword),
		fmt.Sprintf("--mysql-db=%s", testDB), "--tables=1", fmt.Sprintf("--table-size=%d", opts.tableSize), "prepare")
	sysbenchCmd.Stdout = os.Stdout
	sysbenchCmd.Stderr = os.Stderr
	if err := sysbenchCmd.Run(); err != nil {
		return fail("sysbench prepare failed: %s", err)
	}

	logf("Adding last_updated column + index (not part of sysbench's schema)")
	if _, err := primaryTestDB.Exec(
		"ALTER TABLE " + tableName + " ADD COLUMN last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ADD INDEX idx_last_updated (last_updated);"); err != nil {
		return fail("failed to add last_updated column: %s", err)
	}

	logf("Cloning schema+data to replica as MyISAM (this is the slow step for large -table-size)")
	if err := cloneAsMyISAM(opts, primaryPassword, replicaPassword); err != nil {
		return err
	}

	logf("Confirming replica table engine is MyISAM")
	var engine string
	engineRow := replicaDB.QueryRow(
		"SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?;", testDB, tableName)
	if err := engineRow.Scan(&engine); err != nil {
		return fail("failed to read replica table engine: %s", err)
	}
	if engine != "MyISAM" {
		return fail("replica table engine is '%s', expected MyISAM", engine)
	}

	var windowStart string
	if err := primaryDB.QueryRow("SELECT NOW();").Scan(&windowStart); err != nil {
		return fail("failed to read window start: %s", err)
	}
	logf("Drift window starts at %s", windowStart)

	logf("Simulating drift: updating %d existing rows on primary only (filter keeps these off the replica)", opts.driftUpdates)
	if _, err := primaryTestDB.Exec(fmt.Sprintf(
		"UPDATE %s SET k = k + 1, last_updated = NOW() WHERE id <= %d;", tableName, opts.driftUpdates)); err != nil {
		return fail("drift update failed: %s", err)
	}

	logf("Simulating drift: inserting %d new rows on primary only", opts.driftInserts)
	if _, err := primaryTestDB.Exec(fmt.Sprintf(
		"INSERT INTO %s (k, c, pad, last_updated) SELECT k, c, pad, NOW() FROM %s LIMIT %d;",
		tableName, tableName, opts.driftInserts)); err != nil {
		return fail("drift insert failed: %s", err)
	}

	// Pull the window end from the primary's own clock rather than letting
	// go-data-nibble default to the host's local time.Now(): if the MySQL
	// server runs in a different timezone than this host (containers here
	// run UTC), comparing a host-local default against last_updated values
	// stamped by the server's NOW() can put specified-time-end before
	// specified-time-begin. Add a 1-minute buffer on top -- this whole drift
	// simulation runs in well under a second, so an unbuffered "now" can
	// land in the same second as windowStart, producing a zero-width window
	// (a real lagging replica would never have this problem; it's an
	// artifact of how fast this program runs).
	var windowEnd string
	if err := primaryDB.QueryRow("SELECT DATE_ADD(NOW(), INTERVAL 1 MINUTE);").Scan(&windowEnd); err != nil {
		return fail("failed to read window end: %s", err)
	}

	nibbleArgs := []string{
		fmt.Sprintf("--source-db-host=%s", opts.primaryHost), fmt.Sprintf("--source-db-port=%d", opts.primaryPort),
		fmt.Sprintf("--source-db-user=%s", opts.dbUser), fmt.Sprintf("--source-db-password=%s", primaryPassword),
		fmt.Sprintf("--target-db-host=%s", opts.replicaHost), fmt.Sprintf("--target-db-port=%d", opts.replicaPort),
		fmt.Sprintf("--target-db-user=%s", opts.dbUser), fmt.Sprintf("--target-db-password=%s", replicaPassword),
		fmt.Sprintf("--tables=%s.%s", testDB, tableName),
		"--specified-time-column=last_updated",
		fmt.Sprintf("--specified-time-begin=%s", windowStart),
		fmt.Sprintf("--specified-time-end=%s", windowEnd),
		fmt.Sprintf("--time-range-per-step=%s", opts.timeRangePerStep),
		fmt.Sprintf("--batch-diffs=%d", opts.batchDiffs),
		fmt.Sprintf("--max-iterations=%d", opts.maxIterations),
	}

	logf("Dry run (no --execute) -- timing this shows how long one pass takes at this table size")
	dryRunStart := time.Now()
	dryRunOutput, dryRunErr := runNibbleCaptured(nibbleBin, nibbleArgs)
	dryRunElapsed := time.Since(dryRunStart)
	fmt.Println(dryRunOutput)
	if dryRunErr != nil {
		return fail("dry run failed: %s", dryRunErr)
	}
	logf("Dry run took %.0fs", dryRunElapsed.Seconds())
	if strings.Contains(dryRunOutput, "converged after") {
		return fail(
			"dry run reported convergence with no statements applied, but we just injected %d drifted rows -- "+
				"the time window likely didn't cover them (check windowStart/windowEnd against last_updated values), not a real pass",
			opts.driftUpdates+opts.driftInserts)
	}

	logf("Executing (--execute) -- this applies REPLACE INTO statements to the replica")
	executeStart := time.Now()
	executeCmd := exec.Command(nibbleBin, append(append([]string{}, nibbleArgs...), "--execute")...)
	executeCmd.Stdout = os.Stdout
	executeCmd.Stderr = os.Stderr
	if err := executeCmd.Run(); err != nil {
		return fail("execute run failed: %s", err)
	}
	executeElapsed := time.Since(executeStart)
	logf("Execute run took %.0fs", executeElapsed.Seconds())

	logf("Verifying convergence with one more dry run")
	verifyOutput, verifyErr := runNibbleCaptured(nibbleBin, nibbleArgs)
	fmt.Println(verifyOutput)
	if verifyErr != nil {
		return fail("verify run failed: %s", verifyErr)
	}
	if !strings.Contains(verifyOutput, "converged after") {
		return fail("did not observe a 'converged after' log line on the verification pass -- table did not fully converge")
	}

	succeeded = true
	logf("PASS -- table converged. dry-run=%.0fs execute=%.0fs table-size=%d drift=%d rows time-range-per-step=%s batch-diffs=%d",
		dryRunElapsed.Seconds(), executeElapsed.Seconds(), opts.tableSize, opts.driftUpdates+opts.driftInserts, opts.timeRangePerStep, opts.batchDiffs)

	return nil
}

func openDB(host string, port int, user, password, database string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?timeout=10s", user, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// queryRow runs query and returns the first result row as a column-name ->
// string-value map (mirrors the bash script's awk-based column lookup
// against SHOW REPLICA STATUS's tab-separated output). Returns a nil map,
// nil error if the query produced no rows.
func queryRow(db *sql.DB, query string) (map[string]string, error) {
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, rows.Err()
	}

	raw := make([]sql.RawBytes, len(cols))
	scanArgs := make([]any, len(cols))
	for i := range raw {
		scanArgs[i] = &raw[i]
	}
	if err := rows.Scan(scanArgs...); err != nil {
		return nil, err
	}

	result := make(map[string]string, len(cols))
	for i, col := range cols {
		result[col] = string(raw[i])
	}
	return result, rows.Err()
}

// mustExecWarn runs stmt and prints a warning (never fails the caller) --
// used only in cleanup(), which mirrors the bash trap's `|| true` on every
// restoration statement so one failed cleanup step doesn't stop the rest
// from running.
func mustExecWarn(db *sql.DB, stmt string) {
	if _, err := db.Exec(stmt); err != nil {
		fmt.Fprintf(os.Stderr, "warning: cleanup statement failed (%s): %s\n", stmt, err)
	}
}

func readPassword() (string, error) {
	if password := os.Getenv("PRIMARY_DB_PASSWORD"); password != "" {
		return password, nil
	}

	home, err := os.UserHomeDir()
	if err == nil {
		if password, ok := readMyCnfPassword(filepath.Join(home, ".my.cnf")); ok {
			return password, nil
		}
	}

	return "", fail("no password found -- set PRIMARY_DB_PASSWORD or add [client]/password to ~/.my.cnf")
}

func readMyCnfPassword(path string) (string, bool) {
	f, err := os.Open(path)
	if err != nil {
		return "", false
	}
	defer f.Close()

	inClient := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "[") {
			inClient = strings.EqualFold(line, "[client]")
			continue
		}
		if !inClient {
			continue
		}
		if after, ok := strings.CutPrefix(strings.ToLower(line), "password"); ok {
			after = strings.TrimSpace(after)
			if strings.HasPrefix(after, "=") {
				value := strings.TrimSpace(strings.TrimPrefix(after, "="))
				if value != "" {
					return value, true
				}
			}
		}
	}
	return "", false
}

// cloneAsMyISAM pipes mysqldump | sed | mysql, matching the bash script's
// pipeline exactly (including the mysqldump flags needed to avoid
// GTID_PURGED collisions against an already-replicating target).
func cloneAsMyISAM(opts options, primaryPassword, replicaPassword string) error {
	dumpCmd := exec.Command("mysqldump",
		"-h", opts.primaryHost, "-P", fmt.Sprintf("%d", opts.primaryPort), "-u", opts.dbUser,
		fmt.Sprintf("-p%s", primaryPassword), "--single-transaction", "--set-gtid-purged=OFF", testDB, tableName)
	sedCmd := exec.Command("sed", "s/ENGINE=InnoDB/ENGINE=MyISAM/")
	mysqlCmd := exec.Command("mysql",
		"-h", opts.replicaHost, "-P", fmt.Sprintf("%d", opts.replicaPort), "-u", opts.dbUser,
		fmt.Sprintf("-p%s", replicaPassword), testDB)

	dumpCmd.Stderr = os.Stderr
	sedCmd.Stderr = os.Stderr
	mysqlCmd.Stderr = os.Stderr

	var err error
	sedCmd.Stdin, err = dumpCmd.StdoutPipe()
	if err != nil {
		return err
	}
	mysqlCmd.Stdin, err = sedCmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err := dumpCmd.Start(); err != nil {
		return fail("mysqldump failed to start: %s", err)
	}
	if err := sedCmd.Start(); err != nil {
		return fail("sed failed to start: %s", err)
	}
	if err := mysqlCmd.Start(); err != nil {
		return fail("mysql (load) failed to start: %s", err)
	}

	dumpErr := dumpCmd.Wait()
	sedErr := sedCmd.Wait()
	mysqlErr := mysqlCmd.Wait()
	if dumpErr != nil || sedErr != nil || mysqlErr != nil {
		return fail("mysqldump | sed | mysql clone pipeline failed (dump=%v sed=%v mysql=%v)", dumpErr, sedErr, mysqlErr)
	}
	return nil
}

func runNibbleCaptured(nibbleBin string, args []string) (string, error) {
	cmd := exec.Command(nibbleBin, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}
