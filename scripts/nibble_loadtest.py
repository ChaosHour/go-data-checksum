#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "pymysql>=1.1",
# ]
# ///
"""nibble_loadtest.py -- Python/uv port of nibble-loadtest.sh.

Creates a synthetic large-table drift scenario against your existing
primary/replica MySQL containers and runs go-data-nibble against it, so its
behavior at scale (timing, chunking, convergence) can be observed before
pointing it at a real multi-hundred-GB MyISAM table.

This reuses your already-running primary/replica containers (defaults:
127.0.0.1:3306 primary, 127.0.0.1:3307 replica -- matches the
client_primary1/client_replica1 groups in ~/.my.cnf) instead of spinning up
throwaway ones. It does NOT touch any of your existing schemas or their
replication. What it does:

  1. Checks the replica is healthy (IO+SQL threads running) before touching
     anything, and records the current replication filter state so it can
     be restored exactly afterward.
  2. Stops only the replica SQL thread (not IO -- no relay log gap) and adds
     a REPLICATE_IGNORE_DB filter for a dedicated `nibbletest` schema, so
     writes to that schema on the primary are never auto-applied to the
     replica. Restarts the SQL thread. This is what makes injected drift
     possible without disrupting your real replicated databases.
  3. Creates `nibbletest` independently on both sides, seeds a large table
     on the primary via a containerized sysbench (severalnines/sysbench --
     nothing installed on your host), clones it to the replica as MyISAM
     via mysqldump.
  4. Updates/inserts a batch of rows on the primary only, inside a known
     time window -- because of the filter from step 2, these never reach
     the replica, simulating a replica that fell behind.
  5. Runs go-data-nibble dry-run, then --execute, timing each pass.
  6. Re-runs dry-run once more and confirms convergence.
  7. Cleanup: always drops `nibbletest` on both sides and the scoped
     sysbench DB user, and ALWAYS restores the replication filter to
     exactly what it was before (even on failure/Ctrl-C) -- your real
     replication topology is never left modified.

Requirements: docker (for containerized sysbench), and local `mysql` /
`mysqldump` clients. A `bin/go-data-nibble` build (built automatically via
`make build` if missing).

DB credentials: reads PRIMARY_DB_PASSWORD from the environment, or falls
back to the `password=` line under [client] in ~/.my.cnf. Never printed.

Usage:
  uv run scripts/nibble_loadtest.py [options]
  ./scripts/nibble_loadtest.py [options]   (if executable + uv on PATH)
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import pymysql
import pymysql.cursors

REPO_ROOT = Path(__file__).resolve().parent.parent

TEST_DB = "nibbletest"
TABLE_NAME = "sbtest1"
SYSBENCH_USER = "sysbench_lt"
SYSBENCH_PASSWORD = f"sysbench_lt_{os.getpid()}"


def log(msg: str) -> None:
    print(f"\n\033[1;34m==>\033[0m {msg}")


def fail(msg: str) -> None:
    print(f"\n\033[1;31mFAIL:\033[0m {msg}", file=sys.stderr)
    sys.exit(1)


def read_password() -> str:
    password = os.environ.get("PRIMARY_DB_PASSWORD", "")
    if password:
        return password

    my_cnf = Path.home() / ".my.cnf"
    if my_cnf.exists():
        in_client = False
        for line in my_cnf.read_text().splitlines():
            stripped = line.strip()
            if stripped.startswith("["):
                in_client = stripped.lower() == "[client]"
                continue
            if in_client and stripped.lower().startswith("password"):
                _, _, value = stripped.partition("=")
                value = value.strip()
                if value:
                    return value

    fail("no password found -- set PRIMARY_DB_PASSWORD or add [client]/password to ~/.my.cnf")
    raise AssertionError("unreachable")  # fail() exits, this satisfies type checkers


def connect(host: str, port: int, user: str, password: str, database: str | None = None) -> pymysql.connections.Connection:
    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10,
    )


def execute(conn: pymysql.connections.Connection, sql: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(sql)
        try:
            return list(cur.fetchall())
        except pymysql.err.ProgrammingError:
            return []


def run_cmd(args: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(args, **kwargs)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create a synthetic large-table drift scenario and run go-data-nibble against it.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--primary-host", default="127.0.0.1")
    parser.add_argument("--primary-port", type=int, default=3306)
    parser.add_argument("--replica-host", default="127.0.0.1")
    parser.add_argument("--replica-port", type=int, default=3307)
    parser.add_argument("--db-user", default="root")
    parser.add_argument("--table-size", type=int, default=2_000_000)
    parser.add_argument("--drift-updates", type=int, default=50_000)
    parser.add_argument("--drift-inserts", type=int, default=5_000)
    parser.add_argument("--time-range-per-step", default="15m")
    parser.add_argument("--batch-diffs", type=int, default=5_000)
    parser.add_argument("--max-iterations", type=int, default=50)
    parser.add_argument("--keep", action="store_true", help="Don't drop the nibbletest schema/user on success")
    args = parser.parse_args()

    primary_password = read_password()
    replica_password = primary_password

    for tool in ("docker", "mysql", "mysqldump"):
        if subprocess.run(["which", tool], capture_output=True).returncode != 0:
            fail(f"{tool} is required but not found in PATH")

    nibble_bin = REPO_ROOT / "bin" / "go-data-nibble"
    if not (nibble_bin.exists() and os.access(nibble_bin, os.X_OK)):
        log("bin/go-data-nibble not found, building")
        subprocess.run(["make", "build"], cwd=REPO_ROOT, check=True)

    log(f"Checking connectivity to primary ({args.primary_host}:{args.primary_port}) and replica ({args.replica_host}:{args.replica_port})")
    try:
        primary_conn = connect(args.primary_host, args.primary_port, args.db_user, primary_password)
        execute(primary_conn, "SELECT 1;")
    except pymysql.err.MySQLError as exc:
        fail(f"cannot connect to primary: {exc}")
        return
    try:
        replica_conn = connect(args.replica_host, args.replica_port, args.db_user, replica_password)
        execute(replica_conn, "SELECT 1;")
    except pymysql.err.MySQLError as exc:
        fail(f"cannot connect to replica: {exc}")
        return

    log("Checking replica health before touching anything")
    status_rows = execute(replica_conn, "SHOW REPLICA STATUS")
    if not status_rows:
        fail("SHOW REPLICA STATUS returned no rows -- is this host actually a replica?")
        return
    status = status_rows[0]
    io_running = status.get("Replica_IO_Running")
    sql_running = status.get("Replica_SQL_Running")
    if io_running != "Yes" or sql_running != "Yes":
        fail(f"replica is not healthy (IO={io_running} SQL={sql_running}) -- refusing to touch replication filters on an already-unhealthy replica")
        return
    original_ignore_db = status.get("Replicate_Ignore_DB") or ""
    log(f"Replica healthy. Current Replicate_Ignore_DB='{original_ignore_db}' (will be restored on exit)")

    filter_changed = False
    succeeded = False
    primary_test_conn: pymysql.connections.Connection | None = None

    def cleanup() -> None:
        if filter_changed:
            log("Restoring replica replication filter to original state")
            try:
                execute(replica_conn, "STOP REPLICA SQL_THREAD;")
                if original_ignore_db:
                    execute(replica_conn, f"CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = ({original_ignore_db});")
                else:
                    execute(replica_conn, "CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = ();")
                execute(replica_conn, "START REPLICA SQL_THREAD;")
            except pymysql.err.MySQLError as exc:
                print(f"warning: failed to restore replication filter: {exc}", file=sys.stderr)

        if args.keep:
            log(f"Leaving {TEST_DB} schema and {SYSBENCH_USER} user in place (--keep). Replication filter has still been restored.")
            return

        log(f"Dropping {TEST_DB} on both sides and the {SYSBENCH_USER} user")
        try:
            execute(primary_conn, f"DROP DATABASE IF EXISTS {TEST_DB};")
            execute(primary_conn, f"DROP USER IF EXISTS '{SYSBENCH_USER}'@'%';")
            execute(replica_conn, f"DROP DATABASE IF EXISTS {TEST_DB};")
        except pymysql.err.MySQLError as exc:
            print(f"warning: cleanup failed: {exc}", file=sys.stderr)

        if not succeeded:
            log("Run failed. Cleaned up test schema/filter, but see output above for what went wrong.")

    try:
        log(f"Stopping replica SQL thread and adding REPLICATE_IGNORE_DB filter for '{TEST_DB}' (IO thread keeps running -- no relay log gap)")
        execute(replica_conn, "STOP REPLICA SQL_THREAD;")
        combined_ignore_db = f"{original_ignore_db},{TEST_DB}" if original_ignore_db else TEST_DB
        execute(replica_conn, f"CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = ({combined_ignore_db});")
        execute(replica_conn, "START REPLICA SQL_THREAD;")
        filter_changed = True

        log(f"Creating {TEST_DB} independently on primary and replica (filter means primary's copy of the DDL is never applied on replica)")
        execute(primary_conn, f"CREATE DATABASE IF NOT EXISTS {TEST_DB};")
        execute(replica_conn, f"CREATE DATABASE IF NOT EXISTS {TEST_DB};")

        # REPLICATE_IGNORE_DB filters DDL (which is always statement-based,
        # even under binlog_format=ROW) by the connection's *default
        # database* (as set by USE), not by a schema-qualified table name
        # inside the statement -- so "ALTER TABLE nibbletest.sbtest1 ..."
        # issued on a connection with no default database is NOT filtered
        # and replicates straight to the replica, colliding with the column
        # already present from the mysqldump clone. DML (INSERT/UPDATE) is
        # filtered correctly by the row event's actual table, but a
        # dedicated database=TEST_DB connection is used for those too, to
        # not depend on binlog_format being ROW. This mirrors the bash/zsh
        # scripts' `mysql_primary "$TEST_DB" -e ...` (positional db arg sets
        # the default database).
        primary_test_conn = connect(args.primary_host, args.primary_port, args.db_user, primary_password, database=TEST_DB)

        log("Creating scoped sysbench user (mysql_native_password -- the sysbench container's old client can't do caching_sha2_password)")
        # Drop first: a prior run (esp. one left behind via --keep, or interrupted
        # before cleanup) may have left this user with a different password, and
        # CREATE USER IF NOT EXISTS is a no-op on the password when the user
        # already exists, which then makes the new sysbench call fail auth.
        execute(primary_conn, f"DROP USER IF EXISTS '{SYSBENCH_USER}'@'%';")
        execute(primary_conn, f"CREATE USER '{SYSBENCH_USER}'@'%' IDENTIFIED WITH mysql_native_password BY '{SYSBENCH_PASSWORD}';")
        execute(primary_conn, f"GRANT ALL PRIVILEGES ON {TEST_DB}.* TO '{SYSBENCH_USER}'@'%';")
        execute(primary_conn, "FLUSH PRIVILEGES;")

        log(f"Seeding {args.table_size} rows into {TEST_DB}.{TABLE_NAME} on primary via containerized sysbench")
        run_cmd(
            [
                "docker", "run", "--rm", "severalnines/sysbench", "sysbench", "oltp_read_write",
                "--mysql-host=host.docker.internal", f"--mysql-port={args.primary_port}",
                f"--mysql-user={SYSBENCH_USER}", f"--mysql-password={SYSBENCH_PASSWORD}",
                f"--mysql-db={TEST_DB}", "--tables=1", f"--table-size={args.table_size}", "prepare",
            ],
            check=True,
        )

        log("Adding last_updated column + index (not part of sysbench's schema)")
        execute(
            primary_test_conn,
            f"ALTER TABLE {TABLE_NAME} ADD COLUMN last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            f"ADD INDEX idx_last_updated (last_updated);",
        )

        log("Cloning schema+data to replica as MyISAM (this is the slow step for large --table-size)")
        dump_proc = subprocess.Popen(
            [
                "mysqldump", "-h", args.primary_host, "-P", str(args.primary_port), "-u", args.db_user,
                f"-p{primary_password}", "--single-transaction", "--set-gtid-purged=OFF", TEST_DB, TABLE_NAME,
            ],
            stdout=subprocess.PIPE,
        )
        sed_proc = subprocess.Popen(
            ["sed", "s/ENGINE=InnoDB/ENGINE=MyISAM/"],
            stdin=dump_proc.stdout,
            stdout=subprocess.PIPE,
        )
        assert dump_proc.stdout is not None
        dump_proc.stdout.close()
        mysql_load_proc = subprocess.Popen(
            ["mysql", "-h", args.replica_host, "-P", str(args.replica_port), "-u", args.db_user, f"-p{replica_password}", TEST_DB],
            stdin=sed_proc.stdout,
        )
        assert sed_proc.stdout is not None
        sed_proc.stdout.close()
        mysql_load_proc.communicate()
        dump_proc.wait()
        sed_proc.wait()
        if dump_proc.returncode != 0 or sed_proc.returncode != 0 or mysql_load_proc.returncode != 0:
            fail("mysqldump | sed | mysql clone pipeline failed")

        log("Confirming replica table engine is MyISAM")
        engine_rows = execute(
            replica_conn,
            f"SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='{TEST_DB}' AND TABLE_NAME='{TABLE_NAME}';",
        )
        engine = engine_rows[0]["ENGINE"] if engine_rows else None
        if engine != "MyISAM":
            fail(f"replica table engine is '{engine}', expected MyISAM")

        window_start = execute(primary_conn, "SELECT NOW() AS now;")[0]["now"]
        log(f"Drift window starts at {window_start}")

        log(f"Simulating drift: updating {args.drift_updates} existing rows on primary only (filter keeps these off the replica)")
        execute(
            primary_test_conn,
            f"UPDATE {TABLE_NAME} SET k = k + 1, last_updated = NOW() WHERE id <= {args.drift_updates};",
        )

        log(f"Simulating drift: inserting {args.drift_inserts} new rows on primary only")
        execute(
            primary_test_conn,
            f"INSERT INTO {TABLE_NAME} (k, c, pad, last_updated) "
            f"SELECT k, c, pad, NOW() FROM {TABLE_NAME} LIMIT {args.drift_inserts};",
        )

        # Pull the window end from the primary's own clock rather than letting
        # go-data-nibble default to the host's local time.Now(): if the MySQL
        # server runs in a different timezone than this host (containers here
        # run UTC), comparing a host-local default against last_updated values
        # stamped by the server's NOW() can put specified-time-end before
        # specified-time-begin. Add a 1-minute buffer on top -- this whole drift
        # simulation runs in well under a second, so an unbuffered "now" can
        # land in the same second as window_start, producing a zero-width
        # window (a real lagging replica would never have this problem; it's
        # an artifact of how fast this script runs).
        window_end = execute(primary_conn, "SELECT DATE_ADD(NOW(), INTERVAL 1 MINUTE) AS end_ts;")[0]["end_ts"]

        nibble_args = [
            str(nibble_bin),
            f"--source-db-host={args.primary_host}", f"--source-db-port={args.primary_port}",
            f"--source-db-user={args.db_user}", f"--source-db-password={primary_password}",
            f"--target-db-host={args.replica_host}", f"--target-db-port={args.replica_port}",
            f"--target-db-user={args.db_user}", f"--target-db-password={replica_password}",
            f"--tables={TEST_DB}.{TABLE_NAME}",
            "--specified-time-column=last_updated",
            f"--specified-time-begin={window_start}",
            f"--specified-time-end={window_end}",
            f"--time-range-per-step={args.time_range_per_step}",
            f"--batch-diffs={args.batch_diffs}",
            f"--max-iterations={args.max_iterations}",
        ]

        log("Dry run (no --execute) -- timing this shows how long one pass takes at this table size")
        dry_run_start = time.monotonic()
        dry_run = run_cmd(nibble_args, capture_output=True, text=True)
        dry_run_elapsed = time.monotonic() - dry_run_start
        dry_run_output = dry_run.stdout + dry_run.stderr
        print(dry_run_output)
        log(f"Dry run took {dry_run_elapsed:.0f}s")
        if "converged after" in dry_run_output:
            fail(
                f"dry run reported convergence with no statements applied, but we just injected "
                f"{args.drift_updates + args.drift_inserts} drifted rows -- the time window likely didn't cover "
                f"them (check window_start/window_end against last_updated values), not a real pass"
            )

        log("Executing (--execute) -- this applies REPLACE INTO statements to the replica")
        execute_start = time.monotonic()
        run_cmd(nibble_args + ["--execute"], check=True)
        execute_elapsed = time.monotonic() - execute_start
        log(f"Execute run took {execute_elapsed:.0f}s")

        log("Verifying convergence with one more dry run")
        verify = run_cmd(nibble_args, capture_output=True, text=True)
        verify_output = verify.stdout + verify.stderr
        print(verify_output)
        if "converged after" not in verify_output:
            fail("did not observe a 'converged after' log line on the verification pass -- table did not fully converge")

        succeeded = True
        log(
            f"PASS -- table converged. dry-run={dry_run_elapsed:.0f}s execute={execute_elapsed:.0f}s "
            f"table-size={args.table_size} drift={args.drift_updates + args.drift_inserts} rows "
            f"time-range-per-step={args.time_range_per_step} batch-diffs={args.batch_diffs}"
        )
    finally:
        cleanup()
        if primary_test_conn is not None:
            primary_test_conn.close()
        primary_conn.close()
        replica_conn.close()


if __name__ == "__main__":
    main()
