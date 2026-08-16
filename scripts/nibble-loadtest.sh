#!/usr/bin/env bash
# nibble-loadtest.sh -- create a synthetic large-table drift scenario against
# your existing primary/replica MySQL containers and run go-data-nibble
# against it, so its behavior at scale (timing, chunking, convergence) can be
# observed before pointing it at a real multi-hundred-GB MyISAM table.
#
# This reuses your already-running primary/replica containers (defaults:
# 127.0.0.1:3306 primary, 127.0.0.1:3307 replica -- matches the
# client_primary1/client_replica1 groups in ~/.my.cnf) instead of spinning up
# throwaway ones. It does NOT touch any of your existing schemas or their
# replication. What it does:
#
#   1. Checks the replica is healthy (IO+SQL threads running) before touching
#      anything, and records the current replication filter state so it can
#      be restored exactly afterward.
#   2. Stops only the replica SQL thread (not IO -- no relay log gap) and adds
#      a REPLICATE_IGNORE_DB filter for a dedicated `nibbletest` schema, so
#      writes to that schema on the primary are never auto-applied to the
#      replica. Restarts the SQL thread. This is what makes injected drift
#      possible without disrupting your real replicated databases.
#   3. Creates `nibbletest` independently on both sides, seeds a large table
#      on the primary via a containerized sysbench (severalnines/sysbench --
#      nothing installed on your host), clones it to the replica as MyISAM
#      via mysqldump.
#   4. Updates/inserts a batch of rows on the primary only, inside a known
#      time window -- because of the filter from step 2, these never reach
#      the replica, simulating a replica that fell behind.
#   5. Runs go-data-nibble dry-run, then --execute, timing each pass.
#   6. Re-runs dry-run once more and confirms convergence.
#   7. Cleanup: always drops `nibbletest` on both sides and the scoped
#      sysbench DB user, and ALWAYS restores the replication filter to
#      exactly what it was before (even on failure/Ctrl-C) -- your real
#      replication topology is never left modified.
#
# Requirements: docker (for containerized sysbench), and local `mysql` /
# `mysqldump` clients. A `bin/go-data-nibble` build (built automatically via
# `make build` if missing).
#
# DB credentials: reads PRIMARY_DB_PASSWORD from the environment, or falls
# back to the `password=` line under [client] in ~/.my.cnf. Never printed.
#
# Usage:
#   scripts/nibble-loadtest.sh [options]
#
# Options:
#   --primary-host HOST      (default: 127.0.0.1)
#   --primary-port PORT      (default: 3306)
#   --replica-host HOST      (default: 127.0.0.1)
#   --replica-port PORT      (default: 3307)
#   --db-user USER           (default: root)
#   --table-size N           Rows to seed via sysbench (default: 2000000)
#   --drift-updates N        Existing rows to update only on primary (default: 50000)
#   --drift-inserts N        New rows to insert only on primary (default: 5000)
#   --time-range-per-step D  Nibble step duration, eg 15m/1h (default: 15m)
#   --batch-diffs N          --batch-diffs passed to go-data-nibble (default: 5000)
#   --max-iterations N       --max-iterations passed to go-data-nibble (default: 50)
#   --keep                   Don't drop the nibbletest schema/user on success
#                            (replication filter is ALWAYS restored regardless)
#   -h, --help               Show this help

set -euo pipefail

PRIMARY_HOST="127.0.0.1"
PRIMARY_PORT=3306
REPLICA_HOST="127.0.0.1"
REPLICA_PORT=3307
DB_USER="root"
TABLE_SIZE=2000000
DRIFT_UPDATES=50000
DRIFT_INSERTS=5000
TIME_RANGE_PER_STEP="15m"
BATCH_DIFFS=5000
MAX_ITERATIONS=50
KEEP=0

TEST_DB="nibbletest"
TABLE_NAME="sbtest1"
SYSBENCH_USER="sysbench_lt"
SYSBENCH_PASSWORD="sysbench_lt_$$"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --primary-host) PRIMARY_HOST="$2"; shift 2 ;;
    --primary-port) PRIMARY_PORT="$2"; shift 2 ;;
    --replica-host) REPLICA_HOST="$2"; shift 2 ;;
    --replica-port) REPLICA_PORT="$2"; shift 2 ;;
    --db-user) DB_USER="$2"; shift 2 ;;
    --table-size) TABLE_SIZE="$2"; shift 2 ;;
    --drift-updates) DRIFT_UPDATES="$2"; shift 2 ;;
    --drift-inserts) DRIFT_INSERTS="$2"; shift 2 ;;
    --time-range-per-step) TIME_RANGE_PER_STEP="$2"; shift 2 ;;
    --batch-diffs) BATCH_DIFFS="$2"; shift 2 ;;
    --max-iterations) MAX_ITERATIONS="$2"; shift 2 ;;
    --keep) KEEP=1; shift ;;
    -h|--help) grep -E '^#( |$)' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

log() { printf '\n\033[1;34m==>\033[0m %s\n' "$1"; }
fail() { printf '\n\033[1;31mFAIL:\033[0m %s\n' "$1" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

: "${PRIMARY_DB_PASSWORD:=}"
if [[ -z "$PRIMARY_DB_PASSWORD" ]]; then
  PRIMARY_DB_PASSWORD=$(awk '
    /^\[client\]/ { inclient=1; next }
    /^\[/ { inclient=0 }
    inclient && /^[[:space:]]*password[[:space:]]*=/ {
      sub(/^[[:space:]]*password[[:space:]]*=[[:space:]]*/, "");
      print;
      exit
    }' "$HOME/.my.cnf" 2>/dev/null || true)
fi
[[ -n "$PRIMARY_DB_PASSWORD" ]] || fail "no password found -- set PRIMARY_DB_PASSWORD or add [client]/password to ~/.my.cnf"
REPLICA_DB_PASSWORD="$PRIMARY_DB_PASSWORD"

mysql_primary() { mysql -h"$PRIMARY_HOST" -P"$PRIMARY_PORT" -u"$DB_USER" -p"$PRIMARY_DB_PASSWORD" "$@"; }
mysql_replica() { mysql -h"$REPLICA_HOST" -P"$REPLICA_PORT" -u"$DB_USER" -p"$REPLICA_DB_PASSWORD" "$@"; }

command -v docker >/dev/null 2>&1 || fail "docker is required but not found in PATH"
command -v mysql >/dev/null 2>&1 || fail "mysql client is required but not found in PATH"
command -v mysqldump >/dev/null 2>&1 || fail "mysqldump is required but not found in PATH"

if [[ ! -x "$REPO_ROOT/bin/go-data-nibble" ]]; then
  log "bin/go-data-nibble not found, building"
  (cd "$REPO_ROOT" && make build)
fi

log "Checking connectivity to primary ($PRIMARY_HOST:$PRIMARY_PORT) and replica ($REPLICA_HOST:$REPLICA_PORT)"
mysql_primary -e "SELECT 1;" >/dev/null || fail "cannot connect to primary"
mysql_replica -e "SELECT 1;" >/dev/null || fail "cannot connect to replica"

log "Checking replica health before touching anything"
STATUS_FILE=$(mktemp)
mysql_replica -e "SHOW REPLICA STATUS" > "$STATUS_FILE"
get_status_field() { awk -F'\t' -v col="$1" 'NR==1{for(i=1;i<=NF;i++) if($i==col) c=i} NR==2{print $c}' "$STATUS_FILE"; }
IO_RUNNING=$(get_status_field "Replica_IO_Running")
SQL_RUNNING=$(get_status_field "Replica_SQL_Running")
[[ "$IO_RUNNING" == "Yes" && "$SQL_RUNNING" == "Yes" ]] || fail "replica is not healthy (IO=$IO_RUNNING SQL=$SQL_RUNNING) -- refusing to touch replication filters on an already-unhealthy replica"
ORIGINAL_IGNORE_DB=$(get_status_field "Replicate_Ignore_DB")
log "Replica healthy. Current Replicate_Ignore_DB='$ORIGINAL_IGNORE_DB' (will be restored on exit)"
rm -f "$STATUS_FILE"

FILTER_CHANGED=0
SUCCEEDED=0
cleanup() {
  if [[ "$FILTER_CHANGED" -eq 1 ]]; then
    log "Restoring replica replication filter to original state"
    mysql_replica -e "STOP REPLICA SQL_THREAD;" || true
    if [[ -n "$ORIGINAL_IGNORE_DB" ]]; then
      mysql_replica -e "CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = ($ORIGINAL_IGNORE_DB);" || true
    else
      mysql_replica -e "CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = ();" || true
    fi
    mysql_replica -e "START REPLICA SQL_THREAD;" || true
  fi

  if [[ "$KEEP" -eq 1 ]]; then
    log "Leaving $TEST_DB schema and $SYSBENCH_USER user in place (--keep). Replication filter has still been restored."
    return
  fi

  log "Dropping $TEST_DB on both sides and the $SYSBENCH_USER user"
  mysql_primary -e "DROP DATABASE IF EXISTS $TEST_DB; DROP USER IF EXISTS '$SYSBENCH_USER'@'%';" || true
  mysql_replica -e "DROP DATABASE IF EXISTS $TEST_DB;" || true

  if [[ "$SUCCEEDED" -eq 0 ]]; then
    log "Run failed. Cleaned up test schema/filter, but see output above for what went wrong."
  fi
}
trap cleanup EXIT

log "Stopping replica SQL thread and adding REPLICATE_IGNORE_DB filter for '$TEST_DB' (IO thread keeps running -- no relay log gap)"
mysql_replica -e "STOP REPLICA SQL_THREAD;"
COMBINED_IGNORE_DB="$TEST_DB"
[[ -n "$ORIGINAL_IGNORE_DB" ]] && COMBINED_IGNORE_DB="$ORIGINAL_IGNORE_DB,$TEST_DB"
mysql_replica -e "CHANGE REPLICATION FILTER REPLICATE_IGNORE_DB = ($COMBINED_IGNORE_DB);"
mysql_replica -e "START REPLICA SQL_THREAD;"
FILTER_CHANGED=1

log "Creating $TEST_DB independently on primary and replica (filter means primary's copy of the DDL is never applied on replica)"
mysql_primary -e "CREATE DATABASE IF NOT EXISTS $TEST_DB;"
mysql_replica -e "CREATE DATABASE IF NOT EXISTS $TEST_DB;"

log "Creating scoped sysbench user (mysql_native_password -- the sysbench container's old client can't do caching_sha2_password)"
# Drop first: a prior run (esp. one left behind via --keep, or interrupted
# before cleanup) may have left this user with a different password, and
# CREATE USER IF NOT EXISTS is a no-op on the password when the user already
# exists, which then makes the new sysbench call fail auth.
mysql_primary -e "
  DROP USER IF EXISTS '$SYSBENCH_USER'@'%';
  CREATE USER '$SYSBENCH_USER'@'%' IDENTIFIED WITH mysql_native_password BY '$SYSBENCH_PASSWORD';
  GRANT ALL PRIVILEGES ON $TEST_DB.* TO '$SYSBENCH_USER'@'%';
  FLUSH PRIVILEGES;
"

log "Seeding $TABLE_SIZE rows into $TEST_DB.$TABLE_NAME on primary via containerized sysbench"
docker run --rm severalnines/sysbench sysbench oltp_read_write \
  --mysql-host=host.docker.internal --mysql-port="$PRIMARY_PORT" \
  --mysql-user="$SYSBENCH_USER" --mysql-password="$SYSBENCH_PASSWORD" \
  --mysql-db="$TEST_DB" --tables=1 --table-size="$TABLE_SIZE" prepare

log "Adding last_updated column + index (not part of sysbench's schema)"
mysql_primary "$TEST_DB" -e \
  "ALTER TABLE $TABLE_NAME ADD COLUMN last_updated DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, ADD INDEX idx_last_updated (last_updated);"

log "Cloning schema+data to replica as MyISAM (this is the slow step for large --table-size)"
mysqldump -h"$PRIMARY_HOST" -P"$PRIMARY_PORT" -u"$DB_USER" -p"$PRIMARY_DB_PASSWORD" \
  --single-transaction --set-gtid-purged=OFF "$TEST_DB" "$TABLE_NAME" \
  | sed 's/ENGINE=InnoDB/ENGINE=MyISAM/' \
  | mysql_replica "$TEST_DB"

log "Confirming replica table engine is MyISAM"
ENGINE=$(mysql_replica -N -e "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA='$TEST_DB' AND TABLE_NAME='$TABLE_NAME';")
[[ "$ENGINE" == "MyISAM" ]] || fail "replica table engine is '$ENGINE', expected MyISAM"

WINDOW_START=$(mysql_primary -N -e "SELECT NOW();")
log "Drift window starts at $WINDOW_START"

log "Simulating drift: updating $DRIFT_UPDATES existing rows on primary only (filter keeps these off the replica)"
mysql_primary "$TEST_DB" -e \
  "UPDATE $TABLE_NAME SET k = k + 1, last_updated = NOW() WHERE id <= $DRIFT_UPDATES;"

log "Simulating drift: inserting $DRIFT_INSERTS new rows on primary only"
mysql_primary "$TEST_DB" -e "
  INSERT INTO $TABLE_NAME (k, c, pad, last_updated)
  SELECT k, c, pad, NOW() FROM $TABLE_NAME LIMIT $DRIFT_INSERTS;
"

# Pull the window end from the primary's own clock rather than letting
# go-data-nibble default to the host's local time.Now(): if the MySQL server
# runs in a different timezone than this host (containers here run UTC),
# comparing a host-local default against last_updated values stamped by the
# server's NOW() can put specified-time-end before specified-time-begin. Add
# a 1-minute buffer on top -- this whole drift simulation runs in well under
# a second, so an unbuffered "now" can land in the same second as
# WINDOW_START, producing a zero-width window (a real lagging replica would
# never have this problem; it's an artifact of how fast this script runs).
WINDOW_END=$(mysql_primary -N -e "SELECT DATE_ADD(NOW(), INTERVAL 1 MINUTE);")

NIBBLE_ARGS=(
  --source-db-host="$PRIMARY_HOST" --source-db-port="$PRIMARY_PORT" --source-db-user="$DB_USER" --source-db-password="$PRIMARY_DB_PASSWORD"
  --target-db-host="$REPLICA_HOST" --target-db-port="$REPLICA_PORT" --target-db-user="$DB_USER" --target-db-password="$REPLICA_DB_PASSWORD"
  --tables="$TEST_DB.$TABLE_NAME"
  --specified-time-column=last_updated
  --specified-time-begin="$WINDOW_START"
  --specified-time-end="$WINDOW_END"
  --time-range-per-step="$TIME_RANGE_PER_STEP"
  --batch-diffs="$BATCH_DIFFS"
  --max-iterations="$MAX_ITERATIONS"
)

log "Dry run (no --execute) -- timing this shows how long one pass takes at this table size"
DRY_RUN_START=$(date +%s)
DRY_RUN_OUTPUT=$("$REPO_ROOT/bin/go-data-nibble" "${NIBBLE_ARGS[@]}" 2>&1)
DRY_RUN_ELAPSED=$(( $(date +%s) - DRY_RUN_START ))
echo "$DRY_RUN_OUTPUT"
log "Dry run took ${DRY_RUN_ELAPSED}s"
if echo "$DRY_RUN_OUTPUT" | grep -q "converged after"; then
  fail "dry run reported convergence with no statements applied, but we just injected $((DRIFT_UPDATES + DRIFT_INSERTS)) drifted rows -- the time window likely didn't cover them (check WINDOW_START/WINDOW_END against last_updated values), not a real pass"
fi

log "Executing (--execute) -- this applies REPLACE INTO statements to the replica"
EXECUTE_START=$(date +%s)
"$REPO_ROOT/bin/go-data-nibble" "${NIBBLE_ARGS[@]}" --execute
EXECUTE_ELAPSED=$(( $(date +%s) - EXECUTE_START ))
log "Execute run took ${EXECUTE_ELAPSED}s"

log "Verifying convergence with one more dry run"
VERIFY_OUTPUT=$("$REPO_ROOT/bin/go-data-nibble" "${NIBBLE_ARGS[@]}" 2>&1)
echo "$VERIFY_OUTPUT"
if ! echo "$VERIFY_OUTPUT" | grep -q "converged after"; then
  fail "did not observe a 'converged after' log line on the verification pass -- table did not fully converge"
fi

SUCCEEDED=1
log "PASS -- table converged. dry-run=${DRY_RUN_ELAPSED}s execute=${EXECUTE_ELAPSED}s table-size=${TABLE_SIZE} drift=$((DRIFT_UPDATES + DRIFT_INSERTS)) rows time-range-per-step=${TIME_RANGE_PER_STEP} batch-diffs=${BATCH_DIFFS}"
