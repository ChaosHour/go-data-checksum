package tracking

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Status values shared by table_comparisons.status and chunk_comparisons.status.
const (
	StatusEqual     = "equal"
	StatusDifferent = "different"
	StatusError     = "error"
)

type JobTracker struct {
	TrackingDB *sql.DB
	JobID      string
}

type TableComparison struct {
	ComparisonID    int64
	SourceDatabase  string
	SourceTable     string
	TargetDatabase  string
	TargetTable     string
	Status          string
	ChunksProcessed int
	ChunksEqual     int
	ChunksDifferent int
}

type ChunkComparison struct {
	ChunkID          int64
	ComparisonID     int64
	ChunkNumber      int
	RangeStart       interface{}
	RangeEnd         interface{}
	Status           string
	SourceChecksum   string
	TargetChecksum   string
	ProcessingTimeMs int
}

// DifferenceDetail is one differing record destined for difference_details.
type DifferenceDetail struct {
	Type             string // 'missing_in_target' | 'extra_in_target' | 'data_mismatch'
	PrimaryKeyValues map[string]string
	SourceChecksum   string
	TargetChecksum   string
}

// TableStatus maps a finished table run onto the table_comparisons enum.
// An error takes precedence over the equality result.
func TableStatus(isEqual bool, err error) string {
	switch {
	case err != nil:
		return StatusError
	case isEqual:
		return StatusEqual
	default:
		return StatusDifferent
	}
}

// ChunkStatus maps a finished chunk onto the chunk_comparisons enum.
func ChunkStatus(isEqual bool, err error) string {
	return TableStatus(isEqual, err)
}

// generateJobID returns "<src>_<tgt>_<unix>_<rand8hex>", hosts truncated to 20
// chars each, so the result always fits job_id VARCHAR(64). The random suffix
// keeps two jobs for the same host pair within the same second distinct.
func generateJobID(sourceHost, targetHost string, now time.Time) string {
	truncate := func(s string) string {
		if len(s) > 20 {
			return s[:20]
		}
		return s
	}
	suffix := make([]byte, 4)
	if _, err := rand.Read(suffix); err != nil {
		// crypto/rand should never fail; nanosecond bits keep IDs distinct if it does
		nano := uint32(now.UnixNano())
		suffix = []byte{byte(nano >> 24), byte(nano >> 16), byte(nano >> 8), byte(nano)}
	}
	return fmt.Sprintf("%s_%s_%d_%s", truncate(sourceHost), truncate(targetHost), now.Unix(), hex.EncodeToString(suffix))
}

// nullableInt64 maps the "unknown" sentinel -1 to SQL NULL.
func nullableInt64(v int64) sql.NullInt64 {
	if v < 0 {
		return sql.NullInt64{}
	}
	return sql.NullInt64{Int64: v, Valid: true}
}

func nullableString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func NewJobTracker(trackingDB *sql.DB, sourceHost, targetHost string) (*JobTracker, error) {
	jobID := generateJobID(sourceHost, targetHost, time.Now())

	_, err := trackingDB.Exec(`
        INSERT INTO checksum_jobs (job_id, source_host, target_host)
        VALUES (?, ?, ?)
    `, jobID, sourceHost, targetHost)

	if err != nil {
		return nil, err
	}

	return &JobTracker{
		TrackingDB: trackingDB,
		JobID:      jobID,
	}, nil
}

// AttachJobTracker returns a tracker for an existing job, verifying it exists.
func AttachJobTracker(trackingDB *sql.DB, jobID string) (*JobTracker, error) {
	var status string
	err := trackingDB.QueryRow(`SELECT status FROM checksum_jobs WHERE job_id = ?`, jobID).Scan(&status)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("job %q not found in tracking database", jobID)
	}
	if err != nil {
		return nil, err
	}
	return &JobTracker{TrackingDB: trackingDB, JobID: jobID}, nil
}

func (jt *JobTracker) StartTableComparison(sourceDB, sourceTable, targetDB, targetTable string) (int64, error) {
	if jt == nil || jt.TrackingDB == nil {
		return 0, nil
	}
	result, err := jt.TrackingDB.Exec(`
        INSERT INTO table_comparisons
        (job_id, source_database, source_table, target_database, target_table, status, start_time)
        VALUES (?, ?, ?, ?, ?, 'running', NOW())
    `, jt.JobID, sourceDB, sourceTable, targetDB, targetTable)

	if err != nil {
		return 0, err
	}

	return result.LastInsertId()
}

// ReopenTableComparison flips an existing comparison back to 'running' with a
// fresh start_time (resume path).
func (jt *JobTracker) ReopenTableComparison(comparisonID int64) error {
	if jt == nil || jt.TrackingDB == nil {
		return nil
	}
	_, err := jt.TrackingDB.Exec(`
        UPDATE table_comparisons SET status = 'running', start_time = NOW(), end_time = NULL
        WHERE comparison_id = ?
    `, comparisonID)
	return err
}

func (jt *JobTracker) RecordChunkComparison(comparisonID int64, chunkNumber int, rangeStart, rangeEnd interface{}, sourceChecksum, targetChecksum, status string, processingTime time.Duration) error {
	if jt == nil || jt.TrackingDB == nil {
		return nil
	}
	// JSON columns reject []byte parameters (sent as binary), so pass strings
	rangeStartJSON, _ := json.Marshal(rangeStart)
	rangeEndJSON, _ := json.Marshal(rangeEnd)
	rangeStartStr, rangeEndStr := string(rangeStartJSON), string(rangeEndJSON)

	_, err := jt.TrackingDB.Exec(`
        INSERT INTO chunk_comparisons
        (comparison_id, chunk_number, range_start, range_end, status, source_checksum, target_checksum, processing_time_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `, comparisonID, chunkNumber, rangeStartStr, rangeEndStr, status, nullableString(sourceChecksum), nullableString(targetChecksum), int(processingTime.Milliseconds()))

	return err
}

func (jt *JobTracker) UpdateTableComparison(comparisonID int64, status string, sourceRowCount, targetRowCount int64, chunksProcessed, chunksEqual, chunksDifferent int, errorMessage string) error {
	if jt == nil || jt.TrackingDB == nil {
		return nil
	}
	_, err := jt.TrackingDB.Exec(`
        UPDATE table_comparisons
        SET status = ?, end_time = NOW(), source_row_count = ?, target_row_count = ?,
            chunks_processed = ?, chunks_equal = ?, chunks_different = ?, error_message = ?
        WHERE comparison_id = ?
    `, status, nullableInt64(sourceRowCount), nullableInt64(targetRowCount), chunksProcessed, chunksEqual, chunksDifferent, nullableString(errorMessage), comparisonID)

	return err
}

func (jt *JobTracker) CompleteJob(totalTables, tablesEqual, tablesDifferent int) error {
	if jt == nil || jt.TrackingDB == nil {
		return nil
	}
	_, err := jt.TrackingDB.Exec(`
        UPDATE checksum_jobs
        SET status = 'completed', end_time = NOW(), total_tables = ?, tables_processed = ?,
            tables_equal = ?, tables_different = ?
        WHERE job_id = ?
    `, totalTables, totalTables, tablesEqual, tablesDifferent, jt.JobID)

	return err
}

// CompleteJobFromTables finalizes the job by aggregating table_comparisons.
// Used on resume, where in-memory counters do not cover the whole job.
func (jt *JobTracker) CompleteJobFromTables() error {
	if jt == nil || jt.TrackingDB == nil {
		return nil
	}
	_, err := jt.TrackingDB.Exec(`
        UPDATE checksum_jobs j
        JOIN (
            SELECT job_id,
                   COUNT(*) AS total,
                   SUM(status = 'equal') AS equal_cnt,
                   SUM(status IN ('different', 'error')) AS different_cnt
            FROM table_comparisons
            WHERE job_id = ?
            GROUP BY job_id
        ) t ON t.job_id = j.job_id
        SET j.status = 'completed', j.end_time = NOW(), j.total_tables = t.total,
            j.tables_processed = t.total, j.tables_equal = t.equal_cnt, j.tables_different = t.different_cnt
    `, jt.JobID)
	return err
}

// RecordDifferenceDetails persists per-record differences. It inserts one
// synthetic chunk_comparisons row (chunk_number -1) to satisfy the
// difference_details FK, then batch-inserts the details under it.
func (jt *JobTracker) RecordDifferenceDetails(comparisonID int64, details []DifferenceDetail) error {
	if jt == nil || jt.TrackingDB == nil || len(details) == 0 {
		return nil
	}
	analysisRangeJSON, _ := json.Marshal(map[string]string{"analysis": "full-table"})
	analysisRange := string(analysisRangeJSON)
	result, err := jt.TrackingDB.Exec(`
        INSERT INTO chunk_comparisons
        (comparison_id, chunk_number, range_start, range_end, status, source_checksum, target_checksum, processing_time_ms)
        VALUES (?, -1, ?, ?, 'different', NULL, NULL, 0)
    `, comparisonID, analysisRange, analysisRange)
	if err != nil {
		return err
	}
	chunkID, err := result.LastInsertId()
	if err != nil {
		return err
	}
	for _, d := range details {
		pkJSON, _ := json.Marshal(d.PrimaryKeyValues)
		if _, err := jt.TrackingDB.Exec(`
            INSERT INTO difference_details
            (chunk_id, difference_type, primary_key_values, source_checksum, target_checksum)
            VALUES (?, ?, ?, ?, ?)
        `, chunkID, d.Type, string(pkJSON), nullableString(d.SourceChecksum), nullableString(d.TargetChecksum)); err != nil {
			return err
		}
	}
	return nil
}

// Resume functionality for large jobs
func (jt *JobTracker) GetPendingTables() ([]TableComparison, error) {
	if jt == nil || jt.TrackingDB == nil {
		return nil, nil
	}
	rows, err := jt.TrackingDB.Query(`
        SELECT comparison_id, source_database, source_table, target_database, target_table
        FROM table_comparisons
        WHERE job_id = ? AND status IN ('pending', 'running')
        ORDER BY comparison_id
    `, jt.JobID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []TableComparison
	for rows.Next() {
		var tc TableComparison
		err := rows.Scan(&tc.ComparisonID, &tc.SourceDatabase, &tc.SourceTable,
			&tc.TargetDatabase, &tc.TargetTable)
		if err != nil {
			return nil, err
		}
		tables = append(tables, tc)
	}

	return tables, rows.Err()
}
