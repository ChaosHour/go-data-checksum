package tracking

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
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

func NewJobTracker(trackingDB *sql.DB, sourceHost, targetHost string) (*JobTracker, error) {
	jobID := fmt.Sprintf("%s_%s_%d", sourceHost, targetHost, time.Now().Unix())

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

func (jt *JobTracker) StartTableComparison(sourceDB, sourceTable, targetDB, targetTable string) (int64, error) {
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

func (jt *JobTracker) RecordChunkComparison(comparisonID int64, chunkNumber int, rangeStart, rangeEnd interface{}, sourceChecksum, targetChecksum string, processingTime time.Duration) error {
	status := "equal"
	if sourceChecksum != targetChecksum {
		status = "different"
	}

	rangeStartJSON, _ := json.Marshal(rangeStart)
	rangeEndJSON, _ := json.Marshal(rangeEnd)

	_, err := jt.TrackingDB.Exec(`
        INSERT INTO chunk_comparisons 
        (comparison_id, chunk_number, range_start, range_end, status, source_checksum, target_checksum, processing_time_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `, comparisonID, chunkNumber, rangeStartJSON, rangeEndJSON, status, sourceChecksum, targetChecksum, int(processingTime.Milliseconds()))

	return err
}

func (jt *JobTracker) UpdateTableComparison(comparisonID int64, status string, sourceRowCount, targetRowCount int64, chunksEqual, chunksDifferent int) error {
	_, err := jt.TrackingDB.Exec(`
        UPDATE table_comparisons 
        SET status = ?, end_time = NOW(), source_row_count = ?, target_row_count = ?, 
            chunks_equal = ?, chunks_different = ?
        WHERE comparison_id = ?
    `, status, sourceRowCount, targetRowCount, chunksEqual, chunksDifferent, comparisonID)

	return err
}

func (jt *JobTracker) CompleteJob(totalTables, tablesEqual, tablesDifferent int) error {
	_, err := jt.TrackingDB.Exec(`
        UPDATE checksum_jobs 
        SET status = 'completed', end_time = NOW(), total_tables = ?, 
            tables_equal = ?, tables_different = ?
        WHERE job_id = ?
    `, totalTables, tablesEqual, tablesDifferent, jt.JobID)

	return err
}

// Resume functionality for large jobs
func (jt *JobTracker) GetPendingTables() ([]TableComparison, error) {
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

	return tables, nil
}
