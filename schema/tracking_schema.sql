CREATE DATABASE IF NOT EXISTS data_checksum_tracking;
USE data_checksum_tracking;

-- Main job tracking table
CREATE TABLE checksum_jobs (
    job_id VARCHAR(64) PRIMARY KEY,
    source_host VARCHAR(255) NOT NULL,
    target_host VARCHAR(255) NOT NULL,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    status ENUM('running', 'completed', 'failed', 'paused') DEFAULT 'running',
    total_tables INT DEFAULT 0,
    tables_processed INT DEFAULT 0,
    tables_equal INT DEFAULT 0,
    tables_different INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Individual table comparison results
CREATE TABLE table_comparisons (
    comparison_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(64) NOT NULL,
    source_database VARCHAR(64) NOT NULL,
    source_table VARCHAR(64) NOT NULL,
    target_database VARCHAR(64) NOT NULL,
    target_table VARCHAR(64) NOT NULL,
    status ENUM('pending', 'running', 'equal', 'different', 'error') DEFAULT 'pending',
    start_time TIMESTAMP NULL,
    end_time TIMESTAMP NULL,
    source_row_count BIGINT NULL,
    target_row_count BIGINT NULL,
    chunks_processed INT DEFAULT 0,
    chunks_equal INT DEFAULT 0,
    chunks_different INT DEFAULT 0,
    error_message TEXT NULL,
    processing_speed_rows_per_sec INT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES checksum_jobs(job_id),
    INDEX idx_job_status (job_id, status),
    INDEX idx_table_lookup (source_database, source_table)
);

-- Chunk-level tracking for detailed analysis
CREATE TABLE chunk_comparisons (
    chunk_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    comparison_id BIGINT NOT NULL,
    chunk_number INT NOT NULL,
    range_start JSON NOT NULL,  -- Store the key range as JSON
    range_end JSON NOT NULL,
    status ENUM('equal', 'different', 'error') NOT NULL,
    source_checksum VARCHAR(64) NULL,
    target_checksum VARCHAR(64) NULL,
    row_count_estimate INT NULL,
    processing_time_ms INT NULL,
    error_message TEXT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (comparison_id) REFERENCES table_comparisons(comparison_id),
    INDEX idx_comparison_chunk (comparison_id, chunk_number),
    INDEX idx_status (status)
);

-- Detailed differences for investigation
CREATE TABLE difference_details (
    detail_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    chunk_id BIGINT NOT NULL,
    difference_type ENUM('missing_in_target', 'extra_in_target', 'data_mismatch') NOT NULL,
    primary_key_values JSON NOT NULL,
    source_checksum VARCHAR(64) NULL,
    target_checksum VARCHAR(64) NULL,
    sample_data JSON NULL,  -- Store a sample of the differing data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (chunk_id) REFERENCES chunk_comparisons(chunk_id),
    INDEX idx_chunk_type (chunk_id, difference_type)
);
