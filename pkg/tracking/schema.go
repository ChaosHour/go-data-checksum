package tracking

import (
	"database/sql"
	_ "embed"
	"fmt"
	"strings"
)

//go:embed schema.sql
var schemaSQL string

// SplitSQLStatements splits a DDL script into individual statements on ';',
// stripping '--' comments and blank lines. The mysql driver executes one
// statement per Exec (multiStatements is not enabled in our DSNs).
func SplitSQLStatements(script string) []string {
	var cleaned []string
	for _, line := range strings.Split(script, "\n") {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		cleaned = append(cleaned, line)
	}

	var statements []string
	for _, stmt := range strings.Split(strings.Join(cleaned, "\n"), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}
	return statements
}

// EnsureSchema creates the tracking tables (IF NOT EXISTS) on db, which must
// already be connected to the tracking database.
func EnsureSchema(db *sql.DB) error {
	for _, stmt := range SplitSQLStatements(schemaSQL) {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("tracking schema statement failed: %w", err)
		}
	}
	return nil
}
