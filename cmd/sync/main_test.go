package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeTempSQL(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "sync.sql")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}
	return path
}

func TestParseSyncFile_ValidFile(t *testing.T) {
	path := writeTempSQL(t, `-- Sync SQL for db.users => db.users
-- Generated at: 2026-07-05 00:00:00
-- Total differences: source_only=2, target_only=0, modified=1

REPLACE INTO `+"`db`.`users`"+` (`+"`id`, `name`"+`) VALUES (1, 'alice');
REPLACE INTO `+"`db`.`users`"+` (`+"`id`, `name`"+`) VALUES (2, 'it''s bob');
REPLACE INTO `+"`db`.`orders`"+` (`+"`id`"+`) VALUES (7);

-- Total REPLACE INTO statements generated: 3
`)

	statements, err := parseSyncFile(path)
	if err != nil {
		t.Fatalf("parseSyncFile failed: %v", err)
	}
	if len(statements) != 3 {
		t.Fatalf("statements = %d, want 3", len(statements))
	}
	if statements[0].table != "`db`.`users`" {
		t.Errorf("statement 1 table = %q, want %q", statements[0].table, "`db`.`users`")
	}
	if statements[2].table != "`db`.`orders`" {
		t.Errorf("statement 3 table = %q, want %q", statements[2].table, "`db`.`orders`")
	}
	if statements[0].lineNumber != 5 {
		t.Errorf("statement 1 line = %d, want 5", statements[0].lineNumber)
	}
	if strings.HasSuffix(statements[0].sql, ";") {
		t.Error("trailing semicolon must be stripped before execution")
	}
}

func TestParseSyncFile_RejectsNonReplace(t *testing.T) {
	for _, stmt := range []string{
		"DROP TABLE users;",
		"DELETE FROM db.users WHERE id=1;",
		"UPDATE db.users SET name='x';",
		"INSERT INTO db.users VALUES (1);",
	} {
		path := writeTempSQL(t, stmt+"\n")
		if _, err := parseSyncFile(path); err == nil {
			t.Errorf("statement %q must be rejected", stmt)
		}
	}
}

func TestParseSyncFile_RejectsMissingSemicolon(t *testing.T) {
	path := writeTempSQL(t, "REPLACE INTO `db`.`t` (`id`) VALUES (1)\n")
	if _, err := parseSyncFile(path); err == nil {
		t.Error("statement without trailing semicolon must be rejected")
	}
}

func TestParseSyncFile_EmptyAndCommentsOnly(t *testing.T) {
	path := writeTempSQL(t, "-- header only\n\n-- another comment\n")
	statements, err := parseSyncFile(path)
	if err != nil {
		t.Fatalf("comments-only file should parse: %v", err)
	}
	if len(statements) != 0 {
		t.Errorf("statements = %d, want 0", len(statements))
	}
}

func TestParseSyncFile_MissingFile(t *testing.T) {
	if _, err := parseSyncFile(filepath.Join(t.TempDir(), "does-not-exist.sql")); err == nil {
		t.Error("missing file must return an error")
	}
}

func TestParseSyncFile_CaseInsensitiveReplace(t *testing.T) {
	path := writeTempSQL(t, "replace into `db`.`t` (`id`) VALUES (1);\n")
	statements, err := parseSyncFile(path)
	if err != nil {
		t.Fatalf("lowercase replace into should parse: %v", err)
	}
	if len(statements) != 1 {
		t.Errorf("statements = %d, want 1", len(statements))
	}
}
