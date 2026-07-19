package tracking

import (
	"strings"
	"testing"
	"time"
)

func TestGenerateJobID(t *testing.T) {
	now := time.Now()
	longHost := strings.Repeat("a", 80) + ":3306"
	jobID := generateJobID(longHost, longHost, now)
	if len(jobID) > 64 {
		t.Errorf("job_id %q is %d chars, must fit VARCHAR(64)", jobID, len(jobID))
	}
	if !strings.HasPrefix(jobID, strings.Repeat("a", 20)+"_") {
		t.Errorf("job_id %q should start with the truncated source host", jobID)
	}

	// Two IDs generated within the same second must differ
	first := generateJobID("src:3306", "tgt:3307", now)
	second := generateJobID("src:3306", "tgt:3307", now)
	if first == second {
		t.Errorf("job_ids generated in the same second collide: %q", first)
	}
}

func TestSplitSQLStatements(t *testing.T) {
	statements := SplitSQLStatements(schemaSQL)
	if len(statements) != 4 {
		t.Fatalf("embedded schema should split into 4 statements, got %d", len(statements))
	}
	for i, stmt := range statements {
		if !strings.HasPrefix(stmt, "CREATE TABLE IF NOT EXISTS") {
			t.Errorf("statement %d should be CREATE TABLE IF NOT EXISTS, got %q", i, stmt[:40])
		}
		if strings.Contains(stmt, "--") {
			t.Errorf("statement %d still contains a comment: %q", i, stmt)
		}
	}

	script := "-- comment only\nSELECT 1;\n\n  \nSELECT 2;\n-- trailing"
	got := SplitSQLStatements(script)
	if len(got) != 2 || got[0] != "SELECT 1" || got[1] != "SELECT 2" {
		t.Errorf("unexpected split result: %#v", got)
	}
}

func TestTableStatusAndChunkStatus(t *testing.T) {
	someErr := &testError{}
	cases := []struct {
		isEqual bool
		err     error
		want    string
	}{
		{true, nil, StatusEqual},
		{false, nil, StatusDifferent},
		{true, someErr, StatusError}, // error takes precedence over equality
		{false, someErr, StatusError},
	}
	for _, c := range cases {
		if got := TableStatus(c.isEqual, c.err); got != c.want {
			t.Errorf("TableStatus(%v, %v) = %q, want %q", c.isEqual, c.err, got, c.want)
		}
		if got := ChunkStatus(c.isEqual, c.err); got != c.want {
			t.Errorf("ChunkStatus(%v, %v) = %q, want %q", c.isEqual, c.err, got, c.want)
		}
	}
}

type testError struct{}

func (e *testError) Error() string { return "test error" }

func TestNullableInt64(t *testing.T) {
	if v := nullableInt64(-1); v.Valid {
		t.Errorf("nullableInt64(-1) should be NULL")
	}
	if v := nullableInt64(0); !v.Valid || v.Int64 != 0 {
		t.Errorf("nullableInt64(0) should be valid 0, got %+v", v)
	}
	if v := nullableInt64(42); !v.Valid || v.Int64 != 42 {
		t.Errorf("nullableInt64(42) should be valid 42, got %+v", v)
	}
}

func TestNullableString(t *testing.T) {
	if v := nullableString(""); v.Valid {
		t.Errorf("nullableString(\"\") should be NULL")
	}
	if v := nullableString("x"); !v.Valid || v.String != "x" {
		t.Errorf("nullableString(\"x\") should be valid, got %+v", v)
	}
}

// Every tracker method must be a safe no-op on a nil receiver or nil DB, so
// call sites in the checksum flow can be unconditional.
func TestJobTrackerNilSafe(t *testing.T) {
	trackers := []*JobTracker{nil, {}}
	for _, jt := range trackers {
		if id, err := jt.StartTableComparison("db", "t", "db", "t"); id != 0 || err != nil {
			t.Errorf("StartTableComparison on %+v: got (%d, %v)", jt, id, err)
		}
		if err := jt.ReopenTableComparison(1); err != nil {
			t.Errorf("ReopenTableComparison: %v", err)
		}
		if err := jt.RecordChunkComparison(1, 0, nil, nil, "", "", StatusEqual, time.Second); err != nil {
			t.Errorf("RecordChunkComparison: %v", err)
		}
		if err := jt.UpdateTableComparison(1, StatusEqual, -1, -1, 0, 0, 0, ""); err != nil {
			t.Errorf("UpdateTableComparison: %v", err)
		}
		if err := jt.CompleteJob(1, 1, 0); err != nil {
			t.Errorf("CompleteJob: %v", err)
		}
		if err := jt.CompleteJobFromTables(); err != nil {
			t.Errorf("CompleteJobFromTables: %v", err)
		}
		if err := jt.RecordDifferenceDetails(1, []DifferenceDetail{{Type: "data_mismatch"}}); err != nil {
			t.Errorf("RecordDifferenceDetails: %v", err)
		}
		if tables, err := jt.GetPendingTables(); tables != nil || err != nil {
			t.Errorf("GetPendingTables: got (%v, %v)", tables, err)
		}
	}
}
