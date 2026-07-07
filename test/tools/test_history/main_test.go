package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadJUnitHistoryWithMetadataAndMatrix(t *testing.T) {
	dir := t.TempDir()
	runDir := filepath.Join(dir, "run-123")
	reportDir := filepath.Join(runDir, "test-reports-regress-jammy-16")
	if err := os.MkdirAll(reportDir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(runDir, "run-metadata.json"), `{
  "databaseId": 123,
  "url": "https://github.test/runs/123",
  "headSha": "abcdef123456",
  "headBranch": "master",
  "createdAt": "2026-07-07T10:00:00Z",
  "workflowName": "tests"
}`)
	writeFile(t, filepath.Join(reportDir, "router.xml"), `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="router-regress_router-6432" tests="2" failures="1">
  <testcase classname="router-regress_router-6432" name="routing_hint" time="0.25"/>
  <testcase classname="router-regress_router-6432" name="autoprotect_2pc">
    <failure message="regression diff">--- /expected/autoprotect_2pc.out	2026-07-07 10:00:00 +0000
+++ /results/autoprotect_2pc.out	2026-07-07 10:00:01 +0000
+ERROR: boom</failure>
  </testcase>
</testsuite>`)

	records, err := readInputDir(dir, runMetadata{})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	failed := records[1]
	if failed.Suite != "router-regress_router-6432" || failed.Name != "autoprotect_2pc" {
		t.Fatalf("unexpected failed test identity: %#v", failed)
	}
	if failed.Matrix != "jammy-16" {
		t.Fatalf("expected matrix jammy-16, got %q", failed.Matrix)
	}
	if failed.RunID != "123" || failed.RunURL == "" || failed.CommitSHA != "abcdef123456" {
		t.Fatalf("metadata was not attached: %#v", failed)
	}
	if failed.Fingerprint == "" {
		t.Fatalf("failed record has no fingerprint: %#v", failed)
	}
}

func TestBuildCandidatesFindsRepeatedFingerprintAndLastKnownPass(t *testing.T) {
	records := []historyRecord{
		{
			Suite:     "router",
			Name:      "autoprotect_2pc",
			Matrix:    "jammy-16",
			Status:    "passed",
			Timestamp: "2026-07-01T00:00:00Z",
		},
		failureRecord("1", "2026-07-02T00:00:00Z", "--- /expected/a.out\t2026-07-02 10:00:00 +0000\n+++ /results/a.out\t2026-07-02 10:00:01 +0000\n+ERROR: boom"),
		failureRecord("2", "2026-07-03T00:00:00Z", "--- /expected/a.out\t2026-07-03 11:00:00 +0000\n+++ /results/a.out\t2026-07-03 11:00:01 +0000\n+ERROR: boom"),
	}
	for i := range records {
		if isFailure(records[i].Status) {
			records[i].Fingerprint = failureFingerprint(records[i])
		}
	}

	candidates := buildCandidates(records, 2, 5)
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	candidate := candidates[0]
	if candidate.Failures != 2 || candidate.TestRuns != 3 {
		t.Fatalf("unexpected counts: %#v", candidate)
	}
	if candidate.LastKnownPass != "2026-07-01T00:00:00Z" {
		t.Fatalf("unexpected last known pass: %q", candidate.LastKnownPass)
	}
	if len(candidate.Evidence) != 2 {
		t.Fatalf("unexpected evidence count: %d", len(candidate.Evidence))
	}
}

func TestBuildCandidatesComputesMissingFailureFingerprint(t *testing.T) {
	records := []historyRecord{
		failureRecord("1", "2026-07-02T00:00:00Z", "+ERROR: boom"),
		failureRecord("2", "2026-07-03T00:00:00Z", "+ERROR: boom"),
	}

	candidates := buildCandidates(records, 2, 5)
	if len(candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %d", len(candidates))
	}
	if candidates[0].Fingerprint == "" {
		t.Fatalf("expected computed fingerprint: %#v", candidates[0])
	}
}

func TestReadCTRFObject(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "ctrf-report.json"), `{
  "reportFormat": "CTRF",
  "timestamp": "2026-07-07T10:00:00Z",
  "results": {
    "tests": [
      {
        "name": "router-regress_router-6432: autoprotect_2pc",
        "status": "failed",
        "suite": ["router-regress_router-6432"],
        "message": "regression diff",
        "trace": "+ERROR: boom"
      }
    ]
  }
}`)

	records, err := readInputDir(dir, runMetadata{RunID: "99"})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Suite != "router-regress_router-6432" || records[0].Name != "autoprotect_2pc" {
		t.Fatalf("unexpected CTRF identity: %#v", records[0])
	}
	if records[0].RunID != "99" || records[0].Timestamp != "2026-07-07T10:00:00Z" {
		t.Fatalf("unexpected CTRF metadata: %#v", records[0])
	}
}

func TestTimestampFromReportPathOnlyAcceptsRFC3339(t *testing.T) {
	if got := timestampFromReportPath("downloaded/run-123/report.xml"); got != "" {
		t.Fatalf("expected run id fallback to be ignored, got %q", got)
	}
	if got := timestampFromReportPath("downloaded/run-2026-07-07T10:00:00Z/report.xml"); got != "2026-07-07T10:00:00Z" {
		t.Fatalf("expected RFC3339 timestamp fallback, got %q", got)
	}
}

func TestTimestampLessSortsUnknownLast(t *testing.T) {
	if timestampLess("", "2026-07-07T10:00:00Z") {
		t.Fatal("empty timestamp sorted before a real timestamp")
	}
	if !timestampLess("2026-07-07T10:00:00Z", "") {
		t.Fatal("real timestamp should sort before an empty timestamp")
	}
	if timestampLess("not-a-time", "2026-07-07T10:00:00Z") {
		t.Fatal("invalid timestamp sorted before a real timestamp")
	}
}

func TestRenderSummaryUsesLongerFenceForBackticksInTrace(t *testing.T) {
	summary := renderSummary(candidateReport{
		Records:     1,
		MinFailures: 1,
		Candidates: []failureCandidate{
			{
				Suite:        "router",
				Name:         "backtick_case",
				Fingerprint:  "abc",
				Failures:     1,
				TestRuns:     1,
				TraceExcerpt: "diff line\n```sql\nselect 1;\n```",
			},
		},
	})

	if !strings.Contains(summary, "````text\n") {
		t.Fatalf("summary did not use a longer markdown fence:\n%s", summary)
	}
}

func TestRunWritesHistoryCandidatesAndSummary(t *testing.T) {
	dir := t.TempDir()
	inputDir := filepath.Join(dir, "input")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(inputDir, "one.xml"), junitFailureXML("2026-07-01T00:00:00Z"))
	writeFile(t, filepath.Join(inputDir, "two.xml"), junitFailureXML("2026-07-02T00:00:00Z"))

	historyPath := filepath.Join(dir, "out", "history.jsonl")
	candidatesPath := filepath.Join(dir, "out", "candidates.json")
	summaryPath := filepath.Join(dir, "out", "summary.md")

	err := run([]string{
		"--input-dir", inputDir,
		"--history-output", historyPath,
		"--candidates-output", candidatesPath,
		"--summary-output", summaryPath,
		"--min-failures", "2",
	})
	if err != nil {
		t.Fatal(err)
	}

	historyContent, err := os.ReadFile(historyPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(historyContent), "\n"); got != 2 {
		t.Fatalf("expected 2 history lines, got %d:\n%s", got, string(historyContent))
	}

	var report candidateReport
	content, err := os.ReadFile(candidatesPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(content, &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Candidates) != 1 {
		t.Fatalf("expected 1 candidate, got %#v", report.Candidates)
	}

	summary, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(summary), "Test Failure Candidates") {
		t.Fatalf("summary missing title:\n%s", string(summary))
	}
}

func failureRecord(runID, timestamp, trace string) historyRecord {
	return historyRecord{
		Suite:     "router",
		Name:      "autoprotect_2pc",
		Matrix:    "jammy-16",
		Status:    "failed",
		Message:   "regression diff",
		Trace:     trace,
		RunID:     runID,
		Timestamp: timestamp,
	}
}

func junitFailureXML(timestamp string) string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<testsuite name="router" tests="1" failures="1">
  <testcase classname="router" name="autoprotect_2pc">
    <failure message="regression diff">--- /expected/a.out	` + timestamp + `
+++ /results/a.out	` + timestamp + `
+ERROR: boom</failure>
  </testcase>
</testsuite>`
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
}
