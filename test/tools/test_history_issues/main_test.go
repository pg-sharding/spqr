package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIssueSpecFromCrossMatrixCandidate(t *testing.T) {
	spec := issueSpecFromCandidate(failureCandidate{
		ID:               "abc123",
		Scope:            "cross_matrix",
		Suite:            "router-regress_router-6432",
		Name:             "autoprotect_2pc",
		AffectedMatrices: []string{"jammy-14", "noble-15"},
		Fingerprint:      "1d269da6e7c2840e",
		Failures:         3,
		TestRuns:         4,
		FailureRate:      0.75,
		FirstObserved:    "2026-07-07T10:10:06Z",
		LastObserved:     "2026-07-09T15:39:34Z",
		AgentTaskTitle:   "Investigate recurring cross-matrix failure in router-regress_router-6432 / autoprotect_2pc",
		TraceExcerpt:     "diff line\n```sql\nselect 1;\n```",
		Evidence: []runEvidence{
			{
				RunID:      "29030391367",
				RunURL:     "https://github.test/runs/29030391367",
				CommitSHA:  "733275a6eedba6f17469954ad4e14db9bf543b43",
				Matrix:     "jammy-14",
				ReportPath: "29030391367/test-reports-regress-jammy-14/report.xml",
			},
		},
	})

	if spec.CandidateID != "abc123" {
		t.Fatalf("unexpected candidate id: %#v", spec)
	}
	if spec.Title != "[ci-flaky] router-regress_router-6432 / autoprotect_2pc (cross-matrix)" {
		t.Fatalf("unexpected title: %q", spec.Title)
	}
	for _, want := range []string{
		"<!-- spqr-test-failure-candidate:abc123 -->",
		"- Affected matrices: jammy-14, noble-15",
		"- Fingerprint: `1d269da6e7c2840e`",
		"[29030391367](https://github.test/runs/29030391367)",
		"`733275a6eedb`",
		"````text\n",
		"Find the root cause",
	} {
		if !strings.Contains(spec.Body, want) {
			t.Fatalf("body missing %q:\n%s", want, spec.Body)
		}
	}
}

func TestIssueSpecFromMatrixCandidate(t *testing.T) {
	spec := issueSpecFromCandidate(failureCandidate{
		ID:          "matrix123",
		Scope:       "matrix",
		Suite:       "isolation-regress",
		Name:        "pg_advisory_lock",
		Matrix:      "isolation",
		Fingerprint: "d3c2af5c34d33d30",
		Failures:    4,
		TestRuns:    4,
	})

	if spec.Title != "[ci-flaky] isolation-regress / pg_advisory_lock [isolation]" {
		t.Fatalf("unexpected title: %q", spec.Title)
	}
	if !strings.Contains(spec.Body, "- Matrix: `isolation`") {
		t.Fatalf("body missing matrix:\n%s", spec.Body)
	}
}

func TestFindIssueByCandidateID(t *testing.T) {
	issues := []githubIssue{
		{Number: 1, Body: "<!-- spqr-test-failure-candidate:other -->"},
		{Number: 2, Body: "<!-- spqr-test-failure-candidate:target -->"},
		{Number: 3, Body: "<!-- spqr-test-failure-candidate:target -->", PullRequest: map[string]any{}},
	}

	issue := findIssueByCandidateID(issues, "target")
	if issue == nil || issue.Number != 2 {
		t.Fatalf("unexpected issue match: %#v", issue)
	}
	if issue := findIssueByCandidateID(issues, "missing"); issue != nil {
		t.Fatalf("unexpected issue match: %#v", issue)
	}
}

func TestFilterIssueCandidatesDropsMatrixCandidateCoveredByCrossMatrix(t *testing.T) {
	candidates := []failureCandidate{
		{
			ID:          "cross",
			Scope:       "cross_matrix",
			Suite:       "router",
			Name:        "autoprotect_2pc",
			Fingerprint: "same",
		},
		{
			ID:          "matrix",
			Scope:       "matrix",
			Suite:       "router",
			Name:        "autoprotect_2pc",
			Matrix:      "jammy-14",
			Fingerprint: "same",
		},
		{
			ID:          "other",
			Scope:       "matrix",
			Suite:       "isolation",
			Name:        "pg_advisory_lock",
			Matrix:      "isolation",
			Fingerprint: "other",
		},
	}

	filtered := filterIssueCandidates(candidates)
	if len(filtered) != 2 {
		t.Fatalf("unexpected filtered candidates: %#v", filtered)
	}
	if filtered[0].ID != "cross" || filtered[1].ID != "other" {
		t.Fatalf("unexpected filtered order: %#v", filtered)
	}
}

func TestSyncIssuesSkipsEmptyCandidates(t *testing.T) {
	var output strings.Builder
	err := syncIssues(githubClient{}, nil, []string{"ci-flaky"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "no failure candidates to sync\n" {
		t.Fatalf("unexpected output: %q", got)
	}
}

func TestRunDryRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "failure-candidates.json")
	content := `{
  "candidates": [
    {
      "id": "abc123",
      "scope": "cross_matrix",
      "suite": "router",
      "name": "autoprotect_2pc",
      "fingerprint": "fp",
      "failures": 3,
      "test_runs": 3
    }
  ]
}`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	var output strings.Builder
	err := run([]string{"--candidates-input", path, "--dry-run"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if got := output.String(); !strings.Contains(got, "would sync candidate abc123") {
		t.Fatalf("unexpected dry-run output: %q", got)
	}
}

func TestParseRepo(t *testing.T) {
	owner, repo, err := parseRepo("pg-sharding/spqr")
	if err != nil {
		t.Fatal(err)
	}
	if owner != "pg-sharding" || repo != "spqr" {
		t.Fatalf("unexpected repo parse: %q/%q", owner, repo)
	}
	if _, _, err := parseRepo("spqr"); err == nil {
		t.Fatal("expected invalid repo error")
	}
}
