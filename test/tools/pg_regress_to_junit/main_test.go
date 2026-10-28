package main

import (
	"encoding/xml"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseResultsTAP(t *testing.T) {
	results := parseResults(strings.Join([]string{
		"ok 1         - simple                                    12 ms",
		"not ok 2     - move_guard_check                          34 ms",
	}, "\n"))

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0] != (testResult{Name: "simple", Status: "passed"}) {
		t.Fatalf("unexpected first result: %#v", results[0])
	}
	if results[1] != (testResult{Name: "move_guard_check", Status: "failed"}) {
		t.Fatalf("unexpected second result: %#v", results[1])
	}
}

func TestParseResultsPgRegressStyle(t *testing.T) {
	results := parseResults(strings.Join([]string{
		"test setup                    ... ok",
		"test routing_hint             ... FAILED",
	}, "\n"))

	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0] != (testResult{Name: "setup", Status: "passed"}) {
		t.Fatalf("unexpected first result: %#v", results[0])
	}
	if results[1] != (testResult{Name: "routing_hint", Status: "failed"}) {
		t.Fatalf("unexpected second result: %#v", results[1])
	}
}

func TestSplitDiffsByTest(t *testing.T) {
	diffs := strings.Join([]string{
		"diff -U3 /regress/tests/router/expected/alpha.out /regress/tests/router/results/alpha.out",
		"-alpha expected",
		"+alpha actual",
		"diff -U3 /regress/tests/router/expected/beta.out /regress/tests/router/results/beta.out",
		"-beta expected",
		"+beta actual",
		"",
	}, "\n")

	sections := splitDiffsByTest(diffs)
	if strings.Contains(sections["alpha"], "beta actual") {
		t.Fatalf("alpha diff includes beta section:\n%s", sections["alpha"])
	}
	if !strings.Contains(sections["alpha"], "+alpha actual") {
		t.Fatalf("alpha diff missing alpha detail:\n%s", sections["alpha"])
	}
	if !strings.Contains(sections["beta"], "+beta actual") {
		t.Fatalf("beta diff missing beta detail:\n%s", sections["beta"])
	}
}

func TestSplitDiffsByTestWithRelativePaths(t *testing.T) {
	diffs := strings.Join([]string{
		"diff -U3 expected/relative.out results/relative.out",
		"-expected",
		"+actual",
		"",
	}, "\n")

	sections := splitDiffsByTest(diffs)
	if !strings.Contains(sections["relative"], "+actual") {
		t.Fatalf("relative diff missing detail:\n%s", sections["relative"])
	}
}

func TestBuildJUnitUsesPerTestDiff(t *testing.T) {
	fullDiff := strings.Join([]string{
		"diff -U3 /regress/tests/router/expected/alpha.out /regress/tests/router/results/alpha.out",
		"-alpha expected",
		"+alpha actual",
		"diff -U3 /regress/tests/router/expected/beta.out /regress/tests/router/results/beta.out",
		"-beta expected",
		"+beta actual",
		"",
	}, "\n")

	suite := buildJUnitSuite("router", []testResult{
		{Name: "alpha", Status: "failed"},
		{Name: "beta", Status: "failed"},
	}, splitDiffsByTest(fullDiff), fullDiff)

	if suite.Tests != 2 || suite.Failures != 2 {
		t.Fatalf("unexpected summary: tests=%d failures=%d", suite.Tests, suite.Failures)
	}
	if strings.Contains(suite.TestCases[0].Failure.Text, "beta actual") {
		t.Fatalf("alpha failure includes beta diff:\n%s", suite.TestCases[0].Failure.Text)
	}
	if strings.Contains(suite.TestCases[1].Failure.Text, "alpha actual") {
		t.Fatalf("beta failure includes alpha diff:\n%s", suite.TestCases[1].Failure.Text)
	}
}

func TestBuildJUnitFallsBackWhenNoResults(t *testing.T) {
	suite := buildJUnitSuite("empty", nil, nil, "")

	if suite.Tests != 1 || suite.Failures != 0 {
		t.Fatalf("unexpected summary: tests=%d failures=%d", suite.Tests, suite.Failures)
	}
	if suite.TestCases[0].Name != "empty" {
		t.Fatalf("unexpected fallback testcase: %#v", suite.TestCases[0])
	}
}

func TestWriteJUNITEscapesXML(t *testing.T) {
	dir := t.TempDir()
	output := filepath.Join(dir, "report.xml")

	suite := buildJUnitSuite("suite", []testResult{
		{Name: `name <&>"`, Status: "failed"},
	}, nil, `trace <&>"`)

	if err := writeJUnit(output, suite); err != nil {
		t.Fatalf("write junit: %v", err)
	}

	content, err := os.ReadFile(output)
	if err != nil {
		t.Fatalf("read junit: %v", err)
	}

	var parsed junitSuite
	if err := xml.Unmarshal(content, &parsed); err != nil {
		t.Fatalf("invalid xml: %v\n%s", err, string(content))
	}
	if parsed.TestCases[0].Name != `name <&>"` {
		t.Fatalf("unexpected testcase name after XML roundtrip: %q", parsed.TestCases[0].Name)
	}
	if parsed.TestCases[0].Failure.Text != `trace <&>"` {
		t.Fatalf("unexpected failure text after XML roundtrip: %q", parsed.TestCases[0].Failure.Text)
	}
}

func TestReadOptionalFileMissing(t *testing.T) {
	content, err := readOptionalFile(filepath.Join(t.TempDir(), "missing.diffs"))
	if err != nil {
		t.Fatalf("read missing optional file: %v", err)
	}
	if content != "" {
		t.Fatalf("expected empty content for missing optional file, got %q", content)
	}
}
