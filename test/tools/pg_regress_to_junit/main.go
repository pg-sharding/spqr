package main

import (
	"encoding/xml"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type testResult struct {
	Name   string
	Status string
}

type junitSuite struct {
	XMLName   xml.Name        `xml:"testsuite"`
	Name      string          `xml:"name,attr"`
	Tests     int             `xml:"tests,attr"`
	Failures  int             `xml:"failures,attr"`
	Errors    int             `xml:"errors,attr"`
	Skipped   int             `xml:"skipped,attr"`
	TestCases []junitTestCase `xml:"testcase"`
}

type junitTestCase struct {
	ClassName string        `xml:"classname,attr"`
	Name      string        `xml:"name,attr"`
	Failure   *junitFailure `xml:"failure,omitempty"`
}

type junitFailure struct {
	Message string `xml:"message,attr"`
	Text    string `xml:",chardata"`
}

var (
	tapResultRE     = regexp.MustCompile(`^(ok|not ok)\s+\d+\s+-\s+(.+)$`)
	pgRegressLineRE = regexp.MustCompile(`^test\s+(.+?)\s+\.\.\.\s+(ok|FAILED|failed)\b`)
	durationRE      = regexp.MustCompile(`\s+\d+\s+ms$`)
	diffTestNameRE  = regexp.MustCompile(`(?:^|[\s/])(?:expected|results)/([^/\s]+)\.out\b`)
)

func main() {
	suite := flag.String("suite", "", "JUnit testsuite name")
	regressionOutPath := flag.String("regression-out", "", "pg_regress regression.out path")
	diffsPath := flag.String("diffs", "", "pg_regress regression.diffs path")
	outputPath := flag.String("output", "", "JUnit XML output path")
	flag.Parse()

	if *suite == "" || *regressionOutPath == "" || *outputPath == "" {
		fmt.Fprintln(os.Stderr, "usage: pg_regress_to_junit --suite NAME --regression-out FILE [--diffs FILE] --output FILE")
		os.Exit(2)
	}

	regressionOut, err := readOptionalFile(*regressionOutPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read regression output: %v\n", err)
		os.Exit(1)
	}

	diffs, err := readOptionalFile(*diffsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read regression diffs: %v\n", err)
		os.Exit(1)
	}

	report := buildJUnitSuite(*suite, parseResults(regressionOut), splitDiffsByTest(diffs), diffs)
	if err := writeJUnit(*outputPath, report); err != nil {
		fmt.Fprintf(os.Stderr, "write junit report: %v\n", err)
		os.Exit(1)
	}
}

func readOptionalFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}

	content, err := os.ReadFile(path)
	if err == nil {
		return string(content), nil
	}
	if os.IsNotExist(err) {
		return "", nil
	}
	return "", err
}

func parseResults(regressionOut string) []testResult {
	var results []testResult

	for _, line := range strings.Split(regressionOut, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		if match := tapResultRE.FindStringSubmatch(line); match != nil {
			status := "passed"
			if match[1] == "not ok" {
				status = "failed"
			}
			results = append(results, testResult{
				Name:   cleanupTestName(match[2]),
				Status: status,
			})
			continue
		}

		if match := pgRegressLineRE.FindStringSubmatch(line); match != nil {
			status := "passed"
			if strings.EqualFold(match[2], "failed") {
				status = "failed"
			}
			results = append(results, testResult{
				Name:   cleanupTestName(match[1]),
				Status: status,
			})
		}
	}

	return results
}

func cleanupTestName(name string) string {
	return strings.TrimSpace(durationRE.ReplaceAllString(name, ""))
}

func splitDiffsByTest(diffs string) map[string]string {
	sections := make(map[string]string)
	var currentTest string

	for _, line := range strings.SplitAfter(diffs, "\n") {
		if match := diffTestNameRE.FindStringSubmatch(line); match != nil {
			currentTest = match[1]
		}
		if currentTest != "" {
			sections[currentTest] += line
		}
	}

	return sections
}

func buildJUnitSuite(suiteName string, parsedResults []testResult, diffsByTest map[string]string, fullDiff string) junitSuite {
	resultsByName := make(map[string]string)
	var order []string

	addResult := func(name, status string) {
		if name == "" {
			return
		}
		if _, ok := resultsByName[name]; !ok {
			order = append(order, name)
		}
		if status == "failed" || resultsByName[name] == "" {
			resultsByName[name] = status
		}
	}

	for _, result := range parsedResults {
		addResult(result.Name, result.Status)
	}

	diffNames := make([]string, 0, len(diffsByTest))
	for name := range diffsByTest {
		diffNames = append(diffNames, name)
	}
	sort.Strings(diffNames)
	for _, name := range diffNames {
		addResult(name, "failed")
	}

	if len(order) == 0 {
		status := "passed"
		if fullDiff != "" {
			status = "failed"
		}
		addResult(suiteName, status)
	}

	testCases := make([]junitTestCase, 0, len(order))
	failures := 0
	for _, name := range order {
		testCase := junitTestCase{
			ClassName: suiteName,
			Name:      name,
		}
		if resultsByName[name] == "failed" {
			failures++
			diffText := diffsByTest[name]
			if diffText == "" {
				diffText = fullDiff
			}
			testCase.Failure = &junitFailure{
				Message: "regression diff",
				Text:    diffText,
			}
		}
		testCases = append(testCases, testCase)
	}

	return junitSuite{
		Name:      suiteName,
		Tests:     len(testCases),
		Failures:  failures,
		Errors:    0,
		Skipped:   0,
		TestCases: testCases,
	}
}

func writeJUnit(path string, suite junitSuite) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	output, err := xml.MarshalIndent(suite, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(path, append([]byte(xml.Header), append(output, '\n')...), 0644)
}
