package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type stringList []string

func (s *stringList) String() string {
	return strings.Join(*s, ",")
}

func (s *stringList) Set(value string) error {
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			*s = append(*s, part)
		}
	}
	return nil
}

type runMetadata struct {
	RunID     string `json:"run_id,omitempty"`
	RunURL    string `json:"run_url,omitempty"`
	Workflow  string `json:"workflow,omitempty"`
	Branch    string `json:"branch,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

type historyRecord struct {
	ReportFormat string  `json:"report_format"`
	ReportPath   string  `json:"report_path"`
	Suite        string  `json:"suite"`
	Name         string  `json:"name"`
	Matrix       string  `json:"matrix,omitempty"`
	Status       string  `json:"status"`
	DurationMS   float64 `json:"duration_ms,omitempty"`
	Message      string  `json:"message,omitempty"`
	Trace        string  `json:"trace,omitempty"`
	Fingerprint  string  `json:"fingerprint,omitempty"`

	RunID     string `json:"run_id,omitempty"`
	RunURL    string `json:"run_url,omitempty"`
	Workflow  string `json:"workflow,omitempty"`
	Branch    string `json:"branch,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	Timestamp string `json:"timestamp,omitempty"`
}

type candidateReport struct {
	GeneratedAt string             `json:"generated_at"`
	Records     int                `json:"records"`
	MinFailures int                `json:"min_failures"`
	Candidates  []failureCandidate `json:"candidates"`
}

type failureCandidate struct {
	ID               string        `json:"id"`
	Scope            string        `json:"scope"`
	Suite            string        `json:"suite"`
	Name             string        `json:"name"`
	Matrix           string        `json:"matrix,omitempty"`
	AffectedMatrices []string      `json:"affected_matrices,omitempty"`
	Fingerprint      string        `json:"fingerprint"`
	Failures         int           `json:"failures"`
	TestRuns         int           `json:"test_runs"`
	FailureRate      float64       `json:"failure_rate"`
	FirstObserved    string        `json:"first_observed,omitempty"`
	LastObserved     string        `json:"last_observed,omitempty"`
	LastKnownPass    string        `json:"last_known_pass,omitempty"`
	Message          string        `json:"message,omitempty"`
	TraceExcerpt     string        `json:"trace_excerpt,omitempty"`
	Evidence         []runEvidence `json:"evidence"`
	AgentTaskTitle   string        `json:"agent_task_title"`
}

type runEvidence struct {
	RunID      string `json:"run_id,omitempty"`
	RunURL     string `json:"run_url,omitempty"`
	CommitSHA  string `json:"commit_sha,omitempty"`
	Branch     string `json:"branch,omitempty"`
	Timestamp  string `json:"timestamp,omitempty"`
	Matrix     string `json:"matrix,omitempty"`
	ReportPath string `json:"report_path,omitempty"`
	Message    string `json:"message,omitempty"`
}

type junitSuite struct {
	Name      string       `xml:"name,attr"`
	TestCase  []junitCase  `xml:"testcase"`
	TestSuite []junitSuite `xml:"testsuite"`
}

type junitCase struct {
	ClassName string        `xml:"classname,attr"`
	Name      string        `xml:"name,attr"`
	Time      string        `xml:"time,attr"`
	Failures  []junitResult `xml:"failure"`
	Errors    []junitResult `xml:"error"`
	Skipped   []junitResult `xml:"skipped"`
}

type junitResult struct {
	Message string `xml:"message,attr"`
	Text    string `xml:",chardata"`
}

var (
	timestampRE = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z| ?[+-]\d{4})?\b`)
	outHeaderRE = regexp.MustCompile(`^((?:---|\+\+\+) \S+\.out)(?:\s+.*)?$`)
	spaceRE     = regexp.MustCompile(`[ \t]+`)
)

const (
	candidateScopeMatrix      = "matrix"
	candidateScopeCrossMatrix = "cross_matrix"

	minAffectedMatricesForCrossMatrixCandidate = 2
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	var inputDirs stringList
	var historyInputDirs stringList
	defaultMeta := runMetadata{}

	fs := flag.NewFlagSet("test_history", flag.ContinueOnError)
	fs.Var(&inputDirs, "input-dir", "directory with JUnit XML, CTRF JSON, or history JSONL files; can be repeated")
	fs.Var(&historyInputDirs, "history-input-dir", "directory with existing history JSONL files; can be repeated")
	historyOutput := fs.String("history-output", "", "path for normalized history JSONL")
	candidatesOutput := fs.String("candidates-output", "", "path for failure candidates JSON")
	summaryOutput := fs.String("summary-output", "", "path for failure candidates Markdown")
	minFailures := fs.Int("min-failures", 3, "minimum failures with the same fingerprint before emitting a candidate")
	maxEvidence := fs.Int("max-evidence", 5, "maximum representative failures per candidate")
	fs.StringVar(&defaultMeta.RunID, "run-id", "", "GitHub Actions run id for reports without run-metadata.json")
	fs.StringVar(&defaultMeta.RunURL, "run-url", "", "GitHub Actions run URL for reports without run-metadata.json")
	fs.StringVar(&defaultMeta.Workflow, "workflow", "", "workflow name for reports without run-metadata.json")
	fs.StringVar(&defaultMeta.Branch, "branch", "", "branch name for reports without run-metadata.json")
	fs.StringVar(&defaultMeta.CommitSHA, "sha", "", "commit SHA for reports without run-metadata.json")
	fs.StringVar(&defaultMeta.Timestamp, "timestamp", "", "run timestamp for reports without run-metadata.json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if len(inputDirs) == 0 && len(historyInputDirs) == 0 {
		return errors.New("at least one --input-dir or --history-input-dir is required")
	}
	if *minFailures < 1 {
		return errors.New("--min-failures must be at least 1")
	}
	if *maxEvidence < 1 {
		return errors.New("--max-evidence must be at least 1")
	}

	var records []historyRecord
	for _, dir := range inputDirs {
		parsed, err := readInputDir(dir, defaultMeta)
		if err != nil {
			return err
		}
		records = append(records, parsed...)
	}
	for _, dir := range historyInputDirs {
		parsed, err := readHistoryDir(dir)
		if err != nil {
			return err
		}
		records = append(records, parsed...)
	}

	sortRecords(records)

	if *historyOutput != "" {
		if err := writeHistory(*historyOutput, records); err != nil {
			return err
		}
	}

	report := candidateReport{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Records:     len(records),
		MinFailures: *minFailures,
		Candidates:  buildCandidates(records, *minFailures, *maxEvidence),
	}

	if *candidatesOutput != "" {
		if err := writeJSON(*candidatesOutput, report); err != nil {
			return err
		}
	}
	if *summaryOutput != "" {
		if err := writeText(*summaryOutput, renderSummary(report)); err != nil {
			return err
		}
	}

	return nil
}

func readInputDir(root string, defaultMeta runMetadata) ([]historyRecord, error) {
	var records []historyRecord

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}

		ext := strings.ToLower(filepath.Ext(path))
		base := filepath.Base(path)
		if base == "run-metadata.json" || strings.HasPrefix(base, "failure-candidates") {
			return nil
		}

		switch ext {
		case ".xml":
			parsed, err := parseJUnitFile(root, path, defaultMeta)
			if err != nil {
				return err
			}
			records = append(records, parsed...)
		case ".json":
			parsed, err := parseCTRFFile(root, path, defaultMeta)
			if err != nil {
				return err
			}
			records = append(records, parsed...)
		case ".jsonl":
			parsed, err := readHistoryFile(path)
			if err != nil {
				return err
			}
			records = append(records, parsed...)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return records, nil
}

func readHistoryDir(root string) ([]historyRecord, error) {
	var records []historyRecord
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || strings.ToLower(filepath.Ext(path)) != ".jsonl" {
			return nil
		}
		parsed, err := readHistoryFile(path)
		if err != nil {
			return err
		}
		records = append(records, parsed...)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return records, nil
}

func parseJUnitFile(root, path string, defaultMeta runMetadata) ([]historyRecord, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var probe struct {
		XMLName xml.Name
	}
	if err := xml.Unmarshal(content, &probe); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	var suites []junitSuite
	switch probe.XMLName.Local {
	case "testsuite":
		var suite junitSuite
		if err := xml.Unmarshal(content, &suite); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		suites = append(suites, suite)
	case "testsuites":
		var rootSuites struct {
			Suites []junitSuite `xml:"testsuite"`
		}
		if err := xml.Unmarshal(content, &rootSuites); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		suites = append(suites, rootSuites.Suites...)
	default:
		return nil, nil
	}

	rel := relativePath(root, path)
	meta := metadataForPath(root, path, defaultMeta)
	matrix := deriveMatrix(rel)

	var records []historyRecord
	for _, suite := range suites {
		records = append(records, recordsFromJUnitSuite(suite, rel, matrix, meta)...)
	}
	return records, nil
}

func recordsFromJUnitSuite(suite junitSuite, reportPath, matrix string, meta runMetadata) []historyRecord {
	var records []historyRecord
	for _, nested := range suite.TestSuite {
		records = append(records, recordsFromJUnitSuite(nested, reportPath, matrix, meta)...)
	}
	for _, testCase := range suite.TestCase {
		suiteName := suite.Name
		if suiteName == "" {
			suiteName = testCase.ClassName
		}

		status, message, trace := junitStatus(testCase)
		record := historyRecord{
			ReportFormat: "junit",
			ReportPath:   reportPath,
			Suite:        suiteName,
			Name:         testCase.Name,
			Matrix:       matrix,
			Status:       status,
			DurationMS:   junitDurationMS(testCase.Time),
			Message:      message,
			Trace:        trace,
			RunID:        meta.RunID,
			RunURL:       meta.RunURL,
			Workflow:     meta.Workflow,
			Branch:       meta.Branch,
			CommitSHA:    meta.CommitSHA,
			Timestamp:    meta.Timestamp,
		}
		if record.Timestamp == "" {
			record.Timestamp = timestampFromReportPath(reportPath)
		}
		if isFailure(record.Status) {
			record.Fingerprint = failureFingerprint(record)
		}
		records = append(records, record)
	}
	return records
}

func parseCTRFFile(root, path string, defaultMeta runMetadata) ([]historyRecord, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var rootValue any
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.UseNumber()
	if err := decoder.Decode(&rootValue); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}

	rel := relativePath(root, path)
	meta := metadataForPath(root, path, defaultMeta)
	matrix := deriveMatrix(rel)

	var records []historyRecord
	for _, document := range ctrfDocuments(rootValue) {
		timestamp := stringField(document, "timestamp")
		results, _ := document["results"].(map[string]any)
		if results == nil {
			continue
		}
		tests, _ := results["tests"].([]any)
		for _, item := range tests {
			test, _ := item.(map[string]any)
			if test == nil {
				continue
			}

			suiteName := suiteField(test["suite"])
			testName := stringField(test, "name")
			if suiteName != "" {
				testName = strings.TrimPrefix(testName, suiteName+": ")
			}

			record := historyRecord{
				ReportFormat: "ctrf",
				ReportPath:   rel,
				Suite:        suiteName,
				Name:         testName,
				Matrix:       matrix,
				Status:       stringField(test, "status"),
				DurationMS:   numberField(test, "duration"),
				Message:      stringField(test, "message"),
				Trace:        stringField(test, "trace"),
				RunID:        meta.RunID,
				RunURL:       meta.RunURL,
				Workflow:     meta.Workflow,
				Branch:       meta.Branch,
				CommitSHA:    meta.CommitSHA,
				Timestamp:    firstNonEmpty(meta.Timestamp, timestamp),
			}
			if isFailure(record.Status) {
				record.Fingerprint = failureFingerprint(record)
			}
			records = append(records, record)
		}
	}
	return records, nil
}

func ctrfDocuments(root any) []map[string]any {
	switch typed := root.(type) {
	case map[string]any:
		return []map[string]any{typed}
	case []any:
		documents := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			if document, ok := item.(map[string]any); ok {
				documents = append(documents, document)
			}
		}
		return documents
	default:
		return nil
	}
}

func readHistoryFile(path string) ([]historyRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = file.Close()
	}()

	var records []historyRecord
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 20*1024*1024)
	for line := 1; scanner.Scan(); line++ {
		text := strings.TrimSpace(scanner.Text())
		if text == "" {
			continue
		}
		var record historyRecord
		if err := json.Unmarshal([]byte(text), &record); err != nil {
			return nil, fmt.Errorf("parse %s:%d: %w", path, line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func buildCandidates(records []historyRecord, minFailures, maxEvidence int) []failureCandidate {
	recordsByMatrixTest := make(map[string][]historyRecord)
	recordsByTest := make(map[string][]historyRecord)
	matrixFailuresByFingerprint := make(map[string][]historyRecord)
	crossMatrixFailuresByFingerprint := make(map[string][]historyRecord)

	for _, record := range records {
		if isFailure(record.Status) && record.Fingerprint == "" {
			record.Fingerprint = failureFingerprint(record)
		}
		matrixTestKey := record.testKey()
		testKey := record.testNameKey()
		recordsByMatrixTest[matrixTestKey] = append(recordsByMatrixTest[matrixTestKey], record)
		recordsByTest[testKey] = append(recordsByTest[testKey], record)
		if isFailure(record.Status) {
			matrixKey := candidateScopeMatrix + "\x00" + matrixTestKey + "\x00" + record.Fingerprint
			crossMatrixKey := candidateScopeCrossMatrix + "\x00" + testKey + "\x00" + record.Fingerprint
			matrixFailuresByFingerprint[matrixKey] = append(matrixFailuresByFingerprint[matrixKey], record)
			crossMatrixFailuresByFingerprint[crossMatrixKey] = append(crossMatrixFailuresByFingerprint[crossMatrixKey], record)
		}
	}

	candidates := make([]failureCandidate, 0, len(matrixFailuresByFingerprint)+len(crossMatrixFailuresByFingerprint))
	for key, failures := range matrixFailuresByFingerprint {
		if len(failures) < minFailures {
			continue
		}

		first := earliestRecord(failures)
		candidates = append(candidates, newFailureCandidate(
			candidateScopeMatrix,
			key,
			failures,
			recordsByMatrixTest[first.testKey()],
			nil,
			maxEvidence,
		))
	}

	for key, failures := range crossMatrixFailuresByFingerprint {
		if len(failures) < minFailures {
			continue
		}
		affectedMatrices := affectedMatricesFromFailures(failures)
		if len(affectedMatrices) < minAffectedMatricesForCrossMatrixCandidate {
			continue
		}

		first := earliestRecord(failures)
		candidates = append(candidates, newFailureCandidate(
			candidateScopeCrossMatrix,
			key,
			failures,
			recordsByTest[first.testNameKey()],
			affectedMatrices,
			maxEvidence,
		))
	}

	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Failures != candidates[j].Failures {
			return candidates[i].Failures > candidates[j].Failures
		}
		if candidates[i].LastObserved != candidates[j].LastObserved {
			return candidates[i].LastObserved > candidates[j].LastObserved
		}
		if candidates[i].Scope != candidates[j].Scope {
			return candidates[i].Scope == candidateScopeCrossMatrix
		}
		return candidates[i].ID < candidates[j].ID
	})

	return candidates
}

func newFailureCandidate(scope, key string, failures, testRecords []historyRecord, affectedMatrices []string, maxEvidence int) failureCandidate {
	sortRecords(failures)
	sortRecords(testRecords)

	first := failures[0]
	last := failures[len(failures)-1]
	testRuns := len(testRecords)
	failureRate := 0.0
	if testRuns > 0 {
		failureRate = float64(len(failures)) / float64(testRuns)
	}

	candidate := failureCandidate{
		ID:               shortHash(key),
		Scope:            scope,
		Suite:            first.Suite,
		Name:             first.Name,
		Matrix:           first.Matrix,
		AffectedMatrices: affectedMatrices,
		Fingerprint:      first.Fingerprint,
		Failures:         len(failures),
		TestRuns:         testRuns,
		FailureRate:      failureRate,
		FirstObserved:    first.Timestamp,
		LastObserved:     last.Timestamp,
		LastKnownPass:    lastKnownPass(testRecords, first),
		Message:          last.Message,
		TraceExcerpt:     excerpt(last.Trace, 1800),
		Evidence:         evidenceFromFailures(failures, maxEvidence),
		AgentTaskTitle:   agentTaskTitle(scope, first),
	}
	if scope == candidateScopeCrossMatrix {
		candidate.Matrix = ""
	}
	return candidate
}

func earliestRecord(records []historyRecord) historyRecord {
	copied := append([]historyRecord(nil), records...)
	sortRecords(copied)
	return copied[0]
}

func affectedMatricesFromFailures(failures []historyRecord) []string {
	matrixSet := make(map[string]struct{})
	for _, failure := range failures {
		if failure.Matrix == "" {
			continue
		}
		matrixSet[failure.Matrix] = struct{}{}
	}
	matrices := make([]string, 0, len(matrixSet))
	for matrix := range matrixSet {
		matrices = append(matrices, matrix)
	}
	sort.Strings(matrices)
	return matrices
}

func evidenceFromFailures(failures []historyRecord, maxEvidence int) []runEvidence {
	copied := append([]historyRecord(nil), failures...)
	sort.Slice(copied, func(i, j int) bool {
		return recordAfter(copied[i], copied[j])
	})

	if len(copied) > maxEvidence {
		copied = copied[:maxEvidence]
	}

	evidence := make([]runEvidence, 0, len(copied))
	for _, failure := range copied {
		evidence = append(evidence, runEvidence{
			RunID:      failure.RunID,
			RunURL:     failure.RunURL,
			CommitSHA:  failure.CommitSHA,
			Branch:     failure.Branch,
			Timestamp:  failure.Timestamp,
			Matrix:     failure.Matrix,
			ReportPath: failure.ReportPath,
			Message:    failure.Message,
		})
	}
	return evidence
}

func lastKnownPass(records []historyRecord, firstFailure historyRecord) string {
	lastPass := ""
	for _, record := range records {
		if !isPass(record.Status) {
			continue
		}
		if recordBefore(record, firstFailure) && (lastPass == "" || timestampLess(lastPass, record.Timestamp)) {
			lastPass = record.Timestamp
		}
	}
	return lastPass
}

func writeHistory(path string, records []historyRecord) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(file)
	for _, record := range records {
		if err := encoder.Encode(record); err != nil {
			_ = file.Close()
			return err
		}
	}
	return file.Close()
}

func writeJSON(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	output, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(output, '\n'), 0644)
}

func writeText(path, value string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return os.WriteFile(path, []byte(value), 0644)
}

func renderSummary(report candidateReport) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "# Test Failure Candidates\n\n")
	fmt.Fprintf(&builder, "Records analyzed: %d\n\n", report.Records)
	fmt.Fprintf(&builder, "Minimum failures per fingerprint: %d\n\n", report.MinFailures)

	if len(report.Candidates) == 0 {
		builder.WriteString("No repeated failure candidates matched the configured threshold.\n")
		return builder.String()
	}

	builder.WriteString("| Scope | Failures | Test runs | Suite | Test | Matrix | Affected matrices | First observed | Last observed |\n")
	builder.WriteString("|---|---:|---:|---|---|---|---|---|---|\n")
	for _, candidate := range report.Candidates {
		fmt.Fprintf(
			&builder,
			"| %s | %d | %d | %s | %s | %s | %s | %s | %s |\n",
			markdownCell(candidate.Scope),
			candidate.Failures,
			candidate.TestRuns,
			markdownCell(candidate.Suite),
			markdownCell(candidate.Name),
			markdownCell(candidate.Matrix),
			markdownCell(strings.Join(candidate.AffectedMatrices, ", ")),
			markdownCell(candidate.FirstObserved),
			markdownCell(candidate.LastObserved),
		)
	}

	for _, candidate := range report.Candidates {
		fmt.Fprintf(&builder, "\n## %s\n\n", candidate.AgentTaskTitle)
		fmt.Fprintf(&builder, "- Scope: `%s`\n", candidate.Scope)
		fmt.Fprintf(&builder, "- Fingerprint: `%s`\n", candidate.Fingerprint)
		if len(candidate.AffectedMatrices) > 0 {
			fmt.Fprintf(&builder, "- Affected matrices: %s\n", strings.Join(candidate.AffectedMatrices, ", "))
		}
		fmt.Fprintf(&builder, "- Failure rate in collected history: %.2f\n", candidate.FailureRate)
		if candidate.LastKnownPass != "" {
			fmt.Fprintf(&builder, "- Last known pass before first observed failure: %s\n", candidate.LastKnownPass)
		}
		if len(candidate.Evidence) > 0 {
			builder.WriteString("- Recent evidence:\n")
			for _, evidence := range candidate.Evidence {
				label := evidence.RunID
				if label == "" {
					label = evidence.Timestamp
				}
				if evidence.RunURL != "" {
					fmt.Fprintf(&builder, "  - [%s](%s)", label, evidence.RunURL)
				} else {
					fmt.Fprintf(&builder, "  - %s", label)
				}
				if evidence.CommitSHA != "" {
					fmt.Fprintf(&builder, " `%s`", shortSHA(evidence.CommitSHA))
				}
				if evidence.Matrix != "" {
					fmt.Fprintf(&builder, " `%s`", evidence.Matrix)
				}
				if evidence.ReportPath != "" {
					fmt.Fprintf(&builder, " `%s`", evidence.ReportPath)
				}
				builder.WriteByte('\n')
			}
		}
		if candidate.TraceExcerpt != "" {
			fence := markdownCodeFence(candidate.TraceExcerpt)
			builder.WriteString("\n")
			builder.WriteString(fence)
			builder.WriteString("text\n")
			builder.WriteString(candidate.TraceExcerpt)
			if !strings.HasSuffix(candidate.TraceExcerpt, "\n") {
				builder.WriteByte('\n')
			}
			builder.WriteString(fence)
			builder.WriteByte('\n')
		}
	}

	return builder.String()
}

func junitStatus(testCase junitCase) (string, string, string) {
	if len(testCase.Errors) > 0 {
		return "failed", testCase.Errors[0].Message, strings.TrimSpace(testCase.Errors[0].Text)
	}
	if len(testCase.Failures) > 0 {
		return "failed", testCase.Failures[0].Message, strings.TrimSpace(testCase.Failures[0].Text)
	}
	if len(testCase.Skipped) > 0 {
		return "skipped", "", ""
	}
	return "passed", "", ""
}

func junitDurationMS(value string) float64 {
	if value == "" {
		return 0
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0
	}
	return seconds * 1000
}

func metadataForPath(root, path string, defaults runMetadata) runMetadata {
	meta := defaults
	rootAbs, _ := filepath.Abs(root)
	dir, _ := filepath.Abs(filepath.Dir(path))

	for {
		content, err := os.ReadFile(filepath.Join(dir, "run-metadata.json"))
		if err == nil {
			meta = mergeMetadata(meta, parseRunMetadata(content))
			break
		}
		if dir == rootAbs || dir == filepath.Dir(dir) {
			break
		}
		dir = filepath.Dir(dir)
	}

	return meta
}

func parseRunMetadata(content []byte) runMetadata {
	var raw map[string]any
	if err := json.Unmarshal(content, &raw); err != nil {
		return runMetadata{}
	}
	return runMetadata{
		RunID:     firstNonEmpty(stringAny(raw["run_id"]), stringAny(raw["runId"]), stringAny(raw["databaseId"])),
		RunURL:    firstNonEmpty(stringAny(raw["run_url"]), stringAny(raw["url"])),
		Workflow:  firstNonEmpty(stringAny(raw["workflow"]), stringAny(raw["workflowName"])),
		Branch:    firstNonEmpty(stringAny(raw["branch"]), stringAny(raw["headBranch"])),
		CommitSHA: firstNonEmpty(stringAny(raw["commit_sha"]), stringAny(raw["headSha"])),
		Timestamp: firstNonEmpty(stringAny(raw["timestamp"]), stringAny(raw["createdAt"])),
	}
}

func mergeMetadata(base, override runMetadata) runMetadata {
	if override.RunID != "" {
		base.RunID = override.RunID
	}
	if override.RunURL != "" {
		base.RunURL = override.RunURL
	}
	if override.Workflow != "" {
		base.Workflow = override.Workflow
	}
	if override.Branch != "" {
		base.Branch = override.Branch
	}
	if override.CommitSHA != "" {
		base.CommitSHA = override.CommitSHA
	}
	if override.Timestamp != "" {
		base.Timestamp = override.Timestamp
	}
	return base
}

func deriveMatrix(rel string) string {
	rel = filepath.ToSlash(rel)
	parts := strings.Split(rel, "/")
	for i, part := range parts {
		switch {
		case strings.HasPrefix(part, "test-reports-regress-coord-"):
			return strings.TrimPrefix(part, "test-reports-regress-coord-")
		case strings.HasPrefix(part, "test-reports-regress-"):
			return strings.TrimPrefix(part, "test-reports-regress-")
		case strings.HasPrefix(part, "test-reports-feature-"):
			return strings.TrimPrefix(part, "test-reports-feature-")
		case part == "test-reports-isolation":
			return "isolation"
		case part == "regress-coord" && i+1 < len(parts)-1:
			return parts[i+1]
		case part == "regress" && i+1 < len(parts)-1:
			return parts[i+1]
		case part == "feature" && i+1 < len(parts):
			return strings.TrimSuffix(strings.TrimPrefix(parts[i+1], "feature-"), filepath.Ext(parts[i+1]))
		case part == "isolation":
			return "isolation"
		}
	}
	if len(parts) > 1 && !strings.HasPrefix(parts[0], "ctrf-report") {
		return parts[0]
	}
	return ""
}

func timestampFromReportPath(reportPath string) string {
	for _, part := range strings.Split(filepath.ToSlash(reportPath), "/") {
		if strings.HasPrefix(part, "run-") {
			candidate := strings.TrimPrefix(part, "run-")
			if isRFC3339Timestamp(candidate) {
				return candidate
			}
		}
	}
	return ""
}

func failureFingerprint(record historyRecord) string {
	text := normalizeFailureText(record.Message + "\n" + record.Trace)
	if text == "" {
		text = record.Status
	}
	return shortHash(record.Suite + "\n" + record.Name + "\n" + text)
}

func normalizeFailureText(value string) string {
	value = strings.ReplaceAll(value, "\r\n", "\n")
	value = timestampRE.ReplaceAllString(value, "<timestamp>")

	var lines []string
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimRight(line, " \t")
		line = outHeaderRE.ReplaceAllString(line, "$1")
		line = spaceRE.ReplaceAllString(line, " ")
		if strings.TrimSpace(line) != "" {
			lines = append(lines, line)
		}
		if len(lines) >= 120 {
			break
		}
	}
	return strings.Join(lines, "\n")
}

func (r historyRecord) testKey() string {
	return r.Suite + "\x00" + r.Name + "\x00" + r.Matrix
}

func (r historyRecord) testNameKey() string {
	return r.Suite + "\x00" + r.Name
}

func sortRecords(records []historyRecord) {
	sort.SliceStable(records, func(i, j int) bool {
		if records[i].Timestamp != records[j].Timestamp {
			return timestampLess(records[i].Timestamp, records[j].Timestamp)
		}
		if records[i].RunID != records[j].RunID {
			return records[i].RunID < records[j].RunID
		}
		if records[i].ReportPath != records[j].ReportPath {
			return records[i].ReportPath < records[j].ReportPath
		}
		if records[i].Suite != records[j].Suite {
			return records[i].Suite < records[j].Suite
		}
		return records[i].Name < records[j].Name
	})
}

func recordBefore(left, right historyRecord) bool {
	return timestampLess(left.Timestamp, right.Timestamp)
}

func recordAfter(left, right historyRecord) bool {
	return timestampLess(right.Timestamp, left.Timestamp)
}

func timestampLess(left, right string) bool {
	if left == "" {
		return false
	}
	if right == "" {
		return true
	}
	leftTime, leftErr := time.Parse(time.RFC3339Nano, left)
	rightTime, rightErr := time.Parse(time.RFC3339Nano, right)
	if leftErr == nil && rightErr == nil {
		return leftTime.Before(rightTime)
	}
	if leftErr == nil {
		return true
	}
	if rightErr == nil {
		return false
	}
	return left < right
}

func isFailure(status string) bool {
	status = strings.ToLower(status)
	return status == "failed" || status == "failure" || status == "error"
}

func isPass(status string) bool {
	status = strings.ToLower(status)
	return status == "passed" || status == "pass"
}

func agentTaskTitle(scope string, record historyRecord) string {
	if scope == candidateScopeCrossMatrix {
		return fmt.Sprintf("Investigate recurring cross-matrix failure in %s / %s", record.Suite, record.Name)
	}
	if record.Matrix != "" {
		return fmt.Sprintf("Investigate recurring failure in %s / %s [%s]", record.Suite, record.Name, record.Matrix)
	}
	return fmt.Sprintf("Investigate recurring failure in %s / %s", record.Suite, record.Name)
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func shortSHA(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

func excerpt(value string, limit int) string {
	value = strings.TrimSpace(value)
	if len(value) <= limit {
		return value
	}
	return strings.TrimSpace(value[:limit]) + "\n..."
}

func markdownCodeFence(value string) string {
	longestRun := 0
	currentRun := 0
	for _, char := range value {
		if char == '`' {
			currentRun++
			if currentRun > longestRun {
				longestRun = currentRun
			}
			continue
		}
		currentRun = 0
	}
	if longestRun < 3 {
		longestRun = 3
	} else {
		longestRun++
	}
	return strings.Repeat("`", longestRun)
}

func markdownCell(value string) string {
	value = strings.ReplaceAll(value, "\n", " ")
	value = strings.ReplaceAll(value, "|", "\\|")
	if value == "" {
		return "-"
	}
	return value
}

func isRFC3339Timestamp(value string) bool {
	if value == "" {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
}

func suiteField(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case []any:
		parts := make([]string, 0, len(typed))
		for _, part := range typed {
			if text, ok := part.(string); ok && text != "" {
				parts = append(parts, text)
			}
		}
		return strings.Join(parts, " / ")
	default:
		return ""
	}
}

func stringField(values map[string]any, key string) string {
	return stringAny(values[key])
}

func numberField(values map[string]any, key string) float64 {
	switch typed := values[key].(type) {
	case json.Number:
		value, _ := typed.Float64()
		return value
	case float64:
		return typed
	case int:
		return float64(typed)
	default:
		return 0
	}
}

func stringAny(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		if typed == float64(int64(typed)) {
			return strconv.FormatInt(int64(typed), 10)
		}
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case int:
		return strconv.Itoa(typed)
	default:
		return ""
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func relativePath(root, path string) string {
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return filepath.ToSlash(path)
	}
	return filepath.ToSlash(rel)
}
