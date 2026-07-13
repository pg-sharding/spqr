package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
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

type candidateReport struct {
	Candidates []failureCandidate `json:"candidates"`
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

type issueSpec struct {
	CandidateID string
	Title       string
	Body        string
}

type githubIssue struct {
	Number      int    `json:"number"`
	Title       string `json:"title"`
	Body        string `json:"body"`
	PullRequest any    `json:"pull_request,omitempty"`
}

type githubLabel struct {
	Name        string `json:"name"`
	Color       string `json:"color"`
	Description string `json:"description,omitempty"`
}

type githubClient struct {
	apiURL     string
	owner      string
	repo       string
	token      string
	httpClient *http.Client
}

type options struct {
	candidatesInput string
	repo            string
	token           string
	apiURL          string
	labels          stringList
	dryRun          bool
}

const markerPrefix = "<!-- spqr-test-failure-candidate:"

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout io.Writer) error {
	opts := options{}
	fs := flag.NewFlagSet("test_history_issues", flag.ContinueOnError)
	fs.StringVar(&opts.candidatesInput, "candidates-input", "", "path to failure-candidates.json")
	fs.StringVar(&opts.repo, "repo", "", "GitHub repository in owner/name form")
	fs.StringVar(&opts.token, "github-token", firstNonEmpty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GH_TOKEN")), "GitHub token; defaults to GITHUB_TOKEN or GH_TOKEN")
	fs.StringVar(&opts.apiURL, "github-api-url", "https://api.github.com", "GitHub API base URL")
	fs.Var(&opts.labels, "label", "label to add to created/updated issues; can be repeated or comma-separated")
	fs.BoolVar(&opts.dryRun, "dry-run", false, "render planned issue changes without writing to GitHub")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if opts.candidatesInput == "" {
		return errors.New("--candidates-input is required")
	}
	if len(opts.labels) == 0 {
		opts.labels = stringList{"ci-flaky", "agent:flake-fix"}
	}

	report, err := readCandidateReport(opts.candidatesInput)
	if err != nil {
		return err
	}
	candidates := filterIssueCandidates(report.Candidates)
	specs := make([]issueSpec, 0, len(candidates))
	for _, candidate := range candidates {
		specs = append(specs, issueSpecFromCandidate(candidate))
	}

	if opts.dryRun {
		for _, spec := range specs {
			if _, err := fmt.Fprintf(stdout, "would sync candidate %s: %s\n", spec.CandidateID, spec.Title); err != nil {
				return err
			}
		}
		return nil
	}
	if opts.repo == "" {
		return errors.New("--repo is required unless --dry-run is set")
	}
	if opts.token == "" {
		return errors.New("--github-token or GITHUB_TOKEN is required unless --dry-run is set")
	}

	owner, repo, err := parseRepo(opts.repo)
	if err != nil {
		return err
	}
	client := githubClient{
		apiURL: strings.TrimRight(opts.apiURL, "/"),
		owner:  owner,
		repo:   repo,
		token:  opts.token,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
	return syncIssues(client, specs, []string(opts.labels), stdout)
}

func readCandidateReport(path string) (candidateReport, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return candidateReport{}, err
	}
	var report candidateReport
	if err := json.Unmarshal(content, &report); err != nil {
		return candidateReport{}, err
	}
	return report, nil
}

func filterIssueCandidates(candidates []failureCandidate) []failureCandidate {
	crossMatrixKeys := make(map[string]struct{})
	for _, candidate := range candidates {
		if candidate.Scope == "cross_matrix" {
			crossMatrixKeys[candidate.issueGroupKey()] = struct{}{}
		}
	}

	filtered := make([]failureCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.Scope == "matrix" {
			if _, ok := crossMatrixKeys[candidate.issueGroupKey()]; ok {
				continue
			}
		}
		filtered = append(filtered, candidate)
	}
	return filtered
}

func (candidate failureCandidate) issueGroupKey() string {
	return candidate.Suite + "\x00" + candidate.Name + "\x00" + candidate.Fingerprint
}

func syncIssues(client githubClient, specs []issueSpec, labels []string, stdout io.Writer) error {
	if len(specs) == 0 {
		if _, err := fmt.Fprintln(stdout, "no failure candidates to sync"); err != nil {
			return err
		}
		return nil
	}

	for _, label := range defaultLabelDefinitions(labels) {
		if err := client.ensureLabel(label); err != nil {
			return err
		}
	}

	existingIssues, err := client.listCandidateIssues(labels)
	if err != nil {
		return err
	}

	for _, spec := range specs {
		existing := findIssueByCandidateID(existingIssues, spec.CandidateID)
		if existing == nil {
			created, err := client.createIssue(spec, labels)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(stdout, "created issue #%d for candidate %s\n", created.Number, spec.CandidateID); err != nil {
				return err
			}
			continue
		}
		if existing.Title == spec.Title && existing.Body == spec.Body {
			if _, err := fmt.Fprintf(stdout, "issue #%d already up to date for candidate %s\n", existing.Number, spec.CandidateID); err != nil {
				return err
			}
			continue
		}
		updated, err := client.updateIssue(existing.Number, spec, labels)
		if err != nil {
			return err
		}
		if _, err := fmt.Fprintf(stdout, "updated issue #%d for candidate %s\n", updated.Number, spec.CandidateID); err != nil {
			return err
		}
	}
	return nil
}

func issueSpecFromCandidate(candidate failureCandidate) issueSpec {
	title := candidateTitle(candidate)
	var builder strings.Builder
	fmt.Fprintf(&builder, "%s%s -->\n\n", markerPrefix, candidate.ID)
	builder.WriteString("## Test Failure Candidate\n\n")
	builder.WriteString("This issue is generated from `test-history` and is intended to be an agent-ready investigation task.\n\n")
	builder.WriteString("## Candidate\n\n")
	fmt.Fprintf(&builder, "- Scope: `%s`\n", candidate.Scope)
	fmt.Fprintf(&builder, "- Suite: `%s`\n", candidate.Suite)
	fmt.Fprintf(&builder, "- Test: `%s`\n", candidate.Name)
	if candidate.Matrix != "" {
		fmt.Fprintf(&builder, "- Matrix: `%s`\n", candidate.Matrix)
	}
	if len(candidate.AffectedMatrices) > 0 {
		fmt.Fprintf(&builder, "- Affected matrices: %s\n", strings.Join(candidate.AffectedMatrices, ", "))
	}
	fmt.Fprintf(&builder, "- Fingerprint: `%s`\n", candidate.Fingerprint)
	fmt.Fprintf(&builder, "- Failures: %d / %d collected runs\n", candidate.Failures, candidate.TestRuns)
	fmt.Fprintf(&builder, "- Failure rate in collected history: %.2f\n", candidate.FailureRate)
	if candidate.FirstObserved != "" {
		fmt.Fprintf(&builder, "- First observed: %s\n", candidate.FirstObserved)
	}
	if candidate.LastObserved != "" {
		fmt.Fprintf(&builder, "- Last observed: %s\n", candidate.LastObserved)
	}
	if candidate.LastKnownPass != "" {
		fmt.Fprintf(&builder, "- Last known pass before first observed failure: %s\n", candidate.LastKnownPass)
	}

	if len(candidate.Evidence) > 0 {
		builder.WriteString("\n## Evidence\n\n")
		for _, evidence := range candidate.Evidence {
			builder.WriteString("- ")
			label := firstNonEmpty(evidence.RunID, evidence.Timestamp, "run")
			if evidence.RunURL != "" {
				fmt.Fprintf(&builder, "[%s](%s)", label, evidence.RunURL)
			} else {
				builder.WriteString(label)
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

	builder.WriteString("\n## Agent Task\n\n")
	if candidate.AgentTaskTitle != "" {
		fmt.Fprintf(&builder, "%s.\n\n", candidate.AgentTaskTitle)
	}
	builder.WriteString("Find the root cause, explain when the failure was first observed, and open a focused draft PR with validation evidence.\n")

	if candidate.TraceExcerpt != "" {
		fence := markdownCodeFence(candidate.TraceExcerpt)
		builder.WriteString("\n## Failure Excerpt\n\n")
		builder.WriteString(fence)
		builder.WriteString("text\n")
		builder.WriteString(candidate.TraceExcerpt)
		if !strings.HasSuffix(candidate.TraceExcerpt, "\n") {
			builder.WriteByte('\n')
		}
		builder.WriteString(fence)
		builder.WriteByte('\n')
	}

	return issueSpec{
		CandidateID: candidate.ID,
		Title:       title,
		Body:        builder.String(),
	}
}

func candidateTitle(candidate failureCandidate) string {
	switch {
	case candidate.Scope == "cross_matrix":
		return fmt.Sprintf("[ci-flaky] %s / %s (cross-matrix)", candidate.Suite, candidate.Name)
	case candidate.Matrix != "":
		return fmt.Sprintf("[ci-flaky] %s / %s [%s]", candidate.Suite, candidate.Name, candidate.Matrix)
	default:
		return fmt.Sprintf("[ci-flaky] %s / %s", candidate.Suite, candidate.Name)
	}
}

func findIssueByCandidateID(issues []githubIssue, candidateID string) *githubIssue {
	marker := markerPrefix + candidateID + " -->"
	for i := range issues {
		if issues[i].PullRequest != nil {
			continue
		}
		if strings.Contains(issues[i].Body, marker) {
			return &issues[i]
		}
	}
	return nil
}

func parseRepo(value string) (string, string, error) {
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("--repo must use owner/name form, got %q", value)
	}
	return parts[0], parts[1], nil
}

func defaultLabelDefinitions(labels []string) []githubLabel {
	definitions := make([]githubLabel, 0, len(labels))
	for _, label := range labels {
		switch label {
		case "ci-flaky":
			definitions = append(definitions, githubLabel{Name: label, Color: "d73a4a", Description: "Recurring CI test failure"})
		case "agent:flake-fix":
			definitions = append(definitions, githubLabel{Name: label, Color: "5319e7", Description: "Agent-ready flaky test fix task"})
		default:
			definitions = append(definitions, githubLabel{Name: label, Color: "ededed"})
		}
	}
	return definitions
}

func (client githubClient) ensureLabel(label githubLabel) error {
	path := fmt.Sprintf("/repos/%s/%s/labels/%s", client.owner, client.repo, url.PathEscape(label.Name))
	response, err := client.do("GET", path, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode == http.StatusOK {
		return nil
	}
	if response.StatusCode != http.StatusNotFound {
		return githubError(response)
	}

	payload := map[string]string{
		"name":        label.Name,
		"color":       label.Color,
		"description": label.Description,
	}
	response, err = client.do("POST", fmt.Sprintf("/repos/%s/%s/labels", client.owner, client.repo), payload)
	if err != nil {
		return err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
		return githubError(response)
	}
	return nil
}

func (client githubClient) listCandidateIssues(labels []string) ([]githubIssue, error) {
	query := url.Values{}
	query.Set("state", "all")
	query.Set("per_page", "100")
	query.Set("page", "1")
	if len(labels) > 0 {
		query.Set("labels", strings.Join(labels, ","))
	}
	path := fmt.Sprintf("/repos/%s/%s/issues?%s", client.owner, client.repo, query.Encode())

	var issues []githubIssue
	for {
		response, err := client.do("GET", path, nil)
		if err != nil {
			return nil, err
		}
		if response.StatusCode != http.StatusOK {
			err := githubError(response)
			_ = response.Body.Close()
			return nil, err
		}

		var pageIssues []githubIssue
		if err := json.NewDecoder(response.Body).Decode(&pageIssues); err != nil {
			_ = response.Body.Close()
			return nil, err
		}
		issues = append(issues, pageIssues...)

		nextPath, hasNext := nextLinkPath(response.Header.Get("Link"))
		if err := response.Body.Close(); err != nil {
			return nil, err
		}
		if !hasNext {
			break
		}
		path = nextPath
	}
	return issues, nil
}

func nextLinkPath(header string) (string, bool) {
	for _, link := range strings.Split(header, ",") {
		sections := strings.Split(link, ";")
		if len(sections) < 2 {
			continue
		}

		isNext := false
		for _, section := range sections[1:] {
			if strings.TrimSpace(section) == `rel="next"` {
				isNext = true
				break
			}
		}
		if !isNext {
			continue
		}

		target := strings.TrimSpace(sections[0])
		target = strings.TrimPrefix(strings.TrimSuffix(target, ">"), "<")
		parsed, err := url.Parse(target)
		if err != nil {
			continue
		}
		if parsed.IsAbs() {
			return parsed.RequestURI(), true
		}
		return target, true
	}
	return "", false
}

func (client githubClient) createIssue(spec issueSpec, labels []string) (githubIssue, error) {
	payload := map[string]any{
		"title":  spec.Title,
		"body":   spec.Body,
		"labels": labels,
	}
	response, err := client.do("POST", fmt.Sprintf("/repos/%s/%s/issues", client.owner, client.repo), payload)
	if err != nil {
		return githubIssue{}, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusCreated {
		return githubIssue{}, githubError(response)
	}
	var issue githubIssue
	if err := json.NewDecoder(response.Body).Decode(&issue); err != nil {
		return githubIssue{}, err
	}
	return issue, nil
}

func (client githubClient) updateIssue(number int, spec issueSpec, labels []string) (githubIssue, error) {
	payload := map[string]any{
		"title":  spec.Title,
		"body":   spec.Body,
		"labels": labels,
	}
	path := fmt.Sprintf("/repos/%s/%s/issues/%d", client.owner, client.repo, number)
	response, err := client.do("PATCH", path, payload)
	if err != nil {
		return githubIssue{}, err
	}
	defer func() {
		_ = response.Body.Close()
	}()
	if response.StatusCode != http.StatusOK {
		return githubIssue{}, githubError(response)
	}
	var issue githubIssue
	if err := json.NewDecoder(response.Body).Decode(&issue); err != nil {
		return githubIssue{}, err
	}
	return issue, nil
}

func (client githubClient) do(method, path string, payload any) (*http.Response, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, client.apiURL+path, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if client.token != "" {
		request.Header.Set("Authorization", "Bearer "+client.token)
	}
	return client.httpClient.Do(request)
}

func githubError(response *http.Response) error {
	content, _ := io.ReadAll(response.Body)
	return fmt.Errorf("github API %s failed with %s: %s", response.Request.URL.Path, response.Status, strings.TrimSpace(string(content)))
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

func shortSHA(value string) string {
	if len(value) <= 12 {
		return value
	}
	return value[:12]
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
