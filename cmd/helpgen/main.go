// helpgen generates help text files and MDX documentation from YAML command definitions.
// This ensures documentation and console help stay in sync from a single source of truth.
//
// Usage:
//
//	go run ./cmd/helpgen
//
// Or via make:
//
//	make generate_help
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	"gopkg.in/yaml.v3"
)

// Command represents a console command from YAML source
type Command struct {
	Name            string       `yaml:"name"`
	Order           int          `yaml:"order"`
	Description     string       `yaml:"description"`
	Syntax          string       `yaml:"syntax"`
	Parameters      []Param      `yaml:"parameters"`
	Examples        []Example    `yaml:"examples"`
	Tips            []string     `yaml:"tips"`
	Warnings        []string     `yaml:"warnings"`
	Notes           []string     `yaml:"notes"`
	RelatedCommands []string     `yaml:"related_commands"`
	Snippet         string       `yaml:"snippet"`
	Subcommands     []Subcommand `yaml:"subcommands"`
}

// Subcommand represents a sub-variant of a command rendered as a separate MDX section
type Subcommand struct {
	Name        string    `yaml:"name"`
	Description string    `yaml:"description"`
	Syntax      string    `yaml:"syntax"`
	Examples    []Example `yaml:"examples"`
	Warnings    []string  `yaml:"warnings"`
	Notes       []string  `yaml:"notes"`
}

type Param struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

type Example struct {
	Description string `yaml:"description"`
	Code        string `yaml:"code"`
	Output      string `yaml:"output"`
}

const helpTemplate = `NAME
    {{.Name}} - {{firstLine .Description}}

SYNTAX
{{indent .Syntax 4}}

DESCRIPTION
{{indent .Description 4}}
{{- if .Warnings}}

WARNINGS
{{- range .Warnings}}
    - {{.}}
{{- end}}
{{- end}}
{{- if .Parameters}}

PARAMETERS
{{- range .Parameters}}
    <{{.Name}}> - {{.Description}}
{{- end}}
{{- end}}
{{- if .Examples}}

EXAMPLES
{{- range .Examples}}
    {{.Description}}:
{{indent .Code 8}}
{{- if .Output}}
    Output:
{{indent .Output 8}}
{{- end}}
{{- end}}
{{- end}}
{{- if .Tips}}

TIPS
{{- range .Tips}}
    - {{.}}
{{- end}}
{{- end}}
{{- if .RelatedCommands}}

RELATED COMMANDS
{{- range .RelatedCommands}}
    {{.}}
{{- end}}
{{- end}}
`

const mdxCommandTemplate = `### {{.Name}}

{{.Description}}
{{- if .Warnings}}
{{range .Warnings}}
<Warning>{{.}}</Warning>
{{end}}
{{- end}}
{{- if .Notes}}
{{range .Notes}}
<Note>{{.}}</Note>
{{end}}
{{- end}}

` + "```sql" + `
{{trimSpace .Syntax}}
` + "```" + `
{{- if .Examples}}

**Examples:**

` + "```sql" + `
{{- range .Examples}}
-- {{.Description}}
{{.Code}}
{{- if .Output}}
{{.Output}}
{{- end}}
{{- end}}
` + "```" + `
{{- end}}

`

const mdxSubcommandTemplate = `### {{.Name}}

{{.Description}}
{{- if .Warnings}}
{{range .Warnings}}
<Warning>{{.}}</Warning>
{{end}}
{{- end}}
{{- if .Notes}}
{{range .Notes}}
<Note>{{.}}</Note>
{{end}}
{{- end}}

` + "```sql" + `
{{trimSpace .Syntax}}
` + "```" + `
{{- if .Examples}}

**Examples:**

` + "```sql" + `
{{- range .Examples}}
-- {{.Description}}
{{.Code}}
{{- if .Output}}
{{.Output}}
{{- end}}
{{- end}}
` + "```" + `
{{- end}}

`

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "helpgen: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	root, err := findProjectRoot()
	if err != nil {
		return fmt.Errorf("finding project root: %w", err)
	}

	commandsDir := filepath.Join(root, "yacc", "console", "commands")
	helpDir := filepath.Join(root, "yacc", "console", "help")

	commands, err := loadCommands(commandsDir)
	if err != nil {
		return fmt.Errorf("loading commands: %w", err)
	}

	if len(commands) == 0 {
		return fmt.Errorf("no command YAML files found in %s", commandsDir)
	}

	fmt.Printf("Loaded %d commands from %s\n", len(commands), commandsDir)

	if err := generateHelpFiles(commands, helpDir); err != nil {
		return fmt.Errorf("generating help files: %w", err)
	}

	snippetsDir := filepath.Join(root, "docs", "snippets")

	snippetGroups := make(map[string][]*Command)
	for _, cmd := range commands {
		if cmd.Snippet == "" {
			return fmt.Errorf("command %q is missing required 'snippet' field", cmd.Name)
		}
		snippetGroups[cmd.Snippet] = append(snippetGroups[cmd.Snippet], cmd)
	}

	for snippet, cmds := range snippetGroups {
		snippetPath := filepath.Join(snippetsDir, snippet+".mdx")
		if err := generateMDXSnippet(cmds, snippetPath); err != nil {
			return fmt.Errorf("generating MDX snippet %s: %w", snippet, err)
		}
	}

	fmt.Println("Done!")
	return nil
}

func findProjectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("could not find go.mod in any parent directory")
		}
		dir = parent
	}
}

func loadCommands(dir string) ([]*Command, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading directory %s: %w", dir, err)
	}

	var commands []*Command
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", path, err)
		}

		var cmd Command
		if err := yaml.Unmarshal(data, &cmd); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", path, err)
		}

		commands = append(commands, &cmd)
	}

	sort.Slice(commands, func(i, j int) bool {
		return commands[i].Name < commands[j].Name
	})

	return commands, nil
}

func generateHelpFiles(commands []*Command, dir string) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("creating directory %s: %w", dir, err)
	}

	funcMap := template.FuncMap{
		"firstLine": func(s string) string {
			lines := strings.SplitN(strings.TrimSpace(s), "\n", 2)
			if len(lines) > 0 {
				return strings.TrimSpace(lines[0])
			}
			return ""
		},
		"indent": func(s string, spaces int) string {
			prefix := strings.Repeat(" ", spaces)
			lines := strings.Split(strings.TrimSpace(s), "\n")
			for i, line := range lines {
				if line != "" {
					lines[i] = prefix + line
				}
			}
			return strings.Join(lines, "\n")
		},
	}

	tmpl, err := template.New("help").Funcs(funcMap).Parse(helpTemplate)
	if err != nil {
		return fmt.Errorf("parsing help template: %w", err)
	}

	for _, cmd := range commands {
		filename := strings.ReplaceAll(cmd.Name, " ", "_") + ".txt"
		path := filepath.Join(dir, filename)

		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, cmd); err != nil {
			return fmt.Errorf("executing template for %s: %w", cmd.Name, err)
		}

		if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
			return fmt.Errorf("writing %s: %w", path, err)
		}

		fmt.Printf("  Generated help/%s\n", filename)
	}

	return nil
}

func generateMDXSnippet(commands []*Command, path string) error {
	funcMap := template.FuncMap{
		"trimSpace": strings.TrimSpace,
	}

	cmdTmpl, err := template.New("mdx").Funcs(funcMap).Parse(mdxCommandTemplate)
	if err != nil {
		return fmt.Errorf("parsing MDX template: %w", err)
	}

	subTmpl, err := template.New("mdxSub").Funcs(funcMap).Parse(mdxSubcommandTemplate)
	if err != nil {
		return fmt.Errorf("parsing MDX subcommand template: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("creating directory for %s: %w", path, err)
	}

	sort.Slice(commands, func(i, j int) bool {
		if commands[i].Order != commands[j].Order {
			return commands[i].Order < commands[j].Order
		}
		return commands[i].Name < commands[j].Name
	})

	var buf bytes.Buffer
	buf.WriteString("{/* Auto-generated by cmd/helpgen. Do not edit manually. */}\n\n")

	for _, cmd := range commands {
		if len(cmd.Subcommands) > 0 {
			// Render each subcommand as a separate ### section
			for _, sub := range cmd.Subcommands {
				if err := subTmpl.Execute(&buf, sub); err != nil {
					return fmt.Errorf("executing subcommand template for %s: %w", sub.Name, err)
				}
			}
		} else {
			if err := cmdTmpl.Execute(&buf, cmd); err != nil {
				return fmt.Errorf("executing template for %s: %w", cmd.Name, err)
			}
		}
	}

	if err := os.WriteFile(path, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("writing %s: %w", path, err)
	}

	fmt.Printf("  Generated %s\n", path)
	return nil
}
