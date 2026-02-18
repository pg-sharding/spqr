package spqrparser

import (
	"embed"
	"fmt"
	"strings"
	"sync"
)

//go:embed help/*.txt
var helpFS embed.FS

type HelpEntry struct {
	Name    string
	Content string
}

var (
	helpRegistry map[string]*HelpEntry
	registryMu   sync.RWMutex
	initialized  bool
)

// InitHelpRegistry loads all help files from the embedded filesystem
func InitHelpRegistry() error {
	registryMu.Lock()
	defer registryMu.Unlock()

	if initialized {
		return nil
	}

	helpRegistry = make(map[string]*HelpEntry)

	// Read all .txt files from embedded help directory
	entries, err := helpFS.ReadDir("help")
	if err != nil {
		return fmt.Errorf("failed to read embedded help directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".txt") {
			content, err := helpFS.ReadFile("help/" + entry.Name())
			if err != nil {
				return fmt.Errorf("failed to read help file %s: %w", entry.Name(), err)
			}

			// Extract command name from filename (e.g., "CREATE_KEY_RANGE.txt" -> "CREATE KEY RANGE")
			commandName := strings.TrimSuffix(entry.Name(), ".txt")
			commandName = strings.ReplaceAll(commandName, "_", " ")

			helpRegistry[commandName] = &HelpEntry{
				Name:    commandName,
				Content: string(content),
			}
		}
	}

	initialized = true
	return nil
}

// GetHelp retrieves help for a specific command
func GetHelp(commandName string) (*HelpEntry, error) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	if !initialized {
		return nil, fmt.Errorf("help registry not initialized")
	}

	help, exists := helpRegistry[commandName]
	if !exists {
		return nil, fmt.Errorf("no help available for command: %s", commandName)
	}

	return help, nil
}

// ListAvailableCommands returns list of all available help commands
func ListAvailableCommands() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	commands := make([]string, 0, len(helpRegistry))
	for cmd := range helpRegistry {
		commands = append(commands, cmd)
	}
	return commands
}
