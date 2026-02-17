package spqrparser

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type HelpEntry struct {
	Name        string
	Content     string
}

var (
	helpRegistry map[string]*HelpEntry
	registryMu   sync.RWMutex
	initialized  bool
)

// InitHelpRegistry loads all help files from the help directory
func InitHelpRegistry(helpDir string) error {
	registryMu.Lock()
	defer registryMu.Unlock()

	if initialized {
		return nil
	}

	helpRegistry = make(map[string]*HelpEntry)

	// Check if help directory exists
	if _, err := os.Stat(helpDir); os.IsNotExist(err) {
		// Help directory doesn't exist, this is OK - continue with empty registry
		initialized = true
		return nil
	}

	// Read all .txt files from help directory
	entries, err := os.ReadDir(helpDir)
	if err != nil {
		return fmt.Errorf("failed to read help directory %s: %w", helpDir, err)
	}

	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".txt") {
			filePath := filepath.Join(helpDir, entry.Name())
			content, err := os.ReadFile(filePath)
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
