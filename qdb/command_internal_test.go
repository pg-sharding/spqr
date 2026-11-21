package qdb

import (
	"errors"
	"fmt"
	"testing"
)

func TestExecuteCommandsAfterBackupRename(t *testing.T) {
	state := map[string]int{"key": 1}
	err := ExecuteCommands(func() error {
		return fmt.Errorf("%w: %w", errBackupRenamed, errors.New("sync failed"))
	}, NewUpdateCommand(state, "key", 2))
	if !errors.Is(err, errBackupRenamed) {
		t.Fatalf("expected directory sync error, got %v", err)
	}
	if state["key"] != 2 {
		t.Fatalf("state rolled back after backup replacement: %v", state)
	}
}
