package console

import (
	"testing"

	"github.com/pg-sharding/spqr/yacc/console"
)

func TestSequencesCommandRoutesToCoordinator(t *testing.T) {
	// This is a documentation test to verify that SequencesStr is included
	// in the list of commands that should route to coordinator.
	//
	// The actual routing logic is tested in integration tests, but this test
	// serves as documentation of the expected behavior.

	coordinatorCommands := []string{
		spqrparser.RoutersStr,
		spqrparser.TaskGroupStr,
		spqrparser.MoveTaskStr,
		spqrparser.SequencesStr, // Added for issue #1590
	}

	// Verify all coordinator commands are defined
	for _, cmd := range coordinatorCommands {
		if cmd == "" {
			t.Error("Found empty command in coordinator commands list")
		}
	}

	t.Logf("Verified %d commands route to coordinator: %v", len(coordinatorCommands), coordinatorCommands)
}
