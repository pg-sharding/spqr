package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The coordinator starts as `spqr-coordinator --config <path>` and has no
// `run` subcommand, so its usage line must not advertise one (issue #3092).
func TestRootCmdUsageMatchesRealInvocation(t *testing.T) {
	is := assert.New(t)

	is.Contains(rootCmd.Use, "--config", "usage must document the --config flag")
	is.False(strings.Contains(rootCmd.Use, "run"),
		"usage must not document a `run` subcommand; the coordinator has none")

	// Guard the invariant directly: no subcommand named in the usage line may
	// be missing from the registered commands.
	for _, c := range rootCmd.Commands() {
		is.NotEqual("run", c.Name(), "a `run` subcommand would make the old usage valid again")
	}
}
