package api

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.kenn.io/msgvault/internal/clirun"
)

// The daemon-only CLI model means a command missing from the run allowlist
// silently fails end-to-end with no compile-time signal, so assert the
// iPhone voicemail command and its password env var are permitted.
func TestCLIRunCommandAllowedVoicemail(t *testing.T) {
	for _, args := range [][]string{
		{"import-iphone-voicemail"},
		{"import-iphone-voicemail", "--backup-path", "/x", "--after", "2024-01-01"},
	} {
		t.Run(args[0], func(t *testing.T) {
			assert.True(t, cliRunCommandAllowed(args),
				"%v must be runnable via the daemon CLI", args)
		})
	}

	assert.True(t, clirun.EnvAllowed(clirun.EnvBackupPassword),
		"backup password env var must be forwardable to the daemon CLI subprocess")
}
