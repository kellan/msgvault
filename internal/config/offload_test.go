package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffloadConfigDefaults(t *testing.T) {
	cfg := NewDefaultConfig()

	assert.False(t, cfg.Offload.Enabled())
	assert.Equal(t, DefaultOffloadMaxSnapshotAgeDays, cfg.Offload.MaxSnapshotAgeDays)
}

func TestOffloadConfigLoad(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[offload]
repo = "backups/msgvault"
`), 0o600))

	cfg, err := Load(path, "")
	require.NoError(t, err)

	assert.True(t, cfg.Offload.Enabled())
	// Relative paths resolve against the config file's directory when
	// --config is explicit, mirroring [backup] repo.
	assert.Equal(t, filepath.Join(tmpDir, "backups", "msgvault"), cfg.Offload.Repo)
	// Zero-valued age still gets the default re-applied after decode.
	assert.Equal(t, DefaultOffloadMaxSnapshotAgeDays, cfg.Offload.MaxSnapshotAgeDays)
}

func TestOffloadConfigTildeExpansion(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[offload]
repo = "~/Backups/msgvault"
`), 0o600))

	cfg, err := Load(path, "")
	require.NoError(t, err)

	home, err := os.UserHomeDir()
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(home, "Backups", "msgvault"), cfg.Offload.Repo)
}

func TestOffloadConfigRejectsRemoteSchemes(t *testing.T) {
	for _, repo := range []string{
		"s3://bucket/prefix",
		"https://host/repo",
		"http://host/repo",
	} {
		t.Run(repo, func(t *testing.T) {
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "config.toml")
			require.NoError(t, os.WriteFile(path, []byte(`
[offload]
repo = "`+repo+`"
`), 0o600))

			_, err := Load(path, "")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "not yet supported")
		})
	}
}

func TestOffloadConfigRejectsNegativeAge(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	require.NoError(t, os.WriteFile(path, []byte(`
[offload]
repo = "backups"
max_snapshot_age_days = -3
`), 0o600))

	_, err := Load(path, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max_snapshot_age_days")
}
