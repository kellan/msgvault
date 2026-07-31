package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOffloadConfigDefaults(t *testing.T) {
	assert := assert.New(t)
	cfg := NewDefaultConfig()

	assert.False(cfg.Offload.Enabled())
	assert.Equal(DefaultOffloadMaxSnapshotAgeDays, cfg.Offload.MaxSnapshotAgeDays)
}

func TestOffloadConfigLoad(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	require.NoError(os.WriteFile(path, []byte(`
[offload]
repo = "backups/msgvault"
`), 0o600))

	cfg, err := Load(path, "")
	require.NoError(err)

	assert.True(cfg.Offload.Enabled())
	// Relative paths resolve against the config file's directory when
	// --config is explicit, mirroring [backup] repo.
	assert.Equal(filepath.Join(tmpDir, "backups", "msgvault"), cfg.Offload.Repo)
	// Zero-valued age still gets the default re-applied after decode.
	assert.Equal(DefaultOffloadMaxSnapshotAgeDays, cfg.Offload.MaxSnapshotAgeDays)
}

func TestOffloadConfigTildeExpansion(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	require.NoError(os.WriteFile(path, []byte(`
[offload]
repo = "~/Backups/msgvault"
`), 0o600))

	cfg, err := Load(path, "")
	require.NoError(err)

	home, err := os.UserHomeDir()
	require.NoError(err)
	assert.Equal(filepath.Join(home, "Backups", "msgvault"), cfg.Offload.Repo)
}

func TestOffloadConfigAcceptsS3URLs(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	require.NoError(os.WriteFile(path, []byte(`
[offload]
repo = "s3://vault-bucket/msgvault/repo"
s3_endpoint = "https://s3.us-west-000.backblazeb2.com"
s3_region = "us-west-000"
`), 0o600))

	cfg, err := Load(path, "")
	require.NoError(err)
	assert.Equal("s3://vault-bucket/msgvault/repo", cfg.Offload.Repo,
		"s3 URLs bypass path expansion and relative resolution")
	assert.Equal("https://s3.us-west-000.backblazeb2.com", cfg.Offload.S3Endpoint)
	assert.Equal("us-west-000", cfg.Offload.S3Region)
}

func TestOffloadConfigRejectsRemoteSchemes(t *testing.T) {
	for _, repo := range []string{
		"https://host/repo",
		"http://host/repo",
	} {
		t.Run(repo, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			tmpDir := t.TempDir()
			path := filepath.Join(tmpDir, "config.toml")
			require.NoError(os.WriteFile(path, []byte(`
[offload]
repo = "`+repo+`"
`), 0o600))

			_, err := Load(path, "")
			require.Error(err)
			assert.Contains(err.Error(), "not yet supported")
		})
	}
}

func TestOffloadConfigRejectsNegativeAge(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.toml")
	require.NoError(os.WriteFile(path, []byte(`
[offload]
repo = "backups"
max_snapshot_age_days = -3
`), 0o600))

	_, err := Load(path, "")
	require.Error(err)
	assert.Contains(err.Error(), "max_snapshot_age_days")
}
