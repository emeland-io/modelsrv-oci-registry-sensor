package config_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"emeland.io/modelsrv-oci-registry-sensor/internal/config"
)

func TestLoad_Valid(t *testing.T) {
	content := `
subscribers:
  - "http://localhost:8080/api/"
pollInterval: "30s"
registries:
  - url: "registry.example.com"
    username: "user"
    password: "pass"
  - url: "ghcr.io"
    repositories:
      - "emeland-io/modelsrv"
`
	path := writeTempFile(t, content)
	cfg, err := config.Load(path)
	require.NoError(t, err)

	assert.Equal(t, []string{"http://localhost:8080/api/"}, cfg.Subscribers)
	assert.Equal(t, "30s", cfg.PollInterval)
	assert.Len(t, cfg.Registries, 2)
	assert.Equal(t, "registry.example.com", cfg.Registries[0].URL)
	assert.Equal(t, "user", cfg.Registries[0].Username)
	assert.Equal(t, "pass", cfg.Registries[0].Password)
	assert.Equal(t, "ghcr.io", cfg.Registries[1].URL)
	assert.Empty(t, cfg.Registries[1].Username)
	assert.Equal(t, []string{"emeland-io/modelsrv"}, cfg.Registries[1].Repositories)
}

func TestLoad_NoRegistries(t *testing.T) {
	content := `
subscribers:
  - "http://localhost:8080/api/"
registries: []
`
	path := writeTempFile(t, content)
	_, err := config.Load(path)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "at least one registry")
}

func TestLoad_InvalidYAML(t *testing.T) {
	path := writeTempFile(t, "{{invalid")
	_, err := config.Load(path)
	assert.Error(t, err)
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := config.Load("/nonexistent/path.yaml")
	assert.Error(t, err)
}

func writeTempFile(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	return path
}
