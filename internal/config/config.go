// Package config loads the sensor YAML configuration.
package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Registry defines a single OCI registry to scan.
type Registry struct {
	URL      string `yaml:"url"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
}

// Config is the top-level sensor configuration.
type Config struct {
	Subscribers  []string `yaml:"subscribers"`
	PollInterval string   `yaml:"pollInterval,omitempty"`
	Registries   []Registry `yaml:"registries"`
}

// Load reads and parses the YAML config file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if len(cfg.Registries) == 0 {
		return nil, fmt.Errorf("config %s: at least one registry must be defined", path)
	}
	return &cfg, nil
}
