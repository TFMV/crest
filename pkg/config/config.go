package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	Flight   FlightConfig   `yaml:"flight"`
	Iceberg  IcebergConfig  `yaml:"iceberg"`
	Storage  StorageConfig  `yaml:"storage"`
	Batching BatchingConfig `yaml:"batching"`
	Catalog  CatalogConfig  `yaml:"catalog"`
	Metrics  MetricsConfig  `yaml:"metrics"`
}

// FlightConfig represents the Arrow Flight configuration
type FlightConfig struct {
	Servers []string `yaml:"servers"`
	Port    int      `yaml:"port"`
}

// IcebergConfig represents the Iceberg configuration
type IcebergConfig struct {
	DefaultNamespace string `yaml:"defaultNamespace"`
}

// StorageConfig represents the storage configuration
type StorageConfig struct {
	Type      string `yaml:"type"`
	LocalPath string `yaml:"localPath"`
	S3Bucket  string `yaml:"s3Bucket,omitempty"`
	S3Region  string `yaml:"s3Region,omitempty"`
}

// BatchingConfig represents the batching configuration
type BatchingConfig struct {
	MaxRows       int           `yaml:"maxRows"`
	MaxTimeWindow time.Duration `yaml:"maxTimeWindow"`
}

// CatalogConfig represents the Lakekeeper catalog configuration
type CatalogConfig struct {
	Endpoint  string `yaml:"endpoint"`
	Type      string `yaml:"type"`
	Warehouse string `yaml:"warehouse"`
}

// MetricsConfig represents the metrics configuration
type MetricsConfig struct {
	Enabled        bool   `yaml:"enabled"`
	CollectionPath string `yaml:"collectionPath"`
}

// LoadConfig loads the configuration from the specified file
func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set default values if not specified
	if cfg.Batching.MaxRows == 0 {
		cfg.Batching.MaxRows = 1000
	}
	if cfg.Batching.MaxTimeWindow == 0 {
		cfg.Batching.MaxTimeWindow = 60 * time.Second
	}
	if cfg.Storage.Type == "" {
		cfg.Storage.Type = "local"
	}
	if cfg.Storage.LocalPath == "" {
		cfg.Storage.LocalPath = "data"
	}
	if cfg.Iceberg.DefaultNamespace == "" {
		cfg.Iceberg.DefaultNamespace = "default"
	}

	return &cfg, nil
}
