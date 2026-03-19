package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/go-sql-driver/mysql"
	"gopkg.in/yaml.v3"
)

type ColumnMap struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

type StreamFilter struct {
	Column  string   `yaml:"column"`
	Values  []string `yaml:"values"`
	Pattern string   `yaml:"pattern"` // SQL LIKE pattern
}

type TableConfig struct {
	Source         string       `yaml:"source"`
	Target         string       `yaml:"target"`
	PrimaryKey     []string     `yaml:"primary_key"`
	Columns        []ColumnMap  `yaml:"columns"`
	SnapshotFilter string       `yaml:"snapshot_filter"`
	StreamFilter   StreamFilter `yaml:"stream_filter"`

	// Resolved at runtime: source column name → ordinal index in binlog row
	ColIndex map[string]int
	// Resolved at runtime: ordered list of source column ordinals matching Columns order
	SourceOrdinals []int
	// Primary key ordinals in the source table
	PKOrdinals []int
}

type SourceConfig struct {
	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	Flavor   string `yaml:"flavor"`
	ServerID uint32 `yaml:"server_id"`
}

type SinkConfig struct {
	Type string `yaml:"type"` // "postgres", "redis", "redis-json", "stdout"
	Mode string `yaml:"mode"` // "upsert" (default) or "log"
	DSN  string `yaml:"dsn"`  // postgres connection string
	Schema string `yaml:"schema"` // postgres schema

	// Redis options
	Addr     string `yaml:"addr"`     // redis address (e.g. "127.0.0.1:6379")
	Password string `yaml:"password"` // redis password
	DB       int    `yaml:"db"`       // redis database number
	Prefix   string `yaml:"prefix"`   // redis key prefix (default: "cdc")
}

// EffectiveMode returns the sink mode, defaulting to "upsert".
func (sc *SinkConfig) EffectiveMode() SinkMode {
	if sc.Mode == "log" {
		return SinkModeLog
	}
	return SinkModeUpsert
}

type Config struct {
	Source         SourceConfig  `yaml:"source"`
	Sink           SinkConfig    `yaml:"sink"`   // single sink (backwards compat)
	Sinks          []SinkConfig  `yaml:"sinks"`  // multiple sinks
	BatchSize      int           `yaml:"batch_size"`
	FlushInterval  string        `yaml:"flush_interval"`
	CheckpointFile string        `yaml:"checkpoint_file"`
	Tables         []TableConfig `yaml:"tables"`
}

// EffectiveSinks returns the list of sink configs, supporting both
// single "sink:" and multi "sinks:" YAML keys.
func (c *Config) EffectiveSinks() []SinkConfig {
	if len(c.Sinks) > 0 {
		return c.Sinks
	}
	// Backwards compat: single sink field
	if c.Sink.Type == "" && c.Sink.DSN != "" {
		c.Sink.Type = "postgres"
	}
	if c.Sink.Type != "" {
		return []SinkConfig{c.Sink}
	}
	return nil
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}
	if cfg.FlushInterval == "" {
		cfg.FlushInterval = "500ms"
	}
	if cfg.CheckpointFile == "" {
		cfg.CheckpointFile = "checkpoint.json"
	}
	return &cfg, nil
}

// ResolveOrdinals runs DESCRIBE on each source table and maps column names to
// their ordinal positions in the binlog row image.
func ResolveOrdinals(cfg *Config) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", cfg.Source.User, cfg.Source.Password, cfg.Source.Addr, cfg.Source.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connect to source: %w", err)
	}
	defer db.Close()

	for i := range cfg.Tables {
		t := &cfg.Tables[i]
		rows, err := db.Query("DESCRIBE " + t.Source)
		if err != nil {
			return fmt.Errorf("describe %s: %w", t.Source, err)
		}
		t.ColIndex = make(map[string]int)
		ordinal := 0
		for rows.Next() {
			var field, colType, null, key string
			var defVal, extra sql.NullString
			if err := rows.Scan(&field, &colType, &null, &key, &defVal, &extra); err != nil {
				rows.Close()
				return fmt.Errorf("scan describe %s: %w", t.Source, err)
			}
			t.ColIndex[field] = ordinal
			ordinal++
		}
		rows.Close()

		// Build ordered source ordinals for the configured columns
		t.SourceOrdinals = make([]int, len(t.Columns))
		for j, cm := range t.Columns {
			idx, ok := t.ColIndex[cm.Source]
			if !ok {
				return fmt.Errorf("column %q not found in %s", cm.Source, t.Source)
			}
			t.SourceOrdinals[j] = idx
		}

		// Build PK ordinals
		t.PKOrdinals = make([]int, len(t.PrimaryKey))
		for j, pk := range t.PrimaryKey {
			idx, ok := t.ColIndex[pk]
			if !ok {
				return fmt.Errorf("pk column %q not found in %s", pk, t.Source)
			}
			t.PKOrdinals[j] = idx
		}
	}
	return nil
}
