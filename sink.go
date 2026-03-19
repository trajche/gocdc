package main

import "context"

// Sink is the interface that all CDC sink backends must implement.
type Sink interface {
	// EnsureTable prepares the target storage for a table (e.g. CREATE TABLE, ensure key namespace).
	EnsureTable(ctx context.Context, t *TableConfig) error
	// Upsert writes rows to the target. Each row is a slice of values in t.Columns order.
	Upsert(ctx context.Context, t *TableConfig, rows [][]interface{}) error
	// Delete removes rows by primary key values.
	Delete(ctx context.Context, t *TableConfig, keys [][]interface{}) error
	// Close releases resources.
	Close()
}

// sanitizeValue converts MySQL-specific values (like zero dates) to
// sink-compatible equivalents.
func sanitizeValue(v interface{}) interface{} {
	if s, ok := v.(string); ok {
		if s == "0000-00-00 00:00:00" || s == "0000-00-00" {
			return nil
		}
	}
	return v
}
