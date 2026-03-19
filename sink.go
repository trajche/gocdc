package main

import "context"

// RowKind represents the type of change, following Flink CDC conventions.
type RowKind string

const (
	RowKindInsert       RowKind = "+I" // insert
	RowKindUpdateBefore RowKind = "-U" // before image of an update
	RowKindUpdateAfter  RowKind = "+U" // after image of an update
	RowKindDelete       RowKind = "-D" // delete
)

// CDCRow is a single change event flowing through the pipeline.
type CDCRow struct {
	Table  *TableConfig
	Kind   RowKind
	Values []interface{} // column values in t.Columns order (nil for delete if only PKs available)
	PKVals []interface{} // primary key values (always set)
}

// SinkMode controls how sinks handle incoming change events.
type SinkMode string

const (
	SinkModeUpsert SinkMode = "upsert" // compact: latest state only (default)
	SinkModeLog    SinkMode = "log"    // append: every change is a new row
)

// Sink is the interface that all CDC sink backends must implement.
type Sink interface {
	// EnsureTable prepares the target storage for a table.
	EnsureTable(ctx context.Context, t *TableConfig, mode SinkMode) error
	// WriteBatch writes a batch of CDC rows to the sink.
	WriteBatch(ctx context.Context, rows []CDCRow, mode SinkMode) error
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
