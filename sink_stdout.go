package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// StdoutSink writes CDC events as JSON lines to stdout.
type StdoutSink struct{}

func NewStdoutSink() *StdoutSink {
	return &StdoutSink{}
}

func (s *StdoutSink) Close() {}

func (s *StdoutSink) EnsureTable(_ context.Context, _ *TableConfig, _ SinkMode) error {
	return nil
}

func (s *StdoutSink) WriteBatch(_ context.Context, rows []CDCRow, _ SinkMode) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, r := range rows {
		rec := map[string]interface{}{
			"_table":         r.Table.Target,
			"_cdc_kind":      string(r.Kind),
			"_cdc_timestamp": now,
		}
		if r.Values != nil {
			for i, cm := range r.Table.Columns {
				rec[cm.Target] = sanitizeValue(r.Values[i])
			}
		} else {
			pkTargets := pkTargetNames(r.Table)
			for i, name := range pkTargets {
				if i < len(r.PKVals) {
					rec[name] = r.PKVals[i]
				}
			}
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		fmt.Fprintln(os.Stdout, string(data))
	}
	return nil
}
