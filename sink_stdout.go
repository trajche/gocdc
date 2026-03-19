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

func (s *StdoutSink) EnsureTable(_ context.Context, _ *TableConfig) error {
	return nil
}

func (s *StdoutSink) Upsert(_ context.Context, t *TableConfig, rows [][]interface{}) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, row := range rows {
		rec := map[string]interface{}{
			"_table":          t.Target,
			"_cdc_operation":  "upsert",
			"_cdc_timestamp":  now,
		}
		for i, cm := range t.Columns {
			rec[cm.Target] = sanitizeValue(row[i])
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		fmt.Fprintln(os.Stdout, string(data))
	}
	return nil
}

func (s *StdoutSink) Delete(_ context.Context, t *TableConfig, keys [][]interface{}) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	pkTargets := pkTargetNames(t)
	for _, pkVals := range keys {
		rec := map[string]interface{}{
			"_table":          t.Target,
			"_cdc_operation":  "delete",
			"_cdc_timestamp":  now,
		}
		for i, col := range pkTargets {
			rec[col] = pkVals[i]
		}
		data, err := json.Marshal(rec)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		fmt.Fprintln(os.Stdout, string(data))
	}
	return nil
}
