package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresSink struct {
	pool   *pgxpool.Pool
	schema string
}

func NewPostgresSink(ctx context.Context, dsn, schema string) (*PostgresSink, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return &PostgresSink{pool: pool, schema: schema}, nil
}

func (s *PostgresSink) Close() {
	s.pool.Close()
}

func (s *PostgresSink) EnsureTable(ctx context.Context, t *TableConfig) error {
	fqn := s.schema + "." + t.Target
	var cols []string
	for _, cm := range t.Columns {
		cols = append(cols, fmt.Sprintf("%s TEXT", cm.Target))
	}
	cols = append(cols, "_cdc_operation TEXT", "_cdc_timestamp TIMESTAMPTZ DEFAULT now()")

	pkCols := pkTargetNames(t)

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s,\n  PRIMARY KEY (%s)\n)",
		fqn, strings.Join(cols, ",\n  "), strings.Join(pkCols, ", "))

	_, err := s.pool.Exec(ctx, ddl)
	return err
}

func (s *PostgresSink) Upsert(ctx context.Context, t *TableConfig, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	fqn := s.schema + "." + t.Target
	nCols := len(t.Columns) + 1

	targetCols := make([]string, 0, nCols)
	for _, cm := range t.Columns {
		targetCols = append(targetCols, cm.Target)
	}
	targetCols = append(targetCols, "_cdc_operation")

	pkTargets := pkTargetNames(t)

	pkSet := make(map[string]bool)
	for _, p := range pkTargets {
		pkSet[p] = true
	}
	var updateParts []string
	for _, col := range targetCols {
		if !pkSet[col] {
			updateParts = append(updateParts, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
		}
	}
	updateParts = append(updateParts, "_cdc_timestamp = now()")

	var valueParts []string
	var args []interface{}
	argIdx := 1
	for _, row := range rows {
		var placeholders []string
		for _, v := range row {
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, sanitizeValue(v))
			argIdx++
		}
		placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
		args = append(args, "upsert")
		argIdx++
		valueParts = append(valueParts, "("+strings.Join(placeholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s ON CONFLICT (%s) DO UPDATE SET %s",
		fqn,
		strings.Join(targetCols, ", "),
		strings.Join(valueParts, ", "),
		strings.Join(pkTargets, ", "),
		strings.Join(updateParts, ", "),
	)

	_, err := s.pool.Exec(ctx, query, args...)
	return err
}

func (s *PostgresSink) Delete(ctx context.Context, t *TableConfig, keys [][]interface{}) error {
	if len(keys) == 0 {
		return nil
	}

	fqn := s.schema + "." + t.Target
	pkTargets := pkTargetNames(t)

	batch := &pgx.Batch{}
	for _, key := range keys {
		var conditions []string
		var args []interface{}
		for i, col := range pkTargets {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", col, i+1))
			args = append(args, key[i])
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", fqn, strings.Join(conditions, " AND "))
		batch.Queue(query, args...)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range keys {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}
	return nil
}

// pkTargetNames maps source PK column names to their target column names.
func pkTargetNames(t *TableConfig) []string {
	names := make([]string, len(t.PrimaryKey))
	for i, pk := range t.PrimaryKey {
		for _, cm := range t.Columns {
			if cm.Source == pk {
				names[i] = cm.Target
				break
			}
		}
	}
	return names
}
