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

func (s *PostgresSink) EnsureTable(ctx context.Context, t *TableConfig, mode SinkMode) error {
	fqn := s.schema + "." + t.Target
	if mode == SinkModeLog {
		return s.ensureLogTable(ctx, fqn, t)
	}
	return s.ensureUpsertTable(ctx, fqn, t)
}

func (s *PostgresSink) ensureUpsertTable(ctx context.Context, fqn string, t *TableConfig) error {
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

func (s *PostgresSink) ensureLogTable(ctx context.Context, fqn string, t *TableConfig) error {
	var cols []string
	cols = append(cols, "_cdc_seq BIGSERIAL")
	for _, cm := range t.Columns {
		cols = append(cols, fmt.Sprintf("%s TEXT", cm.Target))
	}
	cols = append(cols,
		"_cdc_kind TEXT NOT NULL",
		"_cdc_timestamp TIMESTAMPTZ DEFAULT now()",
	)

	ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s,\n  PRIMARY KEY (_cdc_seq)\n)",
		fqn, strings.Join(cols, ",\n  "))

	_, err := s.pool.Exec(ctx, ddl)
	if err != nil {
		return err
	}

	// Create index on PK columns + timestamp for efficient querying
	pkCols := pkTargetNames(t)
	idxName := strings.ReplaceAll(fqn, ".", "_") + "_pk_idx"
	idxCols := append(pkCols, "_cdc_timestamp")
	idxDDL := fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s (%s)",
		idxName, fqn, strings.Join(idxCols, ", "))
	_, err = s.pool.Exec(ctx, idxDDL)
	return err
}

func (s *PostgresSink) WriteBatch(ctx context.Context, rows []CDCRow, mode SinkMode) error {
	if len(rows) == 0 {
		return nil
	}
	if mode == SinkModeLog {
		return s.writeLog(ctx, rows)
	}
	return s.writeUpsert(ctx, rows)
}

func (s *PostgresSink) writeUpsert(ctx context.Context, rows []CDCRow) error {
	// Group by table
	type tableRows struct {
		upserts []CDCRow
		deletes []CDCRow
	}
	grouped := make(map[string]*tableRows)

	for _, r := range rows {
		key := r.Table.Source
		tr, ok := grouped[key]
		if !ok {
			tr = &tableRows{}
			grouped[key] = tr
		}
		switch r.Kind {
		case RowKindDelete:
			tr.deletes = append(tr.deletes, r)
		case RowKindInsert, RowKindUpdateAfter:
			tr.upserts = append(tr.upserts, r)
		// RowKindUpdateBefore is ignored in upsert mode
		}
	}

	for _, tr := range grouped {
		if len(tr.upserts) > 0 {
			if err := s.upsertRows(ctx, tr.upserts); err != nil {
				return err
			}
		}
		if len(tr.deletes) > 0 {
			if err := s.deleteRows(ctx, tr.deletes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *PostgresSink) upsertRows(ctx context.Context, rows []CDCRow) error {
	t := rows[0].Table
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
	for _, r := range rows {
		var placeholders []string
		for _, v := range r.Values {
			placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
			args = append(args, sanitizeValue(v))
			argIdx++
		}
		placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
		args = append(args, string(r.Kind))
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

func (s *PostgresSink) deleteRows(ctx context.Context, rows []CDCRow) error {
	t := rows[0].Table
	fqn := s.schema + "." + t.Target
	pkTargets := pkTargetNames(t)

	batch := &pgx.Batch{}
	for _, r := range rows {
		var conditions []string
		var args []interface{}
		for i, col := range pkTargets {
			conditions = append(conditions, fmt.Sprintf("%s = $%d", col, i+1))
			args = append(args, r.PKVals[i])
		}
		query := fmt.Sprintf("DELETE FROM %s WHERE %s", fqn, strings.Join(conditions, " AND "))
		batch.Queue(query, args...)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range rows {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("delete: %w", err)
		}
	}
	return nil
}

func (s *PostgresSink) writeLog(ctx context.Context, rows []CDCRow) error {
	// Group by table for batched inserts
	grouped := make(map[string][]CDCRow)
	for _, r := range rows {
		grouped[r.Table.Source] = append(grouped[r.Table.Source], r)
	}

	for _, tableRows := range grouped {
		if err := s.insertLogRows(ctx, tableRows); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresSink) insertLogRows(ctx context.Context, rows []CDCRow) error {
	t := rows[0].Table
	fqn := s.schema + "." + t.Target

	// Columns: all mapped columns + _cdc_kind
	targetCols := make([]string, 0, len(t.Columns)+1)
	for _, cm := range t.Columns {
		targetCols = append(targetCols, cm.Target)
	}
	targetCols = append(targetCols, "_cdc_kind")

	var valueParts []string
	var args []interface{}
	argIdx := 1
	for _, r := range rows {
		var placeholders []string

		if r.Values != nil {
			for _, v := range r.Values {
				placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
				args = append(args, sanitizeValue(v))
				argIdx++
			}
		} else {
			// Delete with only PK vals — fill columns with NULLs except PKs
			pkTargets := pkTargetNames(t)
			pkMap := make(map[string]interface{})
			for i, name := range pkTargets {
				if i < len(r.PKVals) {
					pkMap[name] = r.PKVals[i]
				}
			}
			for _, cm := range t.Columns {
				placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
				if v, ok := pkMap[cm.Target]; ok {
					args = append(args, v)
				} else {
					args = append(args, nil)
				}
				argIdx++
			}
		}

		// _cdc_kind
		placeholders = append(placeholders, fmt.Sprintf("$%d", argIdx))
		args = append(args, string(r.Kind))
		argIdx++

		valueParts = append(valueParts, "("+strings.Join(placeholders, ", ")+")")
	}

	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		fqn,
		strings.Join(targetCols, ", "),
		strings.Join(valueParts, ", "),
	)

	_, err := s.pool.Exec(ctx, query, args...)
	return err
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
