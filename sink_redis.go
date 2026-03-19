package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisSink writes CDC rows to Redis as hash keys.
// In upsert mode: HSET for inserts/updates, DEL for deletes.
// In log mode: RPUSH a JSON event onto a list key {prefix}:{table}:{pk}:log
type RedisSink struct {
	client *redis.Client
	prefix string
}

func NewRedisSink(ctx context.Context, addr, password string, db int, prefix string) (*RedisSink, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connect to redis: %w", err)
	}
	if prefix == "" {
		prefix = "cdc"
	}
	return &RedisSink{client: client, prefix: prefix}, nil
}

func (s *RedisSink) Close() {
	s.client.Close()
}

func (s *RedisSink) EnsureTable(_ context.Context, _ *TableConfig, _ SinkMode) error {
	return nil
}

func (s *RedisSink) WriteBatch(ctx context.Context, rows []CDCRow, mode SinkMode) error {
	if len(rows) == 0 {
		return nil
	}
	if mode == SinkModeLog {
		return s.writeLog(ctx, rows)
	}
	return s.writeUpsert(ctx, rows)
}

func (s *RedisSink) writeUpsert(ctx context.Context, rows []CDCRow) error {
	pipe := s.client.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, r := range rows {
		switch r.Kind {
		case RowKindInsert, RowKindUpdateAfter:
			key := s.buildKeyFromRow(r.Table, r.Values)
			fields := make(map[string]interface{}, len(r.Table.Columns)+2)
			for i, cm := range r.Table.Columns {
				v := sanitizeValue(r.Values[i])
				if v == nil {
					fields[cm.Target] = ""
				} else {
					fields[cm.Target] = v
				}
			}
			fields["_cdc_operation"] = string(r.Kind)
			fields["_cdc_timestamp"] = now
			pipe.HSet(ctx, key, fields)
		case RowKindDelete:
			key := s.buildKeyFromPK(r.Table, r.PKVals)
			pipe.Del(ctx, key)
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisSink) writeLog(ctx context.Context, rows []CDCRow) error {
	pipe := s.client.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, r := range rows {
		listKey := s.buildKeyFromPK(r.Table, r.PKVals) + ":log"
		event := map[string]interface{}{
			"_cdc_kind":      string(r.Kind),
			"_cdc_timestamp": now,
		}
		if r.Values != nil {
			for i, cm := range r.Table.Columns {
				event[cm.Target] = sanitizeValue(r.Values[i])
			}
		} else {
			pkTargets := pkTargetNames(r.Table)
			for i, name := range pkTargets {
				if i < len(r.PKVals) {
					event[name] = r.PKVals[i]
				}
			}
		}
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshal: %w", err)
		}
		pipe.RPush(ctx, listKey, data)
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisSink) buildKeyFromRow(t *TableConfig, row []interface{}) string {
	pkParts := make([]string, len(t.PrimaryKey))
	for i, pk := range t.PrimaryKey {
		for j, cm := range t.Columns {
			if cm.Source == pk {
				v := row[j]
				if v == nil {
					pkParts[i] = ""
				} else {
					pkParts[i] = fmt.Sprintf("%v", v)
				}
				break
			}
		}
	}
	return s.prefix + ":" + t.Target + ":" + strings.Join(pkParts, ":")
}

func (s *RedisSink) buildKeyFromPK(t *TableConfig, pkVals []interface{}) string {
	parts := make([]string, len(pkVals))
	for i, v := range pkVals {
		if v == nil {
			parts[i] = ""
		} else {
			parts[i] = fmt.Sprintf("%v", v)
		}
	}
	return s.prefix + ":" + t.Target + ":" + strings.Join(parts, ":")
}

// RedisJSONSink writes CDC rows as JSON strings.
// In upsert mode: SET with JSON value.
// In log mode: same as RedisSink (RPUSH JSON to list).
type RedisJSONSink struct {
	RedisSink
}

func NewRedisJSONSink(ctx context.Context, addr, password string, db int, prefix string) (*RedisJSONSink, error) {
	base, err := NewRedisSink(ctx, addr, password, db, prefix)
	if err != nil {
		return nil, err
	}
	return &RedisJSONSink{RedisSink: *base}, nil
}

func (s *RedisJSONSink) WriteBatch(ctx context.Context, rows []CDCRow, mode SinkMode) error {
	if len(rows) == 0 {
		return nil
	}
	if mode == SinkModeLog {
		return s.writeLog(ctx, rows)
	}
	return s.writeUpsertJSON(ctx, rows)
}

func (s *RedisJSONSink) writeUpsertJSON(ctx context.Context, rows []CDCRow) error {
	pipe := s.client.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, r := range rows {
		switch r.Kind {
		case RowKindInsert, RowKindUpdateAfter:
			key := s.buildKeyFromRow(r.Table, r.Values)
			fields := make(map[string]interface{}, len(r.Table.Columns)+2)
			for i, cm := range r.Table.Columns {
				fields[cm.Target] = sanitizeValue(r.Values[i])
			}
			fields["_cdc_operation"] = string(r.Kind)
			fields["_cdc_timestamp"] = now
			data, err := json.Marshal(fields)
			if err != nil {
				return fmt.Errorf("marshal: %w", err)
			}
			pipe.Set(ctx, key, data, 0)
		case RowKindDelete:
			key := s.buildKeyFromPK(r.Table, r.PKVals)
			pipe.Del(ctx, key)
		}
	}

	_, err := pipe.Exec(ctx)
	return err
}
