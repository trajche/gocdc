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
// Key format: {prefix}:{table}:{pk1}:{pk2}:...
// Each hash contains the mapped column values plus _cdc_operation and _cdc_timestamp.
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

func (s *RedisSink) EnsureTable(_ context.Context, _ *TableConfig) error {
	// Redis is schema-less — nothing to prepare.
	return nil
}

func (s *RedisSink) Upsert(ctx context.Context, t *TableConfig, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, row := range rows {
		key := s.buildKey(t, row)
		fields := make(map[string]interface{}, len(t.Columns)+2)
		for i, cm := range t.Columns {
			v := sanitizeValue(row[i])
			if v == nil {
				fields[cm.Target] = ""
			} else {
				fields[cm.Target] = v
			}
		}
		fields["_cdc_operation"] = "upsert"
		fields["_cdc_timestamp"] = now
		pipe.HSet(ctx, key, fields)
	}

	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisSink) Delete(ctx context.Context, t *TableConfig, keys [][]interface{}) error {
	if len(keys) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()
	for _, pkVals := range keys {
		key := s.buildKeyFromPK(t, pkVals)
		pipe.Del(ctx, key)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// buildKey constructs the Redis key from a full row using PK column positions.
func (s *RedisSink) buildKey(t *TableConfig, row []interface{}) string {
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

// buildKeyFromPK constructs the Redis key from PK values (used for deletes).
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

// RedisJSONSink writes CDC rows to Redis as JSON strings (for use with RedisJSON or
// applications that prefer GET over HGETALL).
// Key format: {prefix}:{table}:{pk1}:{pk2}:...
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

func (s *RedisJSONSink) Upsert(ctx context.Context, t *TableConfig, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}

	pipe := s.client.Pipeline()
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, row := range rows {
		key := s.buildKey(t, row)
		fields := make(map[string]interface{}, len(t.Columns)+2)
		for i, cm := range t.Columns {
			v := sanitizeValue(row[i])
			fields[cm.Target] = v
		}
		fields["_cdc_operation"] = "upsert"
		fields["_cdc_timestamp"] = now

		data, err := json.Marshal(fields)
		if err != nil {
			return fmt.Errorf("marshal row: %w", err)
		}
		pipe.Set(ctx, key, data, 0)
	}

	_, err := pipe.Exec(ctx)
	return err
}
