package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	_ "github.com/go-sql-driver/mysql"
)

var version = "dev"

// Checkpoint stores the binlog position for resumption.
type Checkpoint struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
}

func loadCheckpoint(path string) (*Checkpoint, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var cp Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func saveCheckpoint(path string, cp Checkpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// configuredSink pairs a Sink with its mode from config.
type configuredSink struct {
	sink Sink
	mode SinkMode
	cfg  SinkConfig
}

// CDCHandler implements canal.EventHandler.
type CDCHandler struct {
	mu       sync.Mutex
	cfg      *Config
	sinks    []configuredSink
	tableMap map[string]*TableConfig // source table name → config
	batch    []CDCRow
	pos      mysql.Position
	ctx      context.Context
}

func (h *CDCHandler) OnRow(e *canal.RowsEvent) error {
	tableName := e.Table.Name
	tc, ok := h.tableMap[tableName]
	if !ok {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	switch e.Action {
	case canal.InsertAction:
		for _, row := range e.Rows {
			if mapped, pkVals, ok := h.mapRow(tc, row); ok {
				h.batch = append(h.batch, CDCRow{
					Table: tc, Kind: RowKindInsert, Values: mapped, PKVals: pkVals,
				})
			}
		}
	case canal.UpdateAction:
		// e.Rows alternates [before, after, before, after, ...]
		for i := 0; i+1 < len(e.Rows); i += 2 {
			before := e.Rows[i]
			after := e.Rows[i+1]

			mappedBefore, pkValsBefore, okBefore := h.mapRow(tc, before)
			mappedAfter, pkValsAfter, okAfter := h.mapRow(tc, after)

			// For log mode we emit both; for upsert mode the sink ignores -U
			if okBefore {
				h.batch = append(h.batch, CDCRow{
					Table: tc, Kind: RowKindUpdateBefore, Values: mappedBefore, PKVals: pkValsBefore,
				})
			}
			if okAfter {
				h.batch = append(h.batch, CDCRow{
					Table: tc, Kind: RowKindUpdateAfter, Values: mappedAfter, PKVals: pkValsAfter,
				})
			}
		}
	case canal.DeleteAction:
		for _, row := range e.Rows {
			mapped, pkVals, ok := h.mapRow(tc, row)
			if !ok {
				// Still extract PKs for delete even if filter doesn't match values
				pkVals = extractPKVals(tc, row)
				if pkVals == nil {
					continue
				}
			}
			h.batch = append(h.batch, CDCRow{
				Table: tc, Kind: RowKindDelete, Values: mapped, PKVals: pkVals,
			})
		}
	}

	if len(h.batch) >= h.cfg.BatchSize {
		return h.flushLocked()
	}
	return nil
}

func extractPKVals(tc *TableConfig, row []interface{}) []interface{} {
	pkVals := make([]interface{}, len(tc.PKOrdinals))
	for i, idx := range tc.PKOrdinals {
		if idx < len(row) {
			pkVals[i] = toString(row[idx])
		}
	}
	return pkVals
}

// mapRow extracts configured columns from a binlog row and applies stream filters.
// Returns (values, pkVals, matched).
func (h *CDCHandler) mapRow(tc *TableConfig, row []interface{}) ([]interface{}, []interface{}, bool) {
	// Apply stream filter
	if tc.StreamFilter.Column != "" {
		idx, ok := tc.ColIndex[tc.StreamFilter.Column]
		if !ok {
			return nil, nil, false
		}
		if idx >= len(row) {
			return nil, nil, false
		}
		val := toString(row[idx])

		if len(tc.StreamFilter.Values) > 0 {
			matched := false
			for _, v := range tc.StreamFilter.Values {
				if val == v {
					matched = true
					break
				}
			}
			if !matched {
				return nil, nil, false
			}
		}

		if tc.StreamFilter.Pattern != "" {
			if !matchLike(val, tc.StreamFilter.Pattern) {
				return nil, nil, false
			}
		}
	}

	mapped := make([]interface{}, len(tc.SourceOrdinals))
	for i, idx := range tc.SourceOrdinals {
		if idx < len(row) {
			mapped[i] = toString(row[idx])
		} else {
			mapped[i] = nil
		}
	}

	pkVals := make([]interface{}, len(tc.PKOrdinals))
	for i, idx := range tc.PKOrdinals {
		if idx < len(row) {
			pkVals[i] = toString(row[idx])
		}
	}

	return mapped, pkVals, true
}

// toString converts a binlog row value to a string, handling []byte properly.
func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return fmt.Sprintf("%v", v)
}

// matchLike does a simple SQL LIKE match (only supports % and _ wildcards).
func matchLike(val, pattern string) bool {
	if strings.HasSuffix(pattern, "%") && !strings.Contains(pattern[:len(pattern)-1], "%") {
		prefix := strings.ReplaceAll(pattern[:len(pattern)-1], "_", "?")
		prefix = strings.ReplaceAll(prefix, "?", "_")
		return strings.HasPrefix(val, prefix)
	}
	return val == pattern
}

func (h *CDCHandler) flushLocked() error {
	if len(h.batch) == 0 {
		return nil
	}

	for _, cs := range h.sinks {
		if err := cs.sink.WriteBatch(h.ctx, h.batch, cs.mode); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
	}

	log.Printf("flushed %d events (pos: %s:%d)", len(h.batch), h.pos.Name, h.pos.Pos)
	h.batch = h.batch[:0]
	return nil
}

func (h *CDCHandler) Flush() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.flushLocked()
}

func (h *CDCHandler) OnRotate(e *replication.EventHeader, r *replication.RotateEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pos = mysql.Position{Name: string(r.NextLogName), Pos: uint32(r.Position)}
	return nil
}

func (h *CDCHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.pos = pos
	return nil
}

func (h *CDCHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	return nil
}
func (h *CDCHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}
func (h *CDCHandler) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	return nil
}
func (h *CDCHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	return nil
}
func (h *CDCHandler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	return nil
}
func (h *CDCHandler) String() string { return "CDCHandler" }

func main() {
	configPath := flag.String("config", "config.yaml", "path to config.yaml")
	snapshotOnly := flag.Bool("snapshot-only", false, "run snapshot then exit")
	noSnapshot := flag.Bool("no-snapshot", false, "skip snapshot, start streaming from checkpoint")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("gocdc", version)
		return
	}

	cfg, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	log.Printf("resolving column ordinals for %d tables...", len(cfg.Tables))
	if err := ResolveOrdinals(cfg); err != nil {
		log.Fatalf("resolve ordinals: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sinkConfigs := cfg.EffectiveSinks()
	if len(sinkConfigs) == 0 {
		log.Fatal("no sinks configured")
	}

	var sinks []configuredSink
	for _, sc := range sinkConfigs {
		s, err := createSink(ctx, sc)
		if err != nil {
			log.Fatalf("create %s sink: %v", sc.Type, err)
		}
		mode := sc.EffectiveMode()
		sinks = append(sinks, configuredSink{sink: s, mode: mode, cfg: sc})
		log.Printf("connected to %s sink (mode: %s)", sc.Type, mode)
	}
	defer func() {
		for _, cs := range sinks {
			cs.sink.Close()
		}
	}()

	// Ensure target tables exist in all sinks
	for _, cs := range sinks {
		for i := range cfg.Tables {
			if err := cs.sink.EnsureTable(ctx, &cfg.Tables[i], cs.mode); err != nil {
				log.Fatalf("ensure table %s: %v", cfg.Tables[i].Target, err)
			}
		}
	}

	cp, err := loadCheckpoint(cfg.CheckpointFile)
	if err != nil {
		log.Fatalf("load checkpoint: %v", err)
	}

	needSnapshot := cp == nil && !*noSnapshot
	if needSnapshot || *snapshotOnly {
		log.Println("starting snapshot...")
		if err := runSnapshot(ctx, cfg, sinks); err != nil {
			log.Fatalf("snapshot: %v", err)
		}
		log.Println("snapshot complete")
		if *snapshotOnly {
			return
		}
	}

	// Start streaming
	log.Println("starting binlog stream...")

	flushInterval, err := time.ParseDuration(cfg.FlushInterval)
	if err != nil {
		log.Fatalf("parse flush_interval: %v", err)
	}

	canalCfg := canal.NewDefaultConfig()
	canalCfg.Addr = cfg.Source.Addr
	canalCfg.User = cfg.Source.User
	canalCfg.Password = cfg.Source.Password
	canalCfg.Flavor = cfg.Source.Flavor
	canalCfg.ServerID = cfg.Source.ServerID
	canalCfg.Dump.ExecutionPath = "" // disable mysqldump

	// Build table regex for canal
	var tablePatterns []string
	for _, t := range cfg.Tables {
		tablePatterns = append(tablePatterns, fmt.Sprintf("%s\\.%s", cfg.Source.Database, t.Source))
	}
	canalCfg.IncludeTableRegex = tablePatterns

	c, err := canal.NewCanal(canalCfg)
	if err != nil {
		log.Fatalf("create canal: %v", err)
	}

	tableMap := make(map[string]*TableConfig)
	for i := range cfg.Tables {
		tableMap[cfg.Tables[i].Source] = &cfg.Tables[i]
	}

	handler := &CDCHandler{
		cfg:      cfg,
		sinks:    sinks,
		tableMap: tableMap,
		ctx:      ctx,
	}
	c.SetEventHandler(handler)

	// Periodic flush timer
	ticker := time.NewTicker(flushInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				if err := handler.Flush(); err != nil {
					log.Printf("flush error: %v", err)
				}
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()

	// Determine start position
	var startPos mysql.Position
	if cp != nil {
		startPos = mysql.Position{Name: cp.File, Pos: cp.Pos}
		log.Printf("resuming from checkpoint %s:%d", cp.File, cp.Pos)
	} else {
		pos, err := getMasterPos(cfg)
		if err != nil {
			log.Fatalf("get master pos: %v", err)
		}
		startPos = pos
		log.Printf("starting from current position %s:%d", pos.Name, pos.Pos)
	}

	// Signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		log.Println("shutting down...")
		cancel()
		c.Close()
	}()

	// Periodic checkpoint save
	go func() {
		cpTicker := time.NewTicker(5 * time.Second)
		defer cpTicker.Stop()
		for {
			select {
			case <-cpTicker.C:
				handler.mu.Lock()
				pos := handler.pos
				handler.mu.Unlock()
				if pos.Name != "" {
					if err := saveCheckpoint(cfg.CheckpointFile, Checkpoint{File: pos.Name, Pos: pos.Pos}); err != nil {
						log.Printf("save checkpoint: %v", err)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	if err := c.RunFrom(startPos); err != nil {
		if ctx.Err() == nil {
			log.Fatalf("canal: %v", err)
		}
	}

	// Final flush and checkpoint
	if err := handler.Flush(); err != nil {
		log.Printf("final flush: %v", err)
	}
	handler.mu.Lock()
	pos := handler.pos
	handler.mu.Unlock()
	if pos.Name != "" {
		if err := saveCheckpoint(cfg.CheckpointFile, Checkpoint{File: pos.Name, Pos: pos.Pos}); err != nil {
			log.Printf("save final checkpoint: %v", err)
		}
	}
	log.Println("shutdown complete")
}

func createSink(ctx context.Context, sc SinkConfig) (Sink, error) {
	switch sc.Type {
	case "postgres", "":
		return NewPostgresSink(ctx, sc.DSN, sc.Schema)
	case "redis":
		return NewRedisSink(ctx, sc.Addr, sc.Password, sc.DB, sc.Prefix)
	case "redis-json":
		return NewRedisJSONSink(ctx, sc.Addr, sc.Password, sc.DB, sc.Prefix)
	case "stdout":
		return NewStdoutSink(), nil
	default:
		return nil, fmt.Errorf("unknown sink type: %q", sc.Type)
	}
}

// getMasterPos queries SHOW MASTER STATUS directly, working around a go-mysql
// bug where it sends SHOW BINARY LOG STATUS to MariaDB (which doesn't support it).
func getMasterPos(cfg *Config) (mysql.Position, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", cfg.Source.User, cfg.Source.Password, cfg.Source.Addr, cfg.Source.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return mysql.Position{}, err
	}
	defer db.Close()

	rows, err := db.Query("SHOW MASTER STATUS")
	if err != nil {
		return mysql.Position{}, fmt.Errorf("SHOW MASTER STATUS: %w", err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	if !rows.Next() {
		return mysql.Position{}, fmt.Errorf("SHOW MASTER STATUS returned no rows")
	}

	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(sql.NullString)
	}
	if err := rows.Scan(vals...); err != nil {
		return mysql.Position{}, fmt.Errorf("SHOW MASTER STATUS scan: %w", err)
	}

	file := vals[0].(*sql.NullString).String
	var pos uint32
	fmt.Sscanf(vals[1].(*sql.NullString).String, "%d", &pos)
	return mysql.Position{Name: file, Pos: pos}, nil
}

func runSnapshot(ctx context.Context, cfg *Config, sinks []configuredSink) error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s", cfg.Source.User, cfg.Source.Password, cfg.Source.Addr, cfg.Source.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connect source: %w", err)
	}
	defer db.Close()

	for i := range cfg.Tables {
		t := &cfg.Tables[i]
		if err := snapshotTable(ctx, db, cfg, t, sinks); err != nil {
			return fmt.Errorf("snapshot %s: %w", t.Source, err)
		}
	}
	return nil
}

func snapshotTable(ctx context.Context, db *sql.DB, cfg *Config, t *TableConfig, sinks []configuredSink) error {
	srcCols := make([]string, len(t.Columns))
	for i, cm := range t.Columns {
		srcCols[i] = cm.Source
	}

	pkCol := t.PrimaryKey[0]
	var lastPK interface{} = 0

	totalRows := 0
	for {
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ?",
			strings.Join(srcCols, ", "), t.Source, pkCol)
		if t.SnapshotFilter != "" {
			query += " AND (" + t.SnapshotFilter + ")"
		}
		query += fmt.Sprintf(" ORDER BY %s LIMIT %d", pkCol, cfg.BatchSize)

		rows, err := db.QueryContext(ctx, query, lastPK)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}

		var batch []CDCRow
		for rows.Next() {
			vals := make([]interface{}, len(srcCols))
			ptrs := make([]interface{}, len(srcCols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				rows.Close()
				return fmt.Errorf("scan: %w", err)
			}

			strVals := make([]interface{}, len(vals))
			for i, v := range vals {
				if v == nil {
					strVals[i] = nil
				} else if b, ok := v.([]byte); ok {
					strVals[i] = string(b)
				} else {
					strVals[i] = fmt.Sprintf("%v", v)
				}
			}

			// Extract PK values
			pkTargets := pkTargetNames(t)
			pkVals := make([]interface{}, len(pkTargets))
			for i, pk := range t.PrimaryKey {
				for j, col := range srcCols {
					if col == pk {
						pkVals[i] = strVals[j]
						break
					}
				}
			}

			batch = append(batch, CDCRow{
				Table: t, Kind: RowKindInsert, Values: strVals, PKVals: pkVals,
			})

			for j, col := range srcCols {
				if col == pkCol {
					lastPK = vals[j]
					break
				}
			}
		}
		rows.Close()

		if len(batch) == 0 {
			break
		}

		for _, cs := range sinks {
			if err := cs.sink.WriteBatch(ctx, batch, cs.mode); err != nil {
				return fmt.Errorf("write batch: %w", err)
			}
		}

		totalRows += len(batch)
		log.Printf("  %s: %d rows snapshotted", t.Source, totalRows)

		if len(batch) < cfg.BatchSize {
			break
		}
	}

	log.Printf("  %s: snapshot complete (%d rows)", t.Source, totalRows)
	return nil
}
