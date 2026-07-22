package buffer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type BufferedItem struct {
	Timestamp time.Time       `json:"timestamp"`
	DataType  string          `json:"data_type"`
	Payload   json.RawMessage `json:"payload"`
}

type Buffer interface {
	AddData(dataType string, payload interface{}) error
	Fetch() ([]BufferedItem, error)
	StartCleanup(ctx context.Context, retention time.Duration)
	Close() error
}

type SQLiteBuffer struct {
	db *sql.DB
}

func NewSQLiteBuffer(dbPath string) (*SQLiteBuffer, error) {
	if err := os.MkdirAll(filepath.Dir(dbPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory for database: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS buffer (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp DATETIME NOT NULL,
			data_type TEXT NOT NULL,
			payload JSON NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_buffer_timestamp ON buffer(timestamp);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &SQLiteBuffer{db: db}, nil
}

func (s *SQLiteBuffer) AddData(dataType string, payload interface{}) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	res, err := s.db.Exec(
		"INSERT INTO buffer (timestamp, data_type, payload) VALUES (?, ?, ?)",
		time.Now().UTC(),
		dataType,
		string(payloadBytes),
	)
	if err != nil {
		return fmt.Errorf("failed to insert data: %w", err)
	}

	if id, err := res.LastInsertId(); err == nil {
		slog.Debug("Buffered telemetry data successfully", "row_id", id, "timestamp", time.Now().UTC())
	}

	return nil
}

func (s *SQLiteBuffer) Fetch() ([]BufferedItem, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	rows, err := tx.Query("SELECT id, timestamp, data_type, payload FROM buffer ORDER BY timestamp ASC")
	if err != nil {
		return nil, fmt.Errorf("failed to query buffer: %w", err)
	}

	var items []BufferedItem
	var ids []int64
	for rows.Next() {
		var id int64
		var item BufferedItem
		var payloadStr string
		if err := rows.Scan(&id, &item.Timestamp, &item.DataType, &payloadStr); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		item.Payload = json.RawMessage(payloadStr)
		items = append(items, item)
		ids = append(ids, id)
	}
	rows.Close()

	if len(ids) > 0 {
		maxId := ids[len(ids)-1]
		if _, err := tx.Exec("DELETE FROM buffer WHERE id <= ?", maxId); err != nil {
			return nil, fmt.Errorf("failed to delete fetched rows: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if items == nil {
		items = []BufferedItem{}
	}

	return items, nil
}

func (s *SQLiteBuffer) StartCleanup(ctx context.Context, retention time.Duration) {
	ticker := time.NewTicker(1 * time.Minute)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				cutoff := time.Now().UTC().Add(-retention)
				res, err := s.db.Exec("DELETE FROM buffer WHERE timestamp < ?", cutoff)
				if err != nil {
					slog.Error("Failed to clean up expired buffer data", "err", err)
					continue
				}
				if rows, err := res.RowsAffected(); err == nil && rows > 0 {
					slog.Info("Pruned expired rows from buffer", "count", rows)
				}
			}
		}
	}()
}

func (s *SQLiteBuffer) Close() error {
	return s.db.Close()
}
