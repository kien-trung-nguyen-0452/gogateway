package chclient

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	"softdream.vn/go-gateway/internal/config"
)

func NewConn(cfg config.Config) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.CHHost, cfg.CHPort)},
		Auth: clickhouse.Auth{
			Database: cfg.CHDatabase,
			Username: cfg.CHUser,
			Password: cfg.CHPassword,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionZSTD, // doi lz4 neu muon it CPU hon, ton bang thong hon
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    20,
		MaxIdleConns:    10,
		ConnMaxLifetime: 30 * time.Minute,
		Settings: clickhouse.Settings{
			"max_block_size": 100000,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("khong tao duoc ket noi ClickHouse: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping ClickHouse that bai: %w", err)
	}

	return conn, nil
}
