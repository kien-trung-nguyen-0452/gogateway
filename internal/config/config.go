package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config chua toan bo cau hinh service, doc tu bien moi truong.
// Khong hardcode password/host de tien deploy nhieu moi truong khac nhau.
type Config struct {
	CHHost     string
	CHPort     int
	CHUser     string
	CHPassword string
	CHDatabase string
	HTTPPort   string
}

func Load() (Config, error) {
	cfg := Config{
		CHHost:     getEnv("CH_HOST", "10.100.112.47"),
		CHPort:     getEnvInt("CH_PORT", 9000), // 9000 = native protocol - day chinh la ly do co satellite nay
		CHUser:     getEnv("CH_USER", "eb_admin"),
		CHPassword: os.Getenv("CH_PASSWORD"),
		CHDatabase: getEnv("CH_DATABASE", "eb"),
		HTTPPort:   getEnv("HTTP_PORT", "8090"),
	}

	if cfg.CHPassword == "" {
		return cfg, fmt.Errorf("bien moi truong CH_PASSWORD chua duoc set")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}
