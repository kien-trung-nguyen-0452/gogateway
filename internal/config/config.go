package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
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
	GRPCPort   string

	// SoChiTietTaiKhoanUseStaging: true = doc tu bang eb_staging thay vi
	// eb_dwh.fact_gl_entry_line. Bat tam thoi bang bien moi truong
	// SO_CHI_TIET_TAI_KHOAN_DATA_SOURCE=staging khi pipeline fact bi loi,
	// khong can deploy lai code. Gia tri mac dinh (khong set, hoac bat ky
	// gia tri nao khac "staging") van la fact - an toan, khong doi hanh vi
	// hien tai.
	SoChiTietTaiKhoanUseStaging bool

	// SoKeToanChiTietQuyTienMatUseStaging: cung co che nhu tren, ap dung cho
	// so ke toan chi tiet quy tien mat. Bat bang bien moi truong
	// SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE=staging.
	SoKeToanChiTietQuyTienMatUseStaging bool
}

func Load() (Config, error) {
	cfg := Config{
		CHHost:     getEnv("CH_HOST", "10.100.112.47"),
		CHPort:     getEnvInt("CH_PORT", 9000), // 9000 = native protocol - day chinh la ly do co satellite nay
		CHUser:     getEnv("CH_USER", "eb_admin"),
		CHPassword: os.Getenv("CH_PASSWORD"),
		CHDatabase: getEnv("CH_DATABASE", "eb"),
		HTTPPort:   getEnv("HTTP_PORT", "8089"),
		GRPCPort:   getEnv("GRPC_PORT", "9090"),

		SoChiTietTaiKhoanUseStaging: strings.EqualFold(
			getEnv("SO_CHI_TIET_TAI_KHOAN_DATA_SOURCE", "fact"), "staging"),

		SoKeToanChiTietQuyTienMatUseStaging: strings.EqualFold(
			getEnv("SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE", "fact"), "staging"),
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
