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

	// GRPCBatchSize: so dong toi da trong mot message cua cac RPC "...Batch"
	// (moi bao cao dung chung - xem internal/grpcbatch). Bien moi truong
	// GRPC_BATCH_SIZE, mac dinh 600, khong set hoac sai thi dung mac dinh.
	GRPCBatchSize int

	// GRPCCompression: false = khong nen zstd cho cac RPC "...Batch" du client
	// co khai bao ho tro. Dung de tat nhanh khi nghi ngo nen gay loi hoac ton CPU,
	// khong can deploy lai code. Bien moi truong GRPC_COMPRESSION=off de tat,
	// mac dinh (hoac bat ky gia tri nao khac "off") la bat.
	GRPCCompression bool

	// SoChiTietTaiKhoanStagingCompanyIDs: danh sach CompanyID (UUID, ngan cach
	// boi dau phay) chay duong staging cho rieng cac cong ty nay, con lai van
	// la fact. Dung de cho vai cong ty lam end tester truoc khi bat dai tra.
	// Bien moi truong SO_CHI_TIET_TAI_KHOAN_STAGING_COMPANY_IDS. Bi bo qua neu
	// SoChiTietTaiKhoanUseStaging da bat (toan bo da la staging).
	SoChiTietTaiKhoanStagingCompanyIDs []string

	// SoKeToanChiTietQuyTienMatUseStaging: cung co che nhu tren, ap dung cho
	// so ke toan chi tiet quy tien mat. Bat bang bien moi truong
	// SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE=staging.
	SoKeToanChiTietQuyTienMatUseStaging bool

	// SoNhatKyChungUseStaging / SoNhatKyChungStagingCompanyIDs: cung co che nhu
	// so chi tiet tai khoan, ap dung cho so nhat ky chung. Bat bang bien moi
	// truong SO_NHAT_KY_CHUNG_DATA_SOURCE=staging, hoac gioi han cho vai cong ty
	// bang SO_NHAT_KY_CHUNG_STAGING_COMPANY_IDS (UUID, ngan cach boi dau phay).
	SoNhatKyChungUseStaging        bool
	SoNhatKyChungStagingCompanyIDs []string

	// SoChiTietCongNoPhaiThuUseStaging / SoChiTietCongNoPhaiThuStagingCompanyIDs: cung
	// co che nhu so chi tiet tai khoan, ap dung cho so chi tiet cong no phai thu. Bat
	// bang bien moi truong SO_CHI_TIET_CONG_NO_PHAI_THU_DATA_SOURCE=staging, hoac gioi
	// han cho vai cong ty bang SO_CHI_TIET_CONG_NO_PHAI_THU_STAGING_COMPANY_IDS (UUID,
	// ngan cach boi dau phay).
	SoChiTietCongNoPhaiThuUseStaging        bool
	SoChiTietCongNoPhaiThuStagingCompanyIDs []string

	// SoChiTietVatLieuOpeningBalanceUseCheckpoint: true = tinh so du dau ky
	// (opening_balance) cua so chi tiet vat lieu hang hoa qua bang
	// eb.repository_ledger_checkpoint + phan le raw, thay vi cong don toan bo
	// lich su PostedDate < FROM_DATE tu eb.repository_ledger (mac dinh). Bat
	// bang bien moi truong SO_CHI_TIET_VAT_LIEU_OPENING_BALANCE_SOURCE=checkpoint,
	// khong can deploy lai code. Gia tri mac dinh (khong set, hoac bat ky gia
	// tri nao khac "checkpoint") van la raw - an toan, khong doi hanh vi hien tai.
	SoChiTietVatLieuOpeningBalanceUseCheckpoint bool

	// TinhGiaXuatKhoOpeningStockUseCheckpoint: true = lay ton dau ky cua dataset tinh gia
	// xuat kho qua bang eb.repository_ledger_checkpoint + phan le raw, thay vi cong don
	// toan bo lich su PostedDate < FROM_DATE tu eb.repository_ledger (mac dinh).
	// Hai bang nay duoc ghi bang hai thao tac tach roi, khong cung dung cung sai, nen khi
	// checkpoint lech thi xoa bien moi truong roi khoi dong lai de quay ve tinh toan bo.
	// Bat bang TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE=checkpoint, khong can deploy lai code.
	TinhGiaXuatKhoOpeningStockUseCheckpoint bool
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

		GRPCBatchSize:   getEnvInt("GRPC_BATCH_SIZE", 0),
		GRPCCompression: !strings.EqualFold(getEnv("GRPC_COMPRESSION", "zstd"), "off"),

		SoChiTietTaiKhoanStagingCompanyIDs: splitCSV(
			os.Getenv("SO_CHI_TIET_TAI_KHOAN_STAGING_COMPANY_IDS")),

		SoKeToanChiTietQuyTienMatUseStaging: strings.EqualFold(
			getEnv("SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE", "fact"), "staging"),

		SoNhatKyChungUseStaging: strings.EqualFold(
			getEnv("SO_NHAT_KY_CHUNG_DATA_SOURCE", "fact"), "staging"),

		SoNhatKyChungStagingCompanyIDs: splitCSV(
			os.Getenv("SO_NHAT_KY_CHUNG_STAGING_COMPANY_IDS")),

		SoChiTietCongNoPhaiThuUseStaging: strings.EqualFold(
			getEnv("SO_CHI_TIET_CONG_NO_PHAI_THU_DATA_SOURCE", "fact"), "staging"),

		SoChiTietCongNoPhaiThuStagingCompanyIDs: splitCSV(
			os.Getenv("SO_CHI_TIET_CONG_NO_PHAI_THU_STAGING_COMPANY_IDS")),

		SoChiTietVatLieuOpeningBalanceUseCheckpoint: strings.EqualFold(
			getEnv("SO_CHI_TIET_VAT_LIEU_OPENING_BALANCE_SOURCE", "raw"), "checkpoint"),

		TinhGiaXuatKhoOpeningStockUseCheckpoint: strings.EqualFold(
			getEnv("TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE", "raw"), "checkpoint"),
	}

	//
	if cfg.CHPassword == "" {
		return cfg, fmt.Errorf("bien moi truong CH_PASSWORD chua duoc set")
	}

	return cfg, nil
}

func splitCSV(v string) []string {
	var out []string
	for _, part := range strings.Split(v, ",") {
		if part = strings.ToLower(strings.TrimSpace(part)); part != "" {
			out = append(out, part)
		}
	}
	return out
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
