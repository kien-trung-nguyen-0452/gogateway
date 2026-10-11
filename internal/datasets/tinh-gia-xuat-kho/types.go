package tinh_gia_xuat_kho

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"
)

// Handler nhận request HTTP, chuyển sang QueryParams rồi gọi Service.
type Handler struct {
	service *Service
}

// Service chứa toàn bộ logic đọc dữ liệu, không phụ thuộc HTTP.
type Service struct {
	conn clickhouse.Conn

	// useCheckpoint: true thì tồn đầu kỳ lấy từ eb.repository_ledger_checkpoint cộng phần lẻ raw,
	// thay vì cộng dồn toàn bộ lịch sử. Xem opening_stock_checkpoint.sql và
	// config.TinhGiaXuatKhoOpeningStockUseCheckpoint, bật bằng biến môi trường
	// TINH_GIA_XUAT_KHO_OPENING_STOCK_SOURCE=checkpoint, không cần deploy lại code.
	useCheckpoint bool
}

// driverRows là interface tối thiểu để scanRow dùng được, không phụ thuộc trực tiếp vào kiểu cụ thể của clickhouse-go.
type driverRows interface {
	Scan(dest ...any) error
}

/*
RequestBody là cấu trúc JSON nhận từ client.
- CompanyID: 		ID công ty, lọc theo CompanyID.
- TypeLedger: 		Sổ làm việc, lọc theo TypeLedger, lấy kèm TypeLedger = 2.
- FromDate: 		Ngày bắt đầu tính giá, lọc theo PostedDate, định dạng "2026-08-01 00:00:00".
- ToDate: 			Ngày kết thúc tính giá, lọc theo PostedDate, định dạng "2026-08-31 23:59:59".
- RepositoryIDs: 	Danh sách Kho, lọc theo RepositoryID, rỗng là tất cả Kho.
- MaterialGoodsIDs: Danh sách VTHH, lọc theo MaterialGoodsID, rỗng là tất cả VTHH.
*/
type RequestBody struct {
	CompanyID        string   `json:"companyID"`
	TypeLedger       int      `json:"typeLedger"`
	FromDate         string   `json:"fromDate"`
	ToDate           string   `json:"toDate"`
	RepositoryIDs    []string `json:"repositoryIDs"`
	MaterialGoodsIDs []string `json:"materialGoodsIDs"`
}

/*
Row là một dòng dữ liệu trả về cho Client.
- IsOpeningStock: 	true là dòng tồn đầu kỳ, false là dòng phát sinh trong kỳ.
- MaterialGoodsID: 	ID VTHH.
- RepositoryID: 	ID Kho.
- DetailID: 		ID chứng từ, dùng để ghi giá vốn ngược lại.
- ReferenceID: 		ID chứng từ gốc.
- TypeID: 			Loại chứng từ.
- PostedDate: 		Ngày hạch toán, Định dạng 1970-01-01.
- MainIWQuantity: 	Số lượng nhập, đã quy về đơn vị chính.
- MainOWQuantity: 	Số lượng xuất, đã quy về đơn vị chính.
- IWAmount: 		Giá trị nhập, đã quy về đơn vị chính.
- OWAmount: 		Giá trị xuất, đã quy về đơn vị chính.
*/
type Row struct {
	IsOpeningStock  bool            `json:"isOpeningStock"`
	MaterialGoodsID string          `json:"materialGoodsID"`
	RepositoryID    string          `json:"repositoryID"`
	DetailID        string          `json:"detailID"`
	ReferenceID     string          `json:"referenceID"`
	TypeID          int32           `json:"typeID"`
	PostedDate      time.Time       `json:"postedDate"`
	MainIWQuantity  decimal.Decimal `json:"mainIwQuantity"`
	MainOWQuantity  decimal.Decimal `json:"mainOwQuantity"`
	IWAmount        decimal.Decimal `json:"iwAmount"`
	OWAmount        decimal.Decimal `json:"owAmount"`
}
