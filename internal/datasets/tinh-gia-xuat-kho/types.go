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
	CompanyID        string   `json:"companyId"`
	TypeLedger       int      `json:"typeLedger"`
	FromDate         string   `json:"fromDate"`
	ToDate           string   `json:"toDate"`
	RepositoryIDs    []string `json:"repositoryIDs"`
	MaterialGoodsIDs []string `json:"materialGoodsIDs"`
}

/*
Response là cấu trúc JSON trả về cho client.
- RowCount: 	Tổng số dòng, tính cả dòng tồn đầu kỳ.
- ElapsedMs: 	Thời gian ClickHouse chạy query, tính bằng mili giây.
- Data: 		Dữ liệu trả về.
*/
type Response struct {
	RowCount  int   `json:"rowCount"`
	ElapsedMs int64 `json:"elapsedMs"`
	Data      []Row `json:"data"`
}

/*
Row là một dòng dữ liệu trả về cho Client.
- RowKind: 			Loại dòng, 0 là tồn đầu kỳ, 1 là phát sinh trong kỳ.
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
	RowKind         int8            `json:"rowKind"`
	MaterialGoodsID string          `json:"materialGoodsID"`
	RepositoryID    string          `json:"repositoryID"`
	DetailID        string          `json:"detailId"`
	ReferenceID     string          `json:"referenceId"`
	TypeID          int32           `json:"typeId"`
	PostedDate      time.Time       `json:"postedDate"`
	MainIWQuantity  decimal.Decimal `json:"mainIwQuantity"`
	MainOWQuantity  decimal.Decimal `json:"mainOwQuantity"`
	IWAmount        decimal.Decimal `json:"iwAmount"`
	OWAmount        decimal.Decimal `json:"owAmount"`
}
