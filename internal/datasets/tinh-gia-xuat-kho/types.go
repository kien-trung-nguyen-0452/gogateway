package tinh_gia_xuat_kho

import (
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/shopspring/decimal"
)

// RowKind phân biệt hai loại dòng trả về trong cùng một kết quả.
const (
	// RowKindOpening — tồn đầu kỳ, một dòng cho mỗi cặp (VTHH × Kho).
	// Luôn đứng trước các dòng chi tiết của cùng nhóm.
	RowKindOpening = 0

	// RowKindDetail — phát sinh trong kỳ, chi tiết từng dòng chứng từ.
	RowKindDetail = 1
)

// Handler nhận request HTTP, chuyển sang QueryParams rồi gọi Service.
type Handler struct {
	service *Service
}

// RequestBody là cấu trúc JSON nhận từ client.
//
// Mỗi trường tương ứng một điều kiện lọc trong query.sql.
type RequestBody struct {
	// CompanyID lọc cột CompanyID. Bắt buộc, đúng một công ty mỗi lần gọi.
	CompanyID string `json:"companyId"`

	// TypeLedger là sổ đang làm việc: 0 = sổ tài chính, 1 = sổ quản trị.
	// Lọc cột TypeLedger, luôn lấy kèm các dòng TypeLedger = 2 (ghi cả hai sổ).
	TypeLedger int `json:"typeLedger"`

	// FromDate là ngày bắt đầu kỳ tính giá, so với cột PostedDate.
	// Dòng có PostedDate nhỏ hơn giá trị này được gộp thành tồn đầu kỳ.
	// Dạng "2026-08-01" hoặc "2026-08-01 00:00:00".
	FromDate string `json:"fromDate"`

	// ToDate là ngày kết thúc kỳ, so với cột PostedDate.
	// Dạng "2026-08-31" hoặc "2026-08-31 23:59:59".
	ToDate string `json:"toDate"`

	// RepositoryIDs lọc cột RepositoryID. Để rỗng nghĩa là lấy tất cả kho.
	RepositoryIDs []string `json:"repositoryIds"`

	// MaterialGoodsIDs lọc cột MaterialGoodsID. Để rỗng nghĩa là lấy tất cả VTHH.
	MaterialGoodsIDs []string `json:"materialGoodsIds"`
}

// Response là cấu trúc JSON trả về cho client.
type Response struct {
	// RowCount là tổng số dòng, tính cả dòng tồn đầu kỳ.
	RowCount int `json:"rowCount"`

	// ElapsedMs là thời gian ClickHouse chạy query, tính bằng mili giây.
	ElapsedMs int64 `json:"elapsedMs"`

	// Data rỗng khi gọi kèm tham số ?metaOnly=true.
	Data []Row `json:"data"`
}

// Service chứa toàn bộ logic đọc dữ liệu, không phụ thuộc HTTP.
type Service struct {
	conn clickhouse.Conn
}

// QueryParams là tham số đã kiểm tra hợp lệ, dùng để dựng câu SQL.
// Ý nghĩa từng trường giống RequestBody.
type QueryParams struct {
	CompanyID        string
	TypeLedger       int
	FromDate         string
	ToDate           string
	RepositoryIDs    []string
	MaterialGoodsIDs []string
}

// driverRows là interface tối thiểu để scanRow dùng được, không phụ thuộc
// trực tiếp vào kiểu cụ thể của clickhouse-go.
type driverRows interface {
	Scan(dest ...any) error
}

// Row là một dòng dữ liệu thô trả về cho ebinventory.
//
// Chỉ giữ những cột cần cho công thức bình quân cuối kỳ. Không trả
// MainConvertRate và Formula vì việc quy đổi đơn giá sang đơn vị chứng từ
// được làm ở câu UPDATE bên SQL Server, đọc trực tiếp hai cột đó trên chính
// dòng cần cập nhật.
type Row struct {
	// RowKind cho biết đây là dòng tồn đầu kỳ hay dòng phát sinh.
	RowKind int8 `json:"rowKind"`

	// MaterialGoodsID và RepositoryID là khoá nghiệp vụ. Đơn giá bình quân
	// được tính riêng cho từng cặp.
	MaterialGoodsID string `json:"materialGoodsId"`
	RepositoryID    string `json:"repositoryId"`

	// DetailID là ID dòng chi tiết chứng từ, dùng để ghi giá vốn ngược lại.
	// Với dòng tồn đầu kỳ, giá trị là UUID toàn số 0.
	DetailID string `json:"detailId"`

	// ReferenceID là ID chứng từ gốc. Với dòng tồn đầu kỳ là UUID toàn số 0.
	ReferenceID string `json:"referenceId"`

	// TypeID là loại chứng từ, 403 là nhập kho từ bán hàng trả lại.
	// Với dòng tồn đầu kỳ, giá trị là 0.
	TypeID int32 `json:"typeId"`

	// PostedDate là ngày hạch toán. Với dòng tồn đầu kỳ là 1970-01-01.
	PostedDate time.Time `json:"postedDate"`

	// Bốn trường số lượng và giá trị, đã quy về đơn vị chính.
	//
	// Với dòng tồn đầu kỳ, MainIWQuantity và IWAmount đã trừ sẵn phần xuất
	// (nhập trừ xuất), hai trường còn lại bằng 0. Nhờ vậy bên nhận cộng dồn
	// theo một công thức duy nhất cho cả hai loại dòng.
	MainIWQuantity decimal.Decimal `json:"mainIwQuantity"`
	MainOWQuantity decimal.Decimal `json:"mainOwQuantity"`
	IWAmount       decimal.Decimal `json:"iwAmount"`
	OWAmount       decimal.Decimal `json:"owAmount"`
}
