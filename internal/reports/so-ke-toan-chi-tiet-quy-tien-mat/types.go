package so_ke_toan_chi_tiet_quy_tien_mat

import (
	"time"

	"github.com/shopspring/decimal"
)

// PositionOrder phân loại dòng — khớp cột cùng tên trong
// Proc_SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT.
const (
	PositionOpening = 0 // Tồn đầu kỳ
	PositionDetail  = 1 // Phát sinh trong kỳ
	PositionGroup   = 2 // Cộng nhóm (cuối mỗi tài khoản)
)

// CAType phân loại chứng từ.
//
// Một bút toán GL chỉ thuộc MỘT trong hai loại: có DebitAmount thì là phiếu
// thu, có CreditAmount thì là phiếu chi. Proc gốc tách bằng UNION ALL nên bút
// toán vừa Nợ vừa Có sẽ ra HAI dòng — xem ghi chú ở service.go.
const (
	CATypeReceipt = 0 // Phiếu thu
	CATypePayment = 1 // Phiếu chi
)

// JSONDecimal xuất ra JSON dưới dạng SỐ (không nháy) nhưng giữ nguyên toàn bộ
// chữ số từ Decimal(25,10) — không đi qua float64.
//
// Frontend dùng pipe ebcurrency vốn cần number. Ép qua float64 ở Go thì mất
// chính xác ngay tại server; cách này ghi thẳng chữ số vào JSON.
type JSONDecimal struct {
	decimal.Decimal
}

func (d JSONDecimal) MarshalJSON() ([]byte, error) {
	return []byte(d.Decimal.String()), nil
}

func (d *JSONDecimal) UnmarshalJSON(b []byte) error {
	s := string(b)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		s = s[1 : len(s)-1]
	}
	if s == "" || s == "null" {
		d.Decimal = decimal.Zero
		return nil
	}
	v, err := decimal.NewFromString(s)
	if err != nil {
		return err
	}
	d.Decimal = v
	return nil
}

func D(v decimal.Decimal) JSONDecimal { return JSONDecimal{v} }

// Row ánh xạ 1-1 với bảng #Result của proc gốc.
type Row struct {
	PositionOrder int32 `json:"positionOrder"`
	CAType        int32 `json:"caType"`
	OrderPriority int32 `json:"orderPriority"`

	RefID  string `json:"refId"`
	TypeID int32  `json:"typeId"`

	Date       time.Time `json:"date"`       // GL.Date — ngày chứng từ
	PostedDate time.Time `json:"postedDate"` // GL.PostedDate — ngày hạch toán

	// Cùng một số hiệu chứng từ (NoFBook/NoMBook) nhưng đặt vào cột nào tuỳ
	// CAType: phiếu thu vào ReceiptRefNo, phiếu chi vào PaymentRefNo.
	ReceiptRefNo string `json:"receiptRefNo"`
	PaymentRefNo string `json:"paymentRefNo"`

	JournalMemo          string `json:"journalMemo"`
	Reason               string `json:"reason"`
	Account              string `json:"account"`
	AccountCorresponding string `json:"accountCorresponding"`
	Note                 string `json:"note"`

	// ── TIỀN TỆ ─────────────────────────────────────────────────────────────
	// PhatSinhNo/Co: cột người dùng thấy, đã chọn theo typeShowCurrency
	//   (0 = quy đổi → debit_amount, 1 = nguyên tệ → debit_amount_original)
	// PhatSinhNoQD/CoQD: luôn là bản quy đổi, dùng cho dòng tổng
	PhatSinhNo   JSONDecimal `json:"phatSinhNo"`
	PhatSinhCo   JSONDecimal `json:"phatSinhCo"`
	PhatSinhNoQD JSONDecimal `json:"phatSinhNoQD"`
	PhatSinhCoQD JSONDecimal `json:"phatSinhCoQD"`

	// SoTon là số dư LUỸ KẾ, tính chạy dọc theo từng tài khoản.
	// Xem walker.push() ở service.go.
	SoTon   JSONDecimal `json:"soTon"`
	SoTonQD JSONDecimal `json:"soTonQD"`

	ExchangeRate JSONDecimal `json:"exchangeRate"`

	AccountingObjectID   string `json:"accountingObjectId"`
	AccountingObjectCode string `json:"accountingObjectCode"`
	AccountingObjectName string `json:"accountingObjectName"`

	CustomField       [5]string `json:"customField"`
	CustomFieldDetail [5]string `json:"customFieldDetail"`

	// ── SÁU CỘT CHIỀU — CHƯA CÓ TRONG FACT ──────────────────────────────────
	// ExpenseItem, BudgetItem, OrganizationUnit (= GL.DepartmentID), CostSet,
	// Contract, StatisticsCode.
	//
	// Proc gốc mang chúng theo ở chế độ CHI TIẾT, nhưng nhánh GỘP thì bỏ hẳn:
	//     group by GL.ReferenceID, Account, AccountCorresponding, AccountingObjectID
	// Nên chúng KHÔNG ảnh hưởng số liệu, chỉ để hiển thị.
	//
	// Hiện fact chưa có. Giữ field ở đây để hợp đồng API ổn định; khi fact
	// được bổ sung thì chỉ cần điền, không phải đổi proto hay sửa frontend.
	ExpenseItemCode      string `json:"expenseItemCode"`
	ExpenseItemName      string `json:"expenseItemName"`
	BudgetItemCode       string `json:"budgetItemCode"`
	BudgetItemName       string `json:"budgetItemName"`
	OrganizationUnitCode string `json:"organizationUnitCode"`
	OrganizationUnitName string `json:"organizationUnitName"`
	CostSetCode          string `json:"costSetCode"`
	CostSetName          string `json:"costSetName"`
	ContractNo           string `json:"contractNo"`
	StatisticsCodeCode   string `json:"statisticsCodeCode"`
	StatisticsCodeName   string `json:"statisticsCodeName"`
}

type Response struct {
	RowCount  int   `json:"rowCount"`
	ElapsedMs int64 `json:"elapsedMs"`
	Data      []Row `json:"data"`
}

// balance gom bốn cột tiền của một tài khoản. Kiểu nội bộ để tính toán, dùng
// decimal.Decimal thuần chứ không JSONDecimal.
type balance struct {
	Debit    decimal.Decimal
	Credit   decimal.Decimal
	DebitQD  decimal.Decimal
	CreditQD decimal.Decimal
}

func (b *balance) add(o balance) {
	b.Debit = b.Debit.Add(o.Debit)
	b.Credit = b.Credit.Add(o.Credit)
	b.DebitQD = b.DebitQD.Add(o.DebitQD)
	b.CreditQD = b.CreditQD.Add(o.CreditQD)
}

func (b balance) net() decimal.Decimal   { return b.Debit.Sub(b.Credit) }
func (b balance) netQD() decimal.Decimal { return b.DebitQD.Sub(b.CreditQD) }

func (b balance) isZero() bool {
	return b.Debit.IsZero() && b.Credit.IsZero() &&
		b.DebitQD.IsZero() && b.CreditQD.IsZero()
}
