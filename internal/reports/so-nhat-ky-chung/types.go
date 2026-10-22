package so_nhat_ky_chung

import (
	"time"

	"github.com/shopspring/decimal"
)

// OrderType phân loại dòng trong sổ. Java map sang SoNhatKyChungDTO.oderType.
const (
	OrderTypeAccum  = 0 // Số lũy kế kỳ trước chuyển sang
	OrderTypeDetail = 1 // Dòng chi tiết
)

// AccumDescription là diễn giải của dòng lũy kế — giữ nguyên chuỗi của proc.
const AccumDescription = "Số lũy kế kỳ trước chuyển sang"

// JSONDecimal xuất ra JSON dưới dạng SỐ (không nháy) nhưng giữ nguyên toàn bộ
// chữ số từ Decimal(25,10) — không đi qua float64. Xem giải thích đầy đủ ở
// so-chi-tiet-cac-tai-khoan/types.go.
type JSONDecimal struct {
	decimal.Decimal
}

// MarshalJSON ghi chữ số trần, không nháy.
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

// D bọc decimal.Decimal thành JSONDecimal.
func D(v decimal.Decimal) JSONDecimal { return JSONDecimal{v} }

// Row ánh xạ một dòng kết quả của Proc_SO_NHAT_KY_CHUNG.
type Row struct {
	OrderType int32 `json:"orderType"`

	ReferenceID string `json:"referenceId"`
	TypeID      int32  `json:"typeId"`

	PostedDate time.Time `json:"postedDate"`
	Date       time.Time `json:"date"` // ngày chứng từ
	No         string    `json:"no"`   // no_fbook hoặc no_mbook tuỳ loại sổ

	InvoiceNo     string    `json:"invoiceNo"`
	InvoiceSeries string    `json:"invoiceSeries"`
	InvoiceDate   time.Time `json:"invoiceDate"`

	Description string `json:"description"`
	Reason      string `json:"reason"`

	AccountNumber              string `json:"accountNumber"`
	CorrespondingAccountNumber string `json:"correspondingAccountNumber"`

	// ── TIỀN TỆ ─────────────────────────────────────────────────────────────
	ExchangeRate   JSONDecimal `json:"exchangeRate"`
	DebitAmountNT  JSONDecimal `json:"debitAmountNT"`
	DebitAmount    JSONDecimal `json:"debitAmount"`
	CreditAmountNT JSONDecimal `json:"creditAmountNT"`
	CreditAmount   JSONDecimal `json:"creditAmount"`

	AccountingObjectCode string `json:"accountingObjectCode"`
	AccountingObjectName string `json:"accountingObjectName"`
	ExpenseItemCode      string `json:"expenseItemCode"`
	ExpenseItemName      string `json:"expenseItemName"`
	BudgetItemCode       string `json:"budgetItemCode"`
	BudgetItemName       string `json:"budgetItemName"`
	OrganizationUnitCode string `json:"organizationUnitCode"`
	OrganizationUnitName string `json:"organizationUnitName"`
	CostSetCode          string `json:"costSetCode"`
	CostSetName          string `json:"costSetName"`
	EMContract           string `json:"eMContract"`
	StatisticsCode       string `json:"statisticsCode"`
	StatisticsCodeName   string `json:"statisticsCodeName"`

	CustomField       [5]string `json:"customField"`
	CustomFieldDetail [5]string `json:"customFieldDetail"`

	IsUnreasonableCost string `json:"isUnreasonableCost"`

	SortOrder     int32 `json:"sortOrder"`
	OrderPriority int32 `json:"orderPriority"`
}

type Response struct {
	RowCount  int   `json:"rowCount"`
	ElapsedMs int64 `json:"elapsedMs"`
	Data      []Row `json:"data"`
}

// accum gom bốn cột tiền của dòng "Số lũy kế kỳ trước chuyển sang".
type accum struct {
	Debit      decimal.Decimal
	Credit     decimal.Decimal
	DebitOrig  decimal.Decimal
	CreditOrig decimal.Decimal
}
