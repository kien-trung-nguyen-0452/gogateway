package so_chi_tiet_cong_no_phai_thu

import (
	"time"

	"github.com/shopspring/decimal"
)

// OrderType phân loại dòng trong sổ. Java map sang ChiTietCongNoPhaiThuDTO.
const (
	OrderTypeOpening = 0 // Số dư đầu kỳ
	OrderTypeDetail  = 1 // Dòng chi tiết

	// Loại tài khoản (dim_account.account_group_kind / AccountList.AccountGroupKind).
	KindDebit  = 0 // tài khoản dư Nợ
	KindCredit = 1 // tài khoản dư Có
	KindBoth   = 2 // lưỡng tính
)

// OpeningDescription là diễn giải của dòng số dư đầu kỳ — giữ nguyên chuỗi của proc.
const OpeningDescription = "Số dư đầu kỳ"

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

// Row ánh xạ một dòng kết quả của Proc_Chi_Tiet_Cong_No_Phai_Thu.
type Row struct {
	OrderType int32 `json:"orderType"`

	AccountingObjectID   string `json:"accountingObjectId"`
	AccountingObjectCode string `json:"accountingObjectCode"`
	AccountingObjectName string `json:"accountingObjectName"`

	ReferenceID string `json:"referenceId"`
	TypeID      int32  `json:"typeId"`

	PostedDate time.Time `json:"postedDate"`
	Date       time.Time `json:"date"` // ngày chứng từ
	No         string    `json:"no"`   // no_fbook hoặc no_mbook tuỳ loại sổ

	Description string `json:"description"`

	AccountNumber              string `json:"accountNumber"`
	CorrespondingAccountNumber string `json:"correspondingAccountNumber"`

	DebitAmountNT  JSONDecimal `json:"debitAmountNt"`
	DebitAmount    JSONDecimal `json:"debitAmount"`
	CreditAmountNT JSONDecimal `json:"creditAmountNt"`
	CreditAmount   JSONDecimal `json:"creditAmount"`

	// Số dư cuối sau dòng này, cộng dồn theo từng đối tượng (walker).
	ClosingDebitAmountNT  JSONDecimal `json:"closingDebitAmountNt"`
	ClosingDebitAmount    JSONDecimal `json:"closingDebitAmount"`
	ClosingCreditAmountNT JSONDecimal `json:"closingCreditAmountNt"`
	ClosingCreditAmount   JSONDecimal `json:"closingCreditAmount"`

	OrderPriority int32 `json:"orderPriority"`
}

// Response là thân trả về của chế độ JSON gộp.
type Response struct {
	RowCount  int   `json:"rowCount"`
	ElapsedMs int64 `json:"elapsedMs"`
	Data      []Row `json:"data"`
}
