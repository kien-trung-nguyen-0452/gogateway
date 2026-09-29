package so_chi_tiet_tai_khoan

import (
	"time"

	"github.com/shopspring/decimal"
)

// OrderType phân loại dòng trong sổ. Java map thẳng sang SoChiTietTaiKhoanDTO.
const (
	OrderTypeOpening    = 0 // Số dư đầu kỳ
	OrderTypeDetail     = 1 // Dòng chi tiết
	OrderTypeTotal      = 2 // Cộng phát sinh (theo từng tài khoản)
	OrderTypeClosing    = 3 // Số dư cuối kỳ
	OrderTypeGrandTotal = 4 // Tổng cộng toàn báo cáo
)

// AccountGroupKind — cách quy đổi số dư thuần sang cột Nợ/Có.
const (
	KindDebit  = 0 // dư Nợ
	KindCredit = 1 // dư Có
	KindBoth   = 2 // lưỡng tính
)

// JSONDecimal xuất ra JSON dưới dạng SỐ (không nháy) nhưng giữ nguyên toàn bộ
// chữ số từ Decimal(25,10) — không đi qua float64.
//
// Frontend dùng pipe `ebcurrency` vốn cần number, nên không thể trả string.
// Nhưng nếu ép qua float64 ở Go thì mất chính xác ngay tại server. Cách này
// ghi thẳng chữ số vào JSON; phần mất mát (nếu có) chỉ xảy ra lúc JSON.parse
// của trình duyệt và chỉ với giá trị vượt ~15-17 chữ số có nghĩa — hiếm gặp
// với số tiền VND thực tế.
type JSONDecimal struct {
	decimal.Decimal
}

// MarshalJSON ghi chữ số trần, không nháy.
// toPlainString tránh ký hiệu khoa học ("1E+3") mà JS đọc được nhưng người
// đọc log thì không.
func (d JSONDecimal) MarshalJSON() ([]byte, error) {
	return []byte(d.Decimal.String()), nil
}

func (d *JSONDecimal) UnmarshalJSON(b []byte) error {
	// Nhận cả dạng số lẫn dạng chuỗi có nháy, để không vỡ nếu client cũ gửi lại.
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

// D bọc decimal.Decimal thành JSONDecimal. Viết ngắn vì dùng rất nhiều chỗ.
func D(v decimal.Decimal) JSONDecimal { return JSONDecimal{v} }

type Row struct {
	OrderType   int32 `json:"orderType"`
	OrderNumber int32 `json:"orderNumber"`
	Bold        bool  `json:"bold"`

	AccountNumber       string `json:"accountNumber"`
	AccountCategoryKind int32  `json:"accountCategoryKind"`

	// Chuỗi "154 - Chi phí SXKD dở dang" — frontend dùng cho dòng tiêu đề
	// nhóm khi người dùng chọn xem "Tất cả" tài khoản.
	AccountNameWithAccountNumber string `json:"accountNameWithAccountNumber"`

	KeyID       string `json:"keyId"`
	ReferenceID string `json:"referenceId"`
	TypeID      int32  `json:"typeId"`

	PostedDate  time.Time `json:"postedDate"`
	Date        time.Time `json:"date"`
	InvoiceDate time.Time `json:"invoiceDate"`
	InvoiceNo   string    `json:"invoiceNo"`
	No          string    `json:"no"` // no_fbook hoặc no_mbook tuỳ loại sổ

	Reason               string `json:"reason"`
	JournalMemo          string `json:"journalMemo"`
	AccountCorresponding string `json:"accountCorresponding"`

	// ── TIỀN TỆ ─────────────────────────────────────────────────────────────
	DebitAmount          JSONDecimal `json:"debitAmount"`
	CreditAmount         JSONDecimal `json:"creditAmount"`
	DebitAmountOriginal  JSONDecimal `json:"debitAmountOriginal"`
	CreditAmountOriginal JSONDecimal `json:"creditAmountOriginal"`

	ClosingDebitAmount          JSONDecimal `json:"closingDebitAmount"`
	ClosingCreditAmount         JSONDecimal `json:"closingCreditAmount"`
	ClosingDebitAmountOriginal  JSONDecimal `json:"closingDebitAmountOriginal"`
	ClosingCreditAmountOriginal JSONDecimal `json:"closingCreditAmountOriginal"`

	ExchangeRate JSONDecimal `json:"exchangeRate"`

	OrderPriority      int32  `json:"orderPriority"`
	IsUnreasonableCost string `json:"isUnreasonableCost"`

	AccountingObjectCode string `json:"accountingObjectCode"`
	AccountingObjectName string `json:"accountingObjectName"`

	CustomField       [5]string `json:"customField"`
	CustomFieldDetail [5]string `json:"customFieldDetail"`
}

type Response struct {
	RowCount  int   `json:"rowCount"`
	ElapsedMs int64 `json:"elapsedMs"`
	Data      []Row `json:"data"`
}

// balance gom bốn cột tiền của một tài khoản — dùng cho SDDK và cộng phát sinh.
// Dùng decimal.Decimal thuần (không JSONDecimal) vì đây là kiểu nội bộ để
// tính toán, không bao giờ serialize trực tiếp.
type balance struct {
	Debit      decimal.Decimal
	Credit     decimal.Decimal
	DebitOrig  decimal.Decimal
	CreditOrig decimal.Decimal
}

func (b *balance) add(o balance) {
	b.Debit = b.Debit.Add(o.Debit)
	b.Credit = b.Credit.Add(o.Credit)
	b.DebitOrig = b.DebitOrig.Add(o.DebitOrig)
	b.CreditOrig = b.CreditOrig.Add(o.CreditOrig)
}

func (b balance) net() decimal.Decimal     { return b.Debit.Sub(b.Credit) }
func (b balance) netOrig() decimal.Decimal { return b.DebitOrig.Sub(b.CreditOrig) }

func (b balance) isZero() bool {
	return b.Debit.IsZero() && b.Credit.IsZero() &&
		b.DebitOrig.IsZero() && b.CreditOrig.IsZero()
}
