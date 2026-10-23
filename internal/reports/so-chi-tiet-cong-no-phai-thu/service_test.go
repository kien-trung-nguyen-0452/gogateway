package so_chi_tiet_cong_no_phai_thu

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

const testCompany = "3d54febe-0c40-4a4f-a2b3-dd5f4a82e428"

func baseParams() QueryParams {
	return QueryParams{
		CompanyIDs: []string{testCompany}, PrimaryCompanyID: testCompany,
		FromDate: "2025-01-01", ToDate: "2025-01-31",
		IsFinancialBook: 1, AccountNumber: "131", CurrencyID: "TH", CheckAll: 1,
	}
}

// Mọi tổ hợp (gộp × staging × có lọc phát sinh × loại sổ) phải điền hết placeholder: sót
// một cái là câu SQL sai cú pháp chỉ lộ ra khi chạy trên ClickHouse.
func TestBuildQueryFillsAllPlaceholders(t *testing.T) {
	for _, group := range []int{0, 1} {
		for _, staging := range []bool{false, true} {
			for _, hasData := range []int{0, 1} {
				for _, checkAll := range []int{0, 1} {
					p := baseParams()
					p.GroupSameItem, p.GetAccountHasData, p.CheckAll = group, hasData, checkAll
					p.ObjectIDs = []string{testCompany}
					p.ClusterID = "EB99_GR2"
					for name, sql := range map[string]string{
						"main":  buildQuery(p.CompanyIDs, p, 0, "no_fbook", staging, true),
						"count": buildCountQuery(p.CompanyIDs, p, 0, "no_fbook", staging),
					} {
						if strings.Contains(sql, "{{") {
							t.Errorf("%s (group=%d staging=%v hasData=%d checkAll=%d) còn placeholder:\n%s",
								name, group, staging, hasData, checkAll, sql)
						}
					}
				}
			}
		}
	}
}

// Câu đếm bọc đúng truy vấn chính nhưng không được mang ORDER BY / SETTINGS bên trong.
func TestCountQueryHasNoOrderBy(t *testing.T) {
	p := baseParams()
	sql := buildCountQuery(p.CompanyIDs, p, 1, "no_mbook", false)
	if strings.Contains(sql, "ORDER BY") {
		t.Errorf("câu đếm không được có ORDER BY:\n%s", sql)
	}
	if !strings.HasPrefix(sql, "SELECT count() FROM (") {
		t.Errorf("câu đếm phải bọc truy vấn chính:\n%s", sql)
	}
}

// Bộ lọc mã/tên/địa chỉ/nhóm chỉ áp dụng khi CheckAll = 1; CheckAll = 0 chỉ lấy ListID.
func TestAOScope(t *testing.T) {
	p := baseParams()
	p.ObjectCode, p.ObjectName, p.ObjectAddr = "KH", "An", "Hà Nội"
	p.GroupID = testCompany
	p.ObjectIDs = []string{testCompany}

	all := buildAOCTE(p)
	for _, want := range []string{"id NOT IN", "positionCaseInsensitiveUTF8(code,", "positionCaseInsensitiveUTF8(name,",
		"address IS NULL OR", "account_object_group_id", "is_active", "object_type"} {
		if !strings.Contains(all, want) {
			t.Errorf("CheckAll=1 thiếu %q:\n%s", want, all)
		}
	}

	p.CheckAll = 0
	only := buildAOCTE(p)
	if !strings.Contains(only, "id IN CAST") {
		t.Errorf("CheckAll=0 phải lọc id IN danh sách:\n%s", only)
	}
	for _, bad := range []string{"NOT IN", "positionCaseInsensitiveUTF8", "account_object_group_id"} {
		if strings.Contains(only, bad) {
			t.Errorf("CheckAll=0 không được áp dụng %q:\n%s", bad, only)
		}
	}
}

func TestSQLStringEscapes(t *testing.T) {
	if got := sqlString(`a'b\c`); got != `'a\'b\\c'` {
		t.Errorf("sqlString = %s", got)
	}
	p := baseParams()
	p.ObjectCode = `x') OR 1=1 --`
	if sql := buildAOCTE(p); strings.Contains(sql, `x')`) {
		t.Errorf("chuỗi lọc chưa được escape:\n%s", sql)
	}
}

func TestValidateParams(t *testing.T) {
	if err := validateParams(baseParams()); err != nil {
		t.Fatalf("tham số hợp lệ bị từ chối: %v", err)
	}
	cases := map[string]func(p *QueryParams){
		"toDate chèn SQL":         func(p *QueryParams) { p.ToDate = "2025-01-31' OR 1=1 --" },
		"clusterId chèn SQL":      func(p *QueryParams) { p.ClusterID = "x'; DROP" },
		"accountNumber chèn":      func(p *QueryParams) { p.AccountNumber = "131' OR '1'='1" },
		"accountNumber rỗng":      func(p *QueryParams) { p.AccountNumber = "" },
		"currencyId chèn SQL":     func(p *QueryParams) { p.CurrencyID = "VND'--" },
		"objectId không là UUID":  func(p *QueryParams) { p.ObjectIDs = []string{"1' OR 1=1"} },
		"groupId không là UUID":   func(p *QueryParams) { p.GroupID = "abc" },
		"groupSameItem ngoài 0/1": func(p *QueryParams) { p.GroupSameItem = 2 },
		"toDate sớm hơn fromDate": func(p *QueryParams) { p.ToDate = "2024-12-31" },
		"bộ lọc quá dài":          func(p *QueryParams) { p.ObjectName = strings.Repeat("a", maxFilterLen+1) },
	}
	for name, mutate := range cases {
		p := baseParams()
		mutate(&p)
		if validateParams(p) == nil {
			t.Errorf("%s phải bị từ chối", name)
		}
	}
}

func dec(s string) JSONDecimal {
	d, _ := decimal.NewFromString(s)
	return D(d)
}

func detailRow(obj string, debit, credit string) Row {
	return Row{OrderType: OrderTypeDetail, AccountingObjectID: obj, DebitAmount: dec(debit), CreditAmount: dec(credit),
		DebitAmountNT: dec("0"), CreditAmountNT: dec("0"), PostedDate: time.Date(2025, 1, 5, 0, 0, 0, 0, time.UTC)}
}

func openingRow(obj string, debit, credit string) Row {
	return Row{OrderType: OrderTypeOpening, AccountingObjectID: obj, DebitAmount: dec(debit), CreditAmount: dec(credit),
		DebitAmountNT: dec("0"), CreditAmountNT: dec("0")}
}

func walk(t *testing.T, kind int32, in ...Row) []Row {
	t.Helper()
	var out []Row
	w := &walker{account: "131", kind: kind, onRow: func(r Row) error { out = append(out, r); return nil }}
	for _, r := range in {
		if err := w.push(r); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// TK 131 dư Nợ: đầu kỳ Nợ 1000, bán thêm Nợ 500, khách trả Có 300 → 1000, 1500, 1200.
func TestWalkerDebitAccount(t *testing.T) {
	out := walk(t, KindDebit,
		openingRow("A", "1000", "0"),
		detailRow("A", "500", "0"),
		detailRow("A", "0", "300"),
	)
	want := []string{"1000", "1500", "1200"}
	for i, w := range want {
		if got := out[i].ClosingDebitAmount.String(); got != w {
			t.Errorf("dòng %d: ClosingDebit = %s, want %s", i, got, w)
		}
		if !out[i].ClosingCreditAmount.IsZero() {
			t.Errorf("dòng %d: TK dư Nợ không được có ClosingCredit = %s", i, out[i].ClosingCreditAmount)
		}
	}
	// Dòng đầu kỳ chỉ mang số dư: phát sinh phải bằng 0 như proc.
	if !out[0].DebitAmount.IsZero() || !out[0].CreditAmount.IsZero() {
		t.Errorf("dòng đầu kỳ không được mang phát sinh: nợ=%s có=%s", out[0].DebitAmount, out[0].CreditAmount)
	}
	if out[0].Description != OpeningDescription || out[0].AccountNumber != "131" {
		t.Errorf("dòng đầu kỳ: %+v", out[0])
	}
}

// Khách trả dư: số dư Nợ âm ở TK dư Nợ phải giữ nguyên dấu (proc: -1 * @CloseAmount).
func TestWalkerDebitAccountOverpaid(t *testing.T) {
	out := walk(t, KindDebit, detailRow("A", "100", "0"), detailRow("A", "0", "250"))
	if got := out[1].ClosingDebitAmount.String(); got != "-150" {
		t.Errorf("ClosingDebit = %s, want -150", got)
	}
}

// TK lưỡng tính (131 theo thông tư): số dư Nợ và Có tách theo dấu, không bao giờ âm.
func TestWalkerBothKind(t *testing.T) {
	out := walk(t, KindBoth, detailRow("A", "100", "0"), detailRow("A", "0", "250"))
	if out[0].ClosingDebitAmount.String() != "100" || !out[0].ClosingCreditAmount.IsZero() {
		t.Errorf("dòng 0: nợ=%s có=%s", out[0].ClosingDebitAmount, out[0].ClosingCreditAmount)
	}
	if !out[1].ClosingDebitAmount.IsZero() || out[1].ClosingCreditAmount.String() != "150" {
		t.Errorf("dòng 1: nợ=%s có=%s", out[1].ClosingDebitAmount, out[1].ClosingCreditAmount)
	}
}

// Số dư cộng dồn phải reset khi sang đối tượng khác — không mang số dư của đối tượng trước.
func TestWalkerResetsPerObject(t *testing.T) {
	out := walk(t, KindDebit,
		openingRow("A", "1000", "0"),
		detailRow("A", "10", "0"),
		detailRow("B", "5", "0"),
	)
	if got := out[2].ClosingDebitAmount.String(); got != "5" {
		t.Errorf("đối tượng B bắt đầu từ 0 nhưng ClosingDebit = %s", got)
	}
}
