// Package so_chi_tiet_tai_khoan — sổ chi tiết tài khoản (SoChiTietTaiKhoan).
//
// KHÁC BIỆT CỐT LÕI SO VỚI BẢN JAVA CŨ
// ------------------------------------
// AccountLedgerDirectRepository.java có hai khuyết tật mà việc viết lại buộc
// phải xử lý:
//
//  1. BỘ NHỚ — queryForList() nạp toàn bộ dòng vào List<Map<String,Object>>.
//     Mỗi dòng là HashMap ~30 entry (~2KB) nên 1 triệu dòng ≈ 2GB heap TRƯỚC
//     KHI dòng đầu tiên được xử lý.
//
//  2. O(N×M) — lặp lồng nhau qua tài khoản × dòng. 1000 tài khoản × 1tr dòng
//     = 1 tỷ vòng, 99,9% chỉ để `continue`. Dữ liệu đã ORDER BY account_number
//     nhưng thông tin đó bị bỏ phí.
//
// Bản Go tận dụng thứ tự sẵn có: mọi dòng của một tài khoản nằm liền nhau nên
// dùng máy trạng thái một lượt. O(M), bộ nhớ hằng số.
package so_chi_tiet_tai_khoan

import (
	"context"
	_ "embed"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

//go:embed query.sql
var queryTemplate string

//go:embed query_opening.sql
var openingTemplate string

//go:embed staging_source.sql
var stagingSourceExpr string

// factSourceExpr là giá trị {{GL_SOURCE}} mặc định — đọc thẳng fact table.
const factSourceExpr = "eb_dwh.fact_gl_entry_line AS f FINAL"

var (
	uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

	// Số hiệu tài khoản CÓ THỂ chứa khoảng trắng và gạch ngang: "154 - TT" là
	// một tài khoản THẬT, tồn tại song song với "154".
	//
	// Dùng danh sách CHẶN thay vì CHO PHÉP: không kiểm soát được quy ước đặt
	// số hiệu của từng khách hàng, nhưng kiểm soát được ký tự nguy hiểm.
	accountPattern = regexp.MustCompile(`^[^'"\\\x00-\x1f]{1,50}$`)

	currencyPattern = regexp.MustCompile(`^[0-9A-Za-z]{2,10}$`)
	clusterPattern  = regexp.MustCompile(`^[0-9A-Za-z._-]{1,50}$`)
)

type Service struct {
	conn clickhouse.Conn

	// useStaging: true = doc tu eb_staging.stg_general_ledger(_detail) thay vi
	// eb_dwh.fact_gl_entry_line - xem staging_source.sql va
	// config.SoChiTietTaiKhoanUseStaging. Dung tam thoi khi pipeline fact loi.
	useStaging bool
}

func NewService(conn clickhouse.Conn, useStaging bool) *Service {
	return &Service{conn: conn, useStaging: useStaging}
}

type driverRows interface {
	Scan(dest ...any) error
}

type QueryParams struct {
	CompanyIDs       []string // đã swap UUID phía Java
	PrimaryCompanyID string
	AccountNumbers   []string
	FromDate         string // YYYY-MM-DD
	ToDate           string // YYYY-MM-DD
	CurrencyID       string // "TH" = tất cả
	IsFinancialBook  int    // 1 = sổ tài chính, 0 = sổ quản trị
	IsDependent      int    // 1 = gộp đơn vị phụ thuộc
	GroupSameItem    int    // 1 = cộng gộp bút toán cùng chứng từ
	IsParentNode     bool   // true = expand xuống tài khoản con
	ClusterID        string // multi-cluster, rỗng = không lọc
}

// =============================================================================
// ENTRYPOINT
// =============================================================================

// GetSoChiTiet gom toàn bộ kết quả vào slice. Chỉ dùng cho báo cáo nhỏ.
//
// CẢNH BÁO: sổ chi tiết cả năm nhiều tài khoản có thể vài trăm nghìn dòng. Ở
// quy mô đó phải dùng StreamSoChiTiet, nếu không sẽ lặp lại đúng vấn đề bộ nhớ
// của bản Java cũ.
func (s *Service) GetSoChiTiet(ctx context.Context, p QueryParams) ([]Row, int64, error) {
	var result []Row
	elapsedMs, err := s.StreamSoChiTiet(ctx, p, func(row Row) error {
		result = append(result, row)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return result, elapsedMs, nil
}

// StreamSoChiTiet gọi onRow cho TỪNG dòng ngay khi dựng xong, không gom vào
// slice. Dùng chung cho cả gRPC streaming lẫn HTTP NDJSON.
func (s *Service) StreamSoChiTiet(
	ctx context.Context, p QueryParams, onRow func(Row) error,
) (int64, error) {
	if err := validateParams(p); err != nil {
		return 0, err
	}
	start := time.Now()

	typeLedger, noCol := 1, "no_mbook"
	if p.IsFinancialBook == 1 {
		typeLedger, noCol = 0, "no_fbook"
	}

	companyIDs, err := s.resolveCompanyScope(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("resolve company scope: %w", err)
	}

	accounts := p.AccountNumbers
	if p.IsParentNode {
		accounts, err = s.expandAccounts(ctx, p.PrimaryCompanyID, accounts)
		if err != nil {
			return 0, fmt.Errorf("expand accounts: %w", err)
		}
	}
	accounts = cleanAccounts(accounts)
	if len(accounts) == 0 {
		return 0, fmt.Errorf("khong con tai khoan nao sau khi loc")
	}
	for _, a := range accounts {
		if !accountPattern.MatchString(a) {
			return 0, fmt.Errorf("so hieu tai khoan khong hop le: %q", a)
		}
	}

	// 	kindMap, nameMap, err := s.resolveAccountInfo(ctx, p.PrimaryCompanyID, accounts, accountNameColumn(p.FromDate))
	kindMap, nameMap, err := s.resolveAccountInfo(ctx, p.PrimaryCompanyID, accounts, accountNameColumn(p.FromDate))
	if err != nil {
		return 0, fmt.Errorf("resolve account info: %w", err)
	}
	sddkMap, err := s.loadOpeningBalances(ctx, companyIDs, accounts, p, typeLedger)
	if err != nil {
		return 0, fmt.Errorf("load opening balances: %w", err)
	}

	w := &walker{
		onRow:    onRow,
		kindMap:  kindMap,
		nameMap:  nameMap,
		sddkMap:  sddkMap,
		accounts: accounts,
		seen:     make(map[string]bool, len(accounts)),
	}

	sql := buildDetailQuery(companyIDs, accounts, p, typeLedger, noCol, s.useStaging)
	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		// Client ngắt giữa chừng thì dừng ngay, không quét nốt phần còn lại.
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		d, err := scanRow(rows)
		if err != nil {
			return 0, fmt.Errorf("scan error: %w", err)
		}
		if err := w.push(d); err != nil {
			return 0, fmt.Errorf("loi xu ly dong: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %w", err)
	}

	if err := w.finish(); err != nil {
		return 0, fmt.Errorf("loi ket thuc: %w", err)
	}

	return time.Since(start).Milliseconds(), nil
}

// =============================================================================
// WALKER — máy trạng thái một lượt
// =============================================================================

type walker struct {
	onRow    func(Row) error
	kindMap  map[string]int32
	nameMap  map[string]string
	sddkMap  map[string]balance
	accounts []string
	seen     map[string]bool

	current string
	opening balance

	// running là số dư luỹ kế TÍNH ĐẾN VÀ BAO GỒM dòng chi tiết hiện tại —
	// khởi tạo bằng opening khi mở tài khoản, cộng dồn sau mỗi dòng. Khớp
	// hành vi bản Java cũ (biến closeAmountOC/closeAmount trong vòng lặp):
	// dòng chi tiết đầu tiên = opening + (credit-debit) của chính nó, dòng
	// sau = running của dòng trước + (credit-debit) của nó.
	//
	// TRƯỚC ĐÂY bản Go dùng thẳng `opening` (không đổi) cho applyKindClosing
	// của MỌI dòng chi tiết trong tài khoản → mọi dòng hiện cùng một số dư,
	// sai với báo cáo gốc (mỗi dòng phải hiện số dư SAU khi phát sinh dòng
	// đó).
	running balance

	tc    balance
	hasTC bool

	// ── Tích luỹ cho dòng Tổng cộng ─────────────────────────────────────────
	grandTC balance

	// Số dư cuối kỳ gộp. KHÔNG cộng từ `net` thuần được: mỗi tài khoản đã quy
	// đổi net sang cặp cột Nợ/Có theo `kind` của nó (applyKindClosing), nên
	// phải cộng SAU khi quy đổi. Một tài khoản dư Có -500 và một tài khoản dư
	// Nợ +500 cộng net lại thành 0, nhưng báo cáo phải hiện Nợ 500 / Có 500.
	grandClosingDebit      decimal.Decimal
	grandClosingCredit     decimal.Decimal
	grandClosingDebitOrig  decimal.Decimal
	grandClosingCreditOrig decimal.Decimal

	anyAccount bool
}

func (w *walker) push(d Row) error {
	if d.AccountNumber != w.current {
		if w.current != "" {
			if err := w.closeAccount(); err != nil {
				return err
			}
		}
		if err := w.openAccount(d.AccountNumber); err != nil {
			return err
		}
	}

	kind := w.kind(w.current)
	d.OrderType = OrderTypeDetail
	d.OrderNumber = OrderTypeDetail
	d.AccountCategoryKind = kind
	d.AccountNameWithAccountNumber = w.name(w.current)

	// Cộng dòng hiện tại vào số dư luỹ kế TRƯỚC khi tính Closing cho chính
	// dòng này — running phải phản ánh số dư SAU dòng này.
	w.running.add(balance{
		Debit:      d.DebitAmount.Decimal,
		Credit:     d.CreditAmount.Decimal,
		DebitOrig:  d.DebitAmountOriginal.Decimal,
		CreditOrig: d.CreditAmountOriginal.Decimal,
	})
	applyKindClosing(&d, w.running.net(), w.running.netOrig(), kind)
	if err := w.onRow(d); err != nil {
		return err
	}

	w.tc.add(balance{
		Debit:      d.DebitAmount.Decimal,
		Credit:     d.CreditAmount.Decimal,
		DebitOrig:  d.DebitAmountOriginal.Decimal,
		CreditOrig: d.CreditAmountOriginal.Decimal,
	})
	w.hasTC = true
	return nil
}

func (w *walker) openAccount(acc string) error {
	w.current = acc
	w.seen[acc] = true
	w.tc = balance{}
	w.hasTC = false
	w.opening = w.sddkMap[acc] // zero value nếu không có
	w.running = w.opening      // luỹ kế bắt đầu từ số dư đầu kỳ

	// Dòng SDDK chỉ phát khi khác 0 — giữ đúng hành vi bản Java.
	if w.opening.net().IsZero() && w.opening.netOrig().IsZero() {
		return nil
	}
	w.anyAccount = true
	return w.onRow(w.summaryRow(acc, OrderTypeOpening, "Số dư đầu kỳ",
		w.opening.net(), w.opening.netOrig()))
}

func (w *walker) closeAccount() error {
	acc, kind := w.current, w.kind(w.current)

	if w.hasTC {
		row := Row{
			OrderType:                    OrderTypeTotal,
			OrderNumber:                  OrderTypeTotal,
			Bold:                         true,
			AccountNumber:                acc,
			AccountCategoryKind:          kind,
			AccountNameWithAccountNumber: w.name(acc),
			JournalMemo:                  "Cộng phát sinh",
			DebitAmount:                  D(w.tc.Debit),
			CreditAmount:                 D(w.tc.Credit),
			DebitAmountOriginal:          D(w.tc.DebitOrig),
			CreditAmountOriginal:         D(w.tc.CreditOrig),
			ExchangeRate:                 D(decimal.Zero),
		}
		if err := w.onRow(row); err != nil {
			return err
		}
	}

	w.grandTC.add(w.tc)

	// SDCK phát khi có SDDK khác 0 HOẶC có phát sinh trong kỳ.
	if w.opening.net().IsZero() && w.opening.netOrig().IsZero() && w.tc.isZero() {
		return nil
	}

	closeNet := w.opening.net().Add(w.tc.Debit).Sub(w.tc.Credit)
	closeOrig := w.opening.netOrig().Add(w.tc.DebitOrig).Sub(w.tc.CreditOrig)

	sdck := w.summaryRow(acc, OrderTypeClosing, "Số dư cuối kỳ", closeNet, closeOrig)
	w.accumulateClosing(sdck)
	w.anyAccount = true

	return w.onRow(sdck)
}

// accumulateClosing cộng dồn số dư cuối kỳ ĐÃ QUY ĐỔI theo kind.
// Gọi SAU summaryRow (tức sau applyKindClosing), không phải trước.
func (w *walker) accumulateClosing(r Row) {
	w.grandClosingDebit = w.grandClosingDebit.Add(r.ClosingDebitAmount.Decimal)
	w.grandClosingCredit = w.grandClosingCredit.Add(r.ClosingCreditAmount.Decimal)
	w.grandClosingDebitOrig = w.grandClosingDebitOrig.Add(r.ClosingDebitAmountOriginal.Decimal)
	w.grandClosingCreditOrig = w.grandClosingCreditOrig.Add(r.ClosingCreditAmountOriginal.Decimal)
}

func (w *walker) finish() error {
	if w.current != "" {
		if err := w.closeAccount(); err != nil {
			return err
		}
	}

	// Tài khoản CHỈ có SDDK mà không phát sinh trong kỳ KHÔNG hiển thị trên
	// màn hình (yêu cầu nghiệp vụ — tài khoản không hoạt động trong kỳ thì
	// không cần hiện dòng SDDK/SDCK gây rối màn). Vẫn cộng vào Tổng cộng để
	// số liệu tổng phản ánh đúng toàn bộ số dư thật của các tài khoản đã
	// chọn — chỉ ẩn dòng hiển thị, không đổi số liệu tổng.
	//
	// LƯU Ý: vì vậy Tổng cộng có thể KHÔNG khớp phép cộng tay các dòng đang
	// hiển thị trên màn (do có tài khoản ẩn góp vào tổng) — cố ý, đã xác
	// nhận với nghiệp vụ.
	for _, acc := range w.accounts {
		if w.seen[acc] {
			continue
		}
		b, ok := w.sddkMap[acc]
		if !ok || (b.net().IsZero() && b.netOrig().IsZero()) {
			continue
		}
		w.seen[acc] = true
		// Không phát dòng SDDK/SDCK ra màn (yêu cầu nghiệp vụ ở trên), nhưng
		// vẫn đánh dấu anyAccount=true — có tiền thật góp vào Tổng cộng nên
		// dòng Tổng cộng vẫn phải xuất hiện, kể cả khi TOÀN BỘ tài khoản
		// được chọn đều không phát sinh (không thì màn hình trống trơn dù
		// có số dư thật).
		w.anyAccount = true

		sdck := w.summaryRow(acc, OrderTypeClosing, "Số dư cuối kỳ", b.net(), b.netOrig())
		w.accumulateClosing(sdck)
	}

	return w.emitGrandTotal()
}

// emitGrandTotal phát dòng cuối cùng của báo cáo.
//
// Frontend nhận diện dòng này bằng journalMemo == "Tổng cộng" — chuỗi phải
// khớp CHÍNH XÁC, không thừa khoảng trắng, không đổi hoa thường.
func (w *walker) emitGrandTotal() error {
	// Báo cáo rỗng thì không có gì để tổng — phát dòng toàn số 0 chỉ gây nhiễu.
	if !w.anyAccount {
		return nil
	}

	return w.onRow(Row{
		OrderType:   OrderTypeGrandTotal,
		OrderNumber: OrderTypeGrandTotal,
		Bold:        true,
		JournalMemo: "Tổng cộng",

		DebitAmount:          D(w.grandTC.Debit),
		CreditAmount:         D(w.grandTC.Credit),
		DebitAmountOriginal:  D(w.grandTC.DebitOrig),
		CreditAmountOriginal: D(w.grandTC.CreditOrig),

		ClosingDebitAmount:          D(w.grandClosingDebit),
		ClosingCreditAmount:         D(w.grandClosingCredit),
		ClosingDebitAmountOriginal:  D(w.grandClosingDebitOrig),
		ClosingCreditAmountOriginal: D(w.grandClosingCreditOrig),

		ExchangeRate: D(decimal.Zero),
	})
}

func (w *walker) kind(acc string) int32 {
	if k, ok := w.kindMap[acc]; ok {
		return k
	}
	return KindBoth // mặc định lưỡng tính, khớp bản Java
}

func (w *walker) name(acc string) string {
	if n, ok := w.nameMap[acc]; ok {
		return n
	}
	return acc
}

func (w *walker) summaryRow(
	acc string, orderType int32, memo string, net, netOrig decimal.Decimal,
) Row {
	kind := w.kind(acc)
	r := Row{
		OrderType:                    orderType,
		OrderNumber:                  orderType,
		Bold:                         true,
		AccountNumber:                acc,
		AccountCategoryKind:          kind,
		AccountNameWithAccountNumber: w.name(acc),
		JournalMemo:                  memo,
		DebitAmount:                  D(decimal.Zero),
		CreditAmount:                 D(decimal.Zero),
		DebitAmountOriginal:          D(decimal.Zero),
		CreditAmountOriginal:         D(decimal.Zero),
		ExchangeRate:                 D(decimal.Zero),
	}
	applyKindClosing(&r, net, netOrig, kind)
	return r
}

// applyKindClosing quy đổi số dư thuần sang cặp cột Nợ/Có theo loại tài khoản.
func applyKindClosing(r *Row, net, netOrig decimal.Decimal, kind int32) {
	switch kind {
	case KindDebit:
		r.ClosingDebitAmount, r.ClosingDebitAmountOriginal = D(net), D(netOrig)
		r.ClosingCreditAmount, r.ClosingCreditAmountOriginal = D(decimal.Zero), D(decimal.Zero)
	case KindCredit:
		r.ClosingDebitAmount, r.ClosingDebitAmountOriginal = D(decimal.Zero), D(decimal.Zero)
		r.ClosingCreditAmount, r.ClosingCreditAmountOriginal = D(net.Neg()), D(netOrig.Neg())
	default:
		r.ClosingDebitAmount = D(decimal.Max(net, decimal.Zero))
		r.ClosingDebitAmountOriginal = D(decimal.Max(netOrig, decimal.Zero))
		r.ClosingCreditAmount = D(decimal.Max(net.Neg(), decimal.Zero))
		r.ClosingCreditAmountOriginal = D(decimal.Max(netOrig.Neg(), decimal.Zero))
	}
}

// =============================================================================
// BUILD SQL
// =============================================================================

// buildDetailQuery sinh câu truy vấn chi tiết trong kỳ.
//
// CHẾ ĐỘ GỘP PHẢI KHỚP VỚI Proc_SO_CHI_TIET_CAC_TAI_KHOAN
// -------------------------------------------------------
// Proc gốc có hai biến thể GROUP BY tuỳ @IsDefaultType. Java truyền
// isDefaultType = false, nên đích cần khớp là biến thể 19 cột:
//
//	GL.ReferenceID, GL.TypeID, GL.PostedDate,
//	CASE WHEN @IsFinancialBook = 1 THEN GL.NoFBook ELSE GL.NoMBook END,
//	GL.Date, GL.reason, GLD.AccountCorresponding,
//	GL.InvoiceNo, GL.invoiceDate, GL.exchangeRate,
//	GLD.AccountingObjectCode, GLD.AccountingObjectName,
//	GL.CustomField1..5, GL.IsUnreasonableCost, GLD.Account
//
// Bản này khớp 18/19 cột. Thiếu GL.Date vì fact_gl_entry_line chưa có cột
// tương ứng (ngày chứng từ, khác posted_date là ngày hạch toán) — cần bổ sung
// ở tầng DWH rồi thêm vào đây.
//
// custom_field_detail1..5 KHÔNG nằm trong GROUP BY của proc gốc → giữ any().
//
// HAI RÀNG BUỘC KỸ THUẬT
// ----------------------
//  1. Mọi cột qualify bằng `f.` — alias SELECT che mất cột gốc cùng tên và gây
//     Code 184 khi cột trần xuất hiện trong WHERE.
//  2. Kiểu trả về phải GIỐNG NHAU giữa hai chế độ vì chỉ có một scanRow. Ép cả
//     hai về non-nullable bằng coalesce, rồi scan vào giá trị.
func buildDetailQuery(
	companyIDs, accounts []string, p QueryParams, typeLedger int, noCol string,
	useStaging bool,
) string {
	sql := queryTemplate

	if p.GroupSameItem == 1 {
		// ── CHẾ ĐỘ GỘP ──────────────────────────────────────────────────────
		// concat() với BẤT KỲ đối số Nullable nào sẽ trả về Nullable(String),
		// khác f.line_bk là String thuần → scanRow lệch kiểu. Phải coalesce
		// từng đối số. posted_date là Date không null nên toString() an toàn.
		sql = strings.ReplaceAll(sql, "{{KEY_ID_EXPR}}",
			"concat("+
				"coalesce(toString(f.reference_id), ''), '|', "+
				"f.account_number, '|', "+
				"toString(f.posted_date), '|', "+
				"coalesce(f.invoice_no, ''), '|', "+
				"toString(coalesce(f.is_unreasonable_cost, 0))"+
				")")

		// Các cột dưới đây NẰM TRONG GROUP BY nên tham chiếu trực tiếp.
		// Trước đây dùng any() cho type_id, reason, custom_field1..5 — đó là
		// nguyên nhân ClickHouse gộp nhiều hơn OLTP.
		sql = strings.ReplaceAll(sql, "{{REFERENCE_ID_EXPR}}", "f.reference_id")
		sql = strings.ReplaceAll(sql, "{{TYPE_ID_EXPR}}", "f.type_id")
		sql = strings.ReplaceAll(sql, "{{POSTED_DATE_EXPR}}", "f.posted_date")
		sql = strings.ReplaceAll(sql, "{{NO_EXPR}}", "f."+noCol)
		sql = strings.ReplaceAll(sql, "{{INVOICE_NO_EXPR}}", "f.invoice_no")
		sql = strings.ReplaceAll(sql, "{{INVOICE_DATE_EXPR}}", "f.invoice_date")
		sql = strings.ReplaceAll(sql, "{{REASON_EXPR}}", "f.reason")

		// journal_memo lấy reason (mức header) thay description (mức dòng) —
		// giữ nguyên quy ước bản Java. reason đã trong GROUP BY nên không cần
		// any().
		sql = strings.ReplaceAll(sql, "{{JOURNAL_MEMO_EXPR}}", "f.reason")

		sql = strings.ReplaceAll(sql, "{{ACCOUNT_NUMBER_EXPR}}", "f.account_number")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_CORRESPONDING_EXPR}}", "f.account_corresponding")

		sql = strings.ReplaceAll(sql, "{{DEBIT_EXPR}}", "sum(coalesce(f.debit_amount, 0))")
		sql = strings.ReplaceAll(sql, "{{CREDIT_EXPR}}", "sum(coalesce(f.credit_amount, 0))")
		sql = strings.ReplaceAll(sql, "{{DEBIT_ORIG_EXPR}}", "sum(coalesce(f.debit_amount_original, 0))")
		sql = strings.ReplaceAll(sql, "{{CREDIT_ORIG_EXPR}}", "sum(coalesce(f.credit_amount_original, 0))")

		// exchange_rate nằm trong GROUP BY nên vẫn Nullable — coalesce để khớp
		// kiểu với chế độ chi tiết.
		sql = strings.ReplaceAll(sql, "{{EXCHANGE_RATE_EXPR}}", "coalesce(f.exchange_rate, 0)")

		// order_priority KHÔNG có trong GROUP BY của proc gốc → any().
		sql = strings.ReplaceAll(sql, "{{ORDER_PRIORITY_EXPR}}", "any(f.order_priority)")

		sql = strings.ReplaceAll(sql, "{{IS_UNREASONABLE_COST_EXPR}}", "f.is_unreasonable_cost")
		sql = strings.ReplaceAll(sql, "{{AO_CODE_EXPR}}", "f.accounting_object_code")
		sql = strings.ReplaceAll(sql, "{{AO_NAME_EXPR}}", "f.accounting_object_name")

		for i := 1; i <= 5; i++ {
			// custom_field NẰM TRONG GROUP BY của proc gốc.
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CF%d}}", i),
				fmt.Sprintf("f.custom_field%d", i))
			// custom_field_detail thì KHÔNG.
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CFD%d}}", i),
				fmt.Sprintf("any(f.custom_field_detail%d)", i))
		}

		sql = strings.ReplaceAll(sql, "{{GROUP_BY_CLAUSE}}",
			"GROUP BY f.reference_id, f.type_id, f.posted_date, f."+noCol+", "+
				"f.reason, f.account_corresponding, f.invoice_no, f.invoice_date, "+
				"f.exchange_rate, f.accounting_object_code, f.accounting_object_name, "+
				"f.custom_field1, f.custom_field2, f.custom_field3, "+
				"f.custom_field4, f.custom_field5, "+
				"f.is_unreasonable_cost, f.account_number")

		// Proc gốc lọc số tiền bằng HAVING, tức SAU khi gộp. Một nhóm gồm +100
		// và -100 sẽ bị loại (tổng bằng 0) — nếu lọc trong WHERE thì cả hai
		// dòng đều khác 0 nên nhóm được giữ lại. Bút toán điều chỉnh hoặc đảo
		// là nơi hay gặp chênh lệch này.
		sql = strings.ReplaceAll(sql, "{{AMOUNT_FILTER}}", "")
		sql = strings.ReplaceAll(sql, "{{HAVING_CLAUSE}}",
			"HAVING sum(coalesce(f.debit_amount, 0))           != 0 "+
				"OR sum(coalesce(f.credit_amount, 0))          != 0 "+
				"OR sum(coalesce(f.debit_amount_original, 0))  != 0 "+
				"OR sum(coalesce(f.credit_amount_original, 0)) != 0")

		// ORDER BY dùng ALIAS, không phải f.* — sau GROUP BY thì cột gốc không
		// tham chiếu trực tiếp được.
		//
		// Thu tu yeu cau: AccountNumber, OrderType, PostedDate, Date, OrderNumber, No.
		// OrderType/OrderNumber/Date la field walker tu sinh (khong co that
		// trong fact table) nen khong the ORDER BY o day; account_number va
		// posted_date da nam trong ORDER BY ngoai (buildDetailQuery), tail chi
		// con "no" la cot that con lai trong chuoi yeu cau.
		sql = strings.ReplaceAll(sql, "{{ORDER_TAIL}}", "no")

	} else {
		// ── CHẾ ĐỘ CHI TIẾT ─────────────────────────────────────────────────
		sql = strings.ReplaceAll(sql, "{{KEY_ID_EXPR}}", "f.line_bk")
		sql = strings.ReplaceAll(sql, "{{REFERENCE_ID_EXPR}}", "f.reference_id")
		sql = strings.ReplaceAll(sql, "{{TYPE_ID_EXPR}}", "f.type_id")
		sql = strings.ReplaceAll(sql, "{{POSTED_DATE_EXPR}}", "f.posted_date")
		sql = strings.ReplaceAll(sql, "{{NO_EXPR}}", "f."+noCol)
		sql = strings.ReplaceAll(sql, "{{INVOICE_NO_EXPR}}", "f.invoice_no")
		sql = strings.ReplaceAll(sql, "{{INVOICE_DATE_EXPR}}", "f.invoice_date")
		sql = strings.ReplaceAll(sql, "{{REASON_EXPR}}", "f.reason")
		sql = strings.ReplaceAll(sql, "{{JOURNAL_MEMO_EXPR}}", "f.description")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_NUMBER_EXPR}}", "f.account_number")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_CORRESPONDING_EXPR}}", "f.account_corresponding")

		// coalesce để khớp kiểu với chế độ gộp — xem ghi chú đầu hàm.
		sql = strings.ReplaceAll(sql, "{{DEBIT_EXPR}}", "coalesce(f.debit_amount, 0)")
		sql = strings.ReplaceAll(sql, "{{CREDIT_EXPR}}", "coalesce(f.credit_amount, 0)")
		sql = strings.ReplaceAll(sql, "{{DEBIT_ORIG_EXPR}}", "coalesce(f.debit_amount_original, 0)")
		sql = strings.ReplaceAll(sql, "{{CREDIT_ORIG_EXPR}}", "coalesce(f.credit_amount_original, 0)")
		sql = strings.ReplaceAll(sql, "{{EXCHANGE_RATE_EXPR}}", "coalesce(f.exchange_rate, 0)")

		sql = strings.ReplaceAll(sql, "{{ORDER_PRIORITY_EXPR}}", "f.order_priority")
		sql = strings.ReplaceAll(sql, "{{IS_UNREASONABLE_COST_EXPR}}", "f.is_unreasonable_cost")
		sql = strings.ReplaceAll(sql, "{{AO_CODE_EXPR}}", "f.accounting_object_code")
		sql = strings.ReplaceAll(sql, "{{AO_NAME_EXPR}}", "f.accounting_object_name")

		for i := 1; i <= 5; i++ {
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CF%d}}", i),
				fmt.Sprintf("f.custom_field%d", i))
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CFD%d}}", i),
				fmt.Sprintf("f.custom_field_detail%d", i))
		}

		// Không gộp thì lọc ngay trong WHERE — mỗi dòng đứng độc lập nên không
		// có chuyện tổng triệt tiêu.
		sql = strings.ReplaceAll(sql, "{{AMOUNT_FILTER}}",
			"AND (coalesce(f.debit_amount, 0)           != 0 "+
				"OR coalesce(f.credit_amount, 0)          != 0 "+
				"OR coalesce(f.debit_amount_original, 0)  != 0 "+
				"OR coalesce(f.credit_amount_original, 0) != 0)")
		sql = strings.ReplaceAll(sql, "{{HAVING_CLAUSE}}", "")

		sql = strings.ReplaceAll(sql, "{{GROUP_BY_CLAUSE}}", "")
		// Xem ghi chu o nhanh GroupSameItem==1 ben tren ve thu tu yeu cau.
		sql = strings.ReplaceAll(sql, "{{ORDER_TAIL}}", "no")
	}

	return applyCommonPlaceholders(sql, companyIDs, accounts, p, typeLedger, useStaging)
}

func buildOpeningQuery(
	companyIDs, accounts []string, p QueryParams, typeLedger int, useStaging bool,
) string {
	return applyCommonPlaceholders(openingTemplate, companyIDs, accounts, p, typeLedger, useStaging)
}

func applyCommonPlaceholders(
	sql string, companyIDs, accounts []string, p QueryParams, typeLedger int,
	useStaging bool,
) string {
	accList := quoteJoin(accounts)

	glSource := factSourceExpr
	if useStaging {
		glSource = stagingSourceExpr
	}
	sql = strings.ReplaceAll(sql, "{{GL_SOURCE}}", glSource)

	sql = strings.ReplaceAll(sql, "{{COMPANY_IDS}}", quoteJoin(companyIDs))
	sql = strings.ReplaceAll(sql, "{{FROM_DATE}}", p.FromDate)
	sql = strings.ReplaceAll(sql, "{{TO_DATE}}", p.ToDate)
	sql = strings.ReplaceAll(sql, "{{TYPE_LEDGER}}", fmt.Sprintf("%d", typeLedger))
	sql = strings.ReplaceAll(sql, "{{ACCOUNT_NUMBERS}}", accList)
	sql = strings.ReplaceAll(sql, "{{ACCOUNT_ORDER}}", accList)
	sql = strings.ReplaceAll(sql, "{{CURRENCY_FILTER}}", currencyFilter(p.CurrencyID))
	sql = strings.ReplaceAll(sql, "{{CLUSTER_FILTER}}", clusterFilter(p.ClusterID))
	// query_opening.sql không có hai placeholder này, ReplaceAll bỏ qua nếu
	// không tìm thấy nên gọi ở đây là an toàn.
	sql = strings.ReplaceAll(sql, "{{AMOUNT_FILTER}}", "")
	sql = strings.ReplaceAll(sql, "{{HAVING_CLAUSE}}", "")
	return sql
}

// =============================================================================
// TRUY VẤN PHỤ TRỢ
// =============================================================================

func (s *Service) resolveCompanyScope(ctx context.Context, p QueryParams) ([]string, error) {
	if p.IsDependent != 1 {
		return p.CompanyIDs, nil
	}
	sql := fmt.Sprintf(`
		SELECT DISTINCT toString(child_org_id)
		FROM eb_dwh.bridge_org_scope
		WHERE root_org_id = '%s' AND child_org_id != root_org_id`, p.PrimaryCompanyID)

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	ids := append([]string{}, p.CompanyIDs...)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return dedupe(ids), rows.Err()
}

func (s *Service) expandAccounts(
	ctx context.Context, companyID string, parents []string,
) ([]string, error) {
	sql := fmt.Sprintf(`
		SELECT DISTINCT b.descendant_account_number
		FROM eb_dwh.bridge_account_hierarchy b
		INNER JOIN eb_dwh.dim_account d
		    ON  d.company_id     = b.company_id
		    AND d.account_number = b.descendant_account_number
		    AND d.is_current     = 1
		    AND d.is_parent_node = 0
		WHERE b.company_id = '%s'
		  AND b.ancestor_account_number IN (%s)`, companyID, quoteJoin(parents))

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Bridge chưa build xong → dùng danh sách gốc (như bản Java).
	if len(out) == 0 {
		return parents, nil
	}
	return out, nil
}

// accountNameColumn chọn cột tên tài khoản trong dim_account theo năm của kỳ
// báo cáo (FromDate).
//
// Chuẩn mực/thông tư mới hiệu lực từ năm 2026 đổi tên một số tài khoản. DWH
// sẽ bổ sung cột account_name_99 để giữ SONG SONG cả tên cũ (account_name_vi,
// vẫn dùng cho dữ liệu trước 2026) lẫn tên mới, không ghi đè.
//
// CẢNH BÁO: cột account_name_99 CHƯA TỒN TẠI trên ClickHouse tại thời điểm
// viết đoạn này (2026-08-23) — báo cáo có FromDate từ năm 2026 trở đi sẽ lỗi
// "column not found" cho đến khi DWH bổ sung cột. Cần phối hợp thời điểm
// deploy với team DWH, không tự ý bật sớm.
func accountNameColumn(fromDate string) string {
	if t, err := time.Parse("2006-01-02", fromDate); err == nil && t.Year() >= 2026 {
		return "account_name_99"
	}
	return "account_name_vi"
}

// resolveAccountInfo lấy CẢ kind lẫn tên trong MỘT truy vấn.
//
// nameCol: xem accountNameColumn — account_name_vi (mặc định) hoặc
// account_name_99 (kỳ báo cáo từ 2026).
func (s *Service) resolveAccountInfo(
	ctx context.Context, companyID string, accounts []string, nameCol string,
) (map[string]int32, map[string]string, error) {
	sql := fmt.Sprintf(`
		SELECT account_number, account_group_kind, %s
		FROM eb_dwh.dim_account
		WHERE company_id = '%s' AND account_number IN (%s) AND is_current = 1`,
		nameCol, companyID, quoteJoin(accounts))

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	kinds := make(map[string]int32, len(accounts))
	names := make(map[string]string, len(accounts))
	for rows.Next() {
		var acc string
		var kind *int32
		var name *string
		if err := rows.Scan(&acc, &kind, &name); err != nil {
			return nil, nil, err
		}
		if kind == nil {
			kinds[acc] = KindBoth
		} else {
			kinds[acc] = *kind
		}
		if name != nil && *name != "" {
			names[acc] = acc + " - " + *name
		} else {
			names[acc] = acc
		}
	}
	return kinds, names, rows.Err()
}

func (s *Service) loadOpeningBalances(
	ctx context.Context, companyIDs, accounts []string, p QueryParams, typeLedger int,
) (map[string]balance, error) {
	rows, err := s.conn.Query(ctx, buildOpeningQuery(companyIDs, accounts, p, typeLedger, s.useStaging))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]balance)
	for rows.Next() {
		var acc string
		var b balance
		if err := rows.Scan(&acc, &b.Debit, &b.Credit, &b.DebitOrig, &b.CreditOrig); err != nil {
			return nil, err
		}
		out[acc] = b
	}
	return out, rows.Err()
}

// =============================================================================
// SCAN
// =============================================================================

// scanRow đọc một dòng kết quả.
//
// QUY TẮC: cột Nullable BẮT BUỘC scan vào con trỏ, cột không Nullable BẮT BUỘC
// scan vào giá trị. Sai chiều nào cũng lỗi lúc chạy — trình biên dịch không
// bắt được, và thông báo chỉ ra đúng một cột nên phải sửa từng cái nếu không
// đối chiếu trước với system.columns.
//
// Đối chiếu DDL thật của eb_dwh.fact_gl_entry_line:
//
//	line_bk                 String            → string
//	reference_id            Nullable(UUID)    → *uuid.UUID
//	type_id                 Nullable(Int32)   → *int32
//	posted_date             Date              → time.Time        (KHÔNG con trỏ)
//	no_fbook / no_mbook     Nullable(String)  → *string
//	invoice_no              Nullable(String)  → *string
//	invoice_date            Nullable(Date)    → *time.Time
//	reason / description    Nullable(String)  → *string
//	account_number          String            → string           (KHÔNG con trỏ)
//	account_corresponding   Nullable(String)  → *string
//	debit_amount ...        đã coalesce       → decimal.Decimal   (KHÔNG con trỏ)
//	exchange_rate           đã coalesce       → decimal.Decimal   (KHÔNG con trỏ)
//	order_priority          Nullable(Int32)   → *int32
//	is_unreasonable_cost    Nullable(UInt8)   → *uint8            ← KHÔNG phải *int32
//	accounting_object_*     Nullable(String)  → *string
//	custom_field*           Nullable(String)  → *string
func scanRow(rows driverRows) (Row, error) {
	var (
		keyID                string
		referenceID          *uuid.UUID
		typeID               *int32
		postedDate           time.Time
		no                   *string
		invoiceNo            *string
		invoiceDate          *time.Time
		reason               *string
		journalMemo          *string
		accountNumber        string
		accountCorresponding *string

		// Đã coalesce trong SQL nên không bao giờ NULL.
		debitAmount      decimal.Decimal
		creditAmount     decimal.Decimal
		debitAmountOrig  decimal.Decimal
		creditAmountOrig decimal.Decimal
		exchangeRate     decimal.Decimal

		orderPriority      *int32
		isUnreasonableCost *uint8

		aoCode, aoName *string
		cf             [5]*string
		cfd            [5]*string
	)

	if err := rows.Scan(
		&keyID, &referenceID, &typeID, &postedDate,
		&no, &invoiceNo, &invoiceDate,
		&reason, &journalMemo, &accountNumber, &accountCorresponding,
		&debitAmount, &creditAmount, &debitAmountOrig, &creditAmountOrig,
		&exchangeRate, &orderPriority, &isUnreasonableCost,
		&aoCode, &aoName,
		&cf[0], &cf[1], &cf[2], &cf[3], &cf[4],
		&cfd[0], &cfd[1], &cfd[2], &cfd[3], &cfd[4],
	); err != nil {
		return Row{}, err
	}

	r := Row{
		KeyID:                keyID,
		ReferenceID:          uuidOrEmpty(referenceID),
		TypeID:               int32OrZero(typeID),
		PostedDate:           postedDate,
		Date:                 postedDate,
		No:                   strOrEmpty(no),
		InvoiceNo:            strOrEmpty(invoiceNo),
		Reason:               strOrEmpty(reason),
		JournalMemo:          strOrEmpty(journalMemo),
		AccountNumber:        accountNumber,
		AccountCorresponding: strOrEmpty(accountCorresponding),

		DebitAmount:          D(debitAmount),
		CreditAmount:         D(creditAmount),
		DebitAmountOriginal:  D(debitAmountOrig),
		CreditAmountOriginal: D(creditAmountOrig),
		ExchangeRate:         D(exchangeRate),

		OrderPriority:        int32OrZero(orderPriority),
		AccountingObjectCode: strOrEmpty(aoCode),
		AccountingObjectName: strOrEmpty(aoName),
	}

	if invoiceDate != nil {
		r.InvoiceDate = *invoiceDate
	}
	if isUnreasonableCost != nil {
		r.IsUnreasonableCost = fmt.Sprint(*isUnreasonableCost)
	}
	for i := 0; i < 5; i++ {
		r.CustomField[i] = strOrEmpty(cf[i])
		r.CustomFieldDetail[i] = strOrEmpty(cfd[i])
	}
	return r, nil
}

// =============================================================================
// VALIDATE + HELPERS
// =============================================================================

func validateParams(p QueryParams) error {
	if len(p.CompanyIDs) == 0 {
		return fmt.Errorf("companyIds khong duoc rong")
	}
	if p.PrimaryCompanyID == "" {
		return fmt.Errorf("primaryCompanyId khong duoc rong")
	}
	if len(p.AccountNumbers) == 0 {
		return fmt.Errorf("accountNumbers khong duoc rong")
	}

	ids := append([]string{}, p.CompanyIDs...)
	ids = append(ids, p.PrimaryCompanyID)
	for _, id := range ids {
		if !uuidPattern.MatchString(id) {
			return fmt.Errorf("ID khong dung dinh dang UUID: %s", id)
		}
	}

	// Ngày PHẢI parse được, không chỉ kiểm rỗng. Chúng đi thẳng vào chuỗi SQL
	// nên nếu chỉ kiểm rỗng thì {"toDate": "2025-12-31' OR 1=1 --"} sẽ chèn
	// được câu lệnh tuỳ ý.
	from, err := time.Parse("2006-01-02", p.FromDate)
	if err != nil {
		return fmt.Errorf("fromDate phai dang YYYY-MM-DD: %q", p.FromDate)
	}
	to, err := time.Parse("2006-01-02", p.ToDate)
	if err != nil {
		return fmt.Errorf("toDate phai dang YYYY-MM-DD: %q", p.ToDate)
	}
	if to.Before(from) {
		return fmt.Errorf("toDate som hon fromDate")
	}

	if p.CurrencyID != "" && p.CurrencyID != "TH" && !currencyPattern.MatchString(p.CurrencyID) {
		return fmt.Errorf("currencyId khong hop le: %q", p.CurrencyID)
	}
	if p.ClusterID != "" && !clusterPattern.MatchString(p.ClusterID) {
		return fmt.Errorf("clusterId khong hop le: %q", p.ClusterID)
	}
	if p.IsFinancialBook != 0 && p.IsFinancialBook != 1 {
		return fmt.Errorf("isFinancialBook phai la 0 hoac 1")
	}
	if p.GroupSameItem != 0 && p.GroupSameItem != 1 {
		return fmt.Errorf("groupSameItem phai la 0 hoac 1")
	}
	return nil
}

// currencyFilter và clusterFilter dùng chung cho query.sql lẫn
// query_opening.sql, nên bí danh `f` phải giống nhau ở cả hai file.
func currencyFilter(currencyID string) string {
	if currencyID == "" || currencyID == "TH" {
		return ""
	}
	// currencyID đã qua currencyPattern trong validateParams.
	return fmt.Sprintf("AND f.currency_code = '%s'", currencyID)
}

func clusterFilter(clusterID string) string {
	if clusterID == "" {
		return ""
	}
	return fmt.Sprintf("AND f.cluster_id = '%s'", clusterID)
}

func cleanAccounts(in []string) []string {
	out := make([]string, 0, len(in))
	seen := make(map[string]bool, len(in))
	for _, a := range in {
		a = strings.TrimSpace(a)
		if a == "" || a == "Tất cả" || seen[a] {
			continue
		}
		seen[a] = true
		out = append(out, a)
	}
	return out
}

func dedupe(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out
}

func quoteJoin(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	q := make([]string, len(ids))
	for i, id := range ids {
		q[i] = fmt.Sprintf("'%s'", id)
	}
	return strings.Join(q, ", ")
}

// sqlServerUUIDSwap đảo byte 3 nhóm đầu của UUID (time_low 4B, time_mid 2B,
// time_hi_and_version 2B) — bù cho cách SQL Server UNIQUEIDENTIFIER lưu byte
// khác chuẩn RFC4122. clock_seq + node (8 byte cuối) giữ nguyên.
//
// company_id đã được PHÍA JAVA swap trước khi gửi vào gogateway (xem
// SqlServerUuidSwap.swap/Common.revertUUID bên Java, và comment "đã swap
// UUID phía Java" ở QueryParams.CompanyIDs) nên WHERE khớp đúng cột UUID
// trong ClickHouse. Nhưng reference_id ĐỌC RA từ ClickHouse chưa được ai
// swap lại — nếu trả thẳng ra, UUID bị lệch byte so với ReferenceID thật
// trong OLTP, khiến reflink ở frontend trỏ sai chứng từ. Áp swap này (hàm tự
// nghịch đảo, gọi 2 lần ra lại UUID gốc) ngay tại nguồn để mọi consumer nhận
// đúng UUID mà không cần biết đặc thù SQL Server.
func sqlServerUUIDSwap(u uuid.UUID) uuid.UUID {
	var out uuid.UUID
	out[0], out[1], out[2], out[3] = u[3], u[2], u[1], u[0]
	out[4], out[5] = u[5], u[4]
	out[6], out[7] = u[7], u[6]
	copy(out[8:], u[8:])
	return out
}

func uuidOrEmpty(u *uuid.UUID) string {
	if u == nil {
		return ""
	}
	return sqlServerUUIDSwap(*u).String()
}

func strOrEmpty(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func int32OrZero(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
