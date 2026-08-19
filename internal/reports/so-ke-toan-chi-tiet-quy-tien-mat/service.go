// Package so_ke_toan_chi_tiet_quy_tien_mat — Sổ kế toán chi tiết quỹ tiền mặt.
//
// Nguồn tham chiếu: Proc_SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT (1.696 dòng).
//
// BA KHÁC BIỆT SO VỚI SỔ CHI TIẾT TÀI KHOẢN
// -----------------------------------------
//
//  1. SoTon luỹ kế — cộng dồn theo từng tài khoản. Proc gốc dùng thủ thuật
//     UPDATE ... SET @var của SQL Server, vốn KHÔNG được đảm bảo về thứ tự và
//     chỉ chạy đúng nhờ clustered index. Ở đây thứ tự do ORDER BY bảo đảm.
//
//  2. Tách Nợ/Có thành phiếu thu / phiếu chi — làm ở SQL bằng UNION ALL, KHÔNG
//     làm ở Go. Xem ghi chú trong query.sql: CAType nằm trong ORDER BY trước
//     số hiệu chứng từ, nên trong cùng một ngày mọi phiếu thu phải chạy trước
//     mọi phiếu chi. Tách ở Go sẽ phát hai dòng liền nhau và sai thứ tự.
//
//  3. typeShowCurrency chọn giữa quy đổi và nguyên tệ cho cột hiển thị, nhưng
//     cột QD luôn giữ bản quy đổi.
//
// Proc gốc lặp lại gần như y hệt bốn lần cho tổ hợp (loại tiền × gộp). Ở đây
// gộp thành hai tham số.
//
// ── SAO CHÉP CÓ Ý THỨC MỘT BẤT NHẤT CỦA PROC ────────────────────────────────
// Tồn đầu kỳ dùng nguyên tệ cho SoTon bất kể typeShowCurrency, trong khi dòng
// chi tiết thì đổi theo tham số. Xem ghi chú đầy đủ ở query_opening.sql. Giữ
// nguyên để khớp OLTP; dev sau quyết định có sửa không.
package so_ke_toan_chi_tiet_quy_tien_mat

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

var (
	uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

	// Số hiệu tài khoản CÓ THỂ chứa khoảng trắng và gạch ngang: "154 - TT" là
	// một tài khoản THẬT. Dùng danh sách CHẶN thay vì CHO PHÉP.
	accountPattern = regexp.MustCompile(`^[^'"\\\x00-\x1f]{1,50}$`)

	currencyPattern = regexp.MustCompile(`^[0-9A-Za-z]{2,10}$`)
	clusterPattern  = regexp.MustCompile(`^[0-9A-Za-z._-]{1,50}$`)
)

type Service struct {
	conn clickhouse.Conn
}

func NewService(conn clickhouse.Conn) *Service { return &Service{conn: conn} }

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

	// 0 = sổ tài chính (no_fbook), 1 = sổ quản trị (no_mbook).
	// CHÚ Ý: proc này nhận thẳng @typeLedger nên 0 mới là sổ tài chính —
	// ngược chiều với isFinancialBook của module so_chi_tiet_tai_khoan.
	TypeLedger int

	// 0 = hiển thị số quy đổi (debit_amount)
	// 1 = hiển thị số nguyên tệ (debit_amount_original)
	TypeShowCurrency int

	GroupTheSameItem int
	IsDependent      int
	ClusterID        string
}

// =============================================================================
// ENTRYPOINT
// =============================================================================

// GetSoQuy gom toàn bộ kết quả vào slice. Chỉ dùng cho báo cáo nhỏ.
func (s *Service) GetSoQuy(ctx context.Context, p QueryParams) ([]Row, int64, error) {
	var result []Row
	elapsedMs, err := s.StreamSoQuy(ctx, p, func(row Row) error {
		result = append(result, row)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return result, elapsedMs, nil
}

// StreamSoQuy gọi onRow cho TỪNG dòng ngay khi dựng xong.
func (s *Service) StreamSoQuy(
	ctx context.Context, p QueryParams, onRow func(Row) error,
) (int64, error) {
	if err := validateParams(p); err != nil {
		return 0, err
	}
	start := time.Now()

	noCol := "no_fbook"
	if p.TypeLedger == 1 {
		noCol = "no_mbook"
	}

	companyIDs, err := s.resolveCompanyScope(ctx, p)
	if err != nil {
		return 0, fmt.Errorf("resolve company scope: %w", err)
	}

	accounts := cleanAccounts(p.AccountNumbers)
	if len(accounts) == 0 {
		return 0, fmt.Errorf("khong con tai khoan nao sau khi loc")
	}
	for _, a := range accounts {
		if !accountPattern.MatchString(a) {
			return 0, fmt.Errorf("so hieu tai khoan khong hop le: %q", a)
		}
	}

	// LUON mo rong xuong tai khoan con — khong co co bat/tat.
	//
	// Ban JDBC cu lam viec nay o Java (DynamicReportQuyServiceImpl goi
	// AccountListService.getListChildAccount) roi moi truyen danh sach da phang
	// vao proc. Chuyen sang day de bo round-trip SQL Server cuoi cung con sot
	// trong luong nay — DWH da co bridge_account_hierarchy.
	//
	// Vi danh sach sau khi mo rong chua CA cha lan con, query_opening.sql PHAI
	// khop chinh xac ma. Neu no cung cong don theo cay thi ton dau ky cua tai
	// khoan cha se dem trung phan da co o cac con.
	accounts, err = s.expandAccounts(ctx, p.PrimaryCompanyID, accounts)
	if err != nil {
		return 0, fmt.Errorf("expand accounts: %w", err)
	}

	sddkMap, err := s.loadOpeningBalances(ctx, companyIDs, accounts, p)
	if err != nil {
		return 0, fmt.Errorf("load opening balances: %w", err)
	}

	w := &walker{
		onRow:            onRow,
		sddkMap:          sddkMap,
		accounts:         accounts,
		seen:             make(map[string]bool, len(accounts)),
		typeShowCurrency: p.TypeShowCurrency,
	}

	sql := buildDetailQuery(companyIDs, accounts, p, noCol)
	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return 0, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
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
// WALKER — máy trạng thái một lượt, có số dư luỹ kế
// =============================================================================

type walker struct {
	onRow            func(Row) error
	sddkMap          map[string]balance
	accounts         []string
	seen             map[string]bool
	typeShowCurrency int

	current string

	// running / runningQD là SoTon luỹ kế.
	//
	// Proc gốc (dòng 1324):
	//     SET @ClosingQuantity = CASE
	//         WHEN RefID IS NULL THEN SoTon                    -- dòng tồn đầu kỳ
	//         WHEN @AccountNumber <> Account THEN PhatSinhNo - PhatSinhCo
	//         ELSE @ClosingQuantity + PhatSinhNo - PhatSinhCo
	//     END
	//
	// Nhánh giữa thực chất là CODE CHẾT: dòng tồn đầu kỳ (PositionOrder = 0)
	// luôn đứng đầu mỗi tài khoản và đã đặt @AccountNumber, nên điều kiện đổi
	// tài khoản không bao giờ đúng ở dòng chi tiết. openAccount() ở đây luôn
	// phát dòng tồn đầu kỳ nên tương đương.
	running   decimal.Decimal
	runningQD decimal.Decimal

	// Cộng dồn phát sinh của tài khoản hiện tại, cho dòng Cộng nhóm.
	tc balance
}

// push xử lý một dòng đã tách sẵn thu/chi từ SQL.
//
// Không tách ở đây — xem ghi chú đầu package và trong query.sql.
func (w *walker) push(d rawRow) error {
	if d.Account != w.current {
		if w.current != "" {
			if err := w.closeAccount(); err != nil {
				return err
			}
		}
		if err := w.openAccount(d.Account); err != nil {
			return err
		}
	}

	// Số dư luỹ kế: cộng thu, trừ chi.
	w.running = w.running.Add(d.PhatSinhNo).Sub(d.PhatSinhCo)
	w.runningQD = w.runningQD.Add(d.PhatSinhNoQD).Sub(d.PhatSinhCoQD)

	w.tc.add(balance{
		Debit:    d.PhatSinhNo,
		Credit:   d.PhatSinhCo,
		DebitQD:  d.PhatSinhNoQD,
		CreditQD: d.PhatSinhCoQD,
	})

	return w.onRow(Row{
		PositionOrder:        PositionDetail,
		CAType:               d.CAType,
		OrderPriority:        d.OrderPriority,
		RefID:                d.RefID,
		TypeID:               d.TypeID,
		Date:                 d.VoucherDate,
		PostedDate:           d.PostedDate,
		ReceiptRefNo:         d.ReceiptRefNo,
		PaymentRefNo:         d.PaymentRefNo,
		JournalMemo:          d.JournalMemo,
		Reason:               d.Reason,
		Account:              d.Account,
		AccountCorresponding: d.AccountCorresponding,
		PhatSinhNo:           D(d.PhatSinhNo),
		PhatSinhCo:           D(d.PhatSinhCo),
		PhatSinhNoQD:         D(d.PhatSinhNoQD),
		PhatSinhCoQD:         D(d.PhatSinhCoQD),
		SoTon:                D(w.running),
		SoTonQD:              D(w.runningQD),
		ExchangeRate:         D(d.ExchangeRate),
		AccountingObjectCode: d.AccountingObjectCode,
		AccountingObjectName: d.AccountingObjectName,
		CustomField:          d.CustomField,
		CustomFieldDetail:    d.CustomFieldDetail,
	})
}

// openAccount phát dòng Tồn đầu kỳ và đặt lại bộ đếm luỹ kế.
func (w *walker) openAccount(acc string) error {
	w.current = acc
	w.seen[acc] = true
	w.tc = balance{}

	b := w.sddkMap[acc] // zero value nếu không có

	// SoTon dùng nguyên tệ, SoTonQD dùng quy đổi — BẤT KỂ typeShowCurrency.
	// Đây là bất nhất của proc gốc, sao chép có ý thức để khớp OLTP.
	// Xem ghi chú đầy đủ ở query_opening.sql.
	w.running = b.Debit.Sub(b.Credit)
	w.runningQD = b.DebitQD.Sub(b.CreditQD)

	// Proc gốc LUÔN phát dòng tồn đầu kỳ, kể cả bằng 0 — khác sổ chi tiết tài
	// khoản vốn bỏ qua khi bằng 0. Cột SoTon cần một mốc bắt đầu.
	return w.onRow(Row{
		PositionOrder: PositionOpening,
		Account:       acc,
		JournalMemo:   "Số tồn đầu kỳ",
		Reason:        "Số tồn đầu kỳ",
		PhatSinhNo:    D(decimal.Zero),
		PhatSinhCo:    D(decimal.Zero),
		PhatSinhNoQD:  D(decimal.Zero),
		PhatSinhCoQD:  D(decimal.Zero),
		SoTon:         D(w.running),
		SoTonQD:       D(w.runningQD),
		ExchangeRate:  D(decimal.Zero),
	})
}

// closeAccount phát dòng Cộng nhóm.
//
// SoTon của dòng này là số dư CUỐI CÙNG của tài khoản, không phải tổng phát
// sinh — proc gốc lấy nó từ dòng có RowNum lớn nhất (dòng 1367).
func (w *walker) closeAccount() error {
	return w.onRow(Row{
		PositionOrder: PositionGroup,
		Account:       w.current,
		JournalMemo:   "Cộng nhóm",
		PhatSinhNo:    D(w.tc.Debit),
		PhatSinhCo:    D(w.tc.Credit),
		PhatSinhNoQD:  D(w.tc.DebitQD),
		PhatSinhCoQD:  D(w.tc.CreditQD),
		SoTon:         D(w.running),
		SoTonQD:       D(w.runningQD),
		ExchangeRate:  D(decimal.Zero),
	})
}

// finish đóng tài khoản đang mở, rồi phát các tài khoản chỉ có tồn đầu kỳ.
func (w *walker) finish() error {
	if w.current != "" {
		if err := w.closeAccount(); err != nil {
			return err
		}
	}

	// Tài khoản có số dư nhưng không phát sinh trong kỳ không xuất hiện trong
	// luồng SQL — phải quét bù.
	for _, acc := range w.accounts {
		if w.seen[acc] {
			continue
		}
		if err := w.openAccount(acc); err != nil {
			return err
		}
		if err := w.closeAccount(); err != nil {
			return err
		}
	}
	return nil
}

// =============================================================================
// BUILD SQL
// =============================================================================

func buildDetailQuery(
	companyIDs, accounts []string, p QueryParams, noCol string,
) string {
	sql := queryTemplate

	// typeShowCurrency chọn cột hiển thị. Cột QD luôn là bản quy đổi.
	debitShow, creditShow := "f.debit_amount", "f.credit_amount"
	if p.TypeShowCurrency == 1 {
		debitShow, creditShow = "f.debit_amount_original", "f.credit_amount_original"
	}

	if p.GroupTheSameItem == 1 {
		// ── CHẾ ĐỘ GỘP ──────────────────────────────────────────────────────
		// Proc gốc chỉ GROUP BY BỐN cột và bỏ hẳn mọi cột chiều (dòng 371):
		//     group by GL.ReferenceID, Account, AccountCorresponding, AccountingObjectID
		// Khác hẳn Proc_SO_CHI_TIET_CAC_TAI_KHOAN vốn gộp theo 19 cột.
		sql = strings.ReplaceAll(sql, "{{REF_ID_EXPR}}", "f.reference_id")
		sql = strings.ReplaceAll(sql, "{{TYPE_ID_EXPR}}", "any(f.type_id)")
		sql = strings.ReplaceAll(sql, "{{DATE_EXPR}}", "any(f.posted_date)")
		sql = strings.ReplaceAll(sql, "{{POSTED_DATE_EXPR}}", "any(f.posted_date)")
		sql = strings.ReplaceAll(sql, "{{NO_EXPR}}", "any(f."+noCol+")")
		sql = strings.ReplaceAll(sql, "{{REASON_EXPR}}", "any(f.reason)")
		sql = strings.ReplaceAll(sql, "{{JOURNAL_MEMO_EXPR}}", "any(f.reason)")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_EXPR}}", "f.account_number")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_CORRESPONDING_EXPR}}", "f.account_corresponding")

		sql = strings.ReplaceAll(sql, "{{DEBIT_EXPR}}", "sum(coalesce("+debitShow+", 0))")
		sql = strings.ReplaceAll(sql, "{{CREDIT_EXPR}}", "sum(coalesce("+creditShow+", 0))")
		sql = strings.ReplaceAll(sql, "{{DEBIT_QD_EXPR}}", "sum(coalesce(f.debit_amount, 0))")
		sql = strings.ReplaceAll(sql, "{{CREDIT_QD_EXPR}}", "sum(coalesce(f.credit_amount, 0))")

		sql = strings.ReplaceAll(sql, "{{EXCHANGE_RATE_EXPR}}", "any(coalesce(f.exchange_rate, 0))")
		sql = strings.ReplaceAll(sql, "{{ORDER_PRIORITY_EXPR}}", "any(f.order_priority)")
		sql = strings.ReplaceAll(sql, "{{AO_CODE_EXPR}}", "any(f.accounting_object_code)")
		sql = strings.ReplaceAll(sql, "{{AO_NAME_EXPR}}", "any(f.accounting_object_name)")

		for i := 1; i <= 5; i++ {
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CF%d}}", i),
				fmt.Sprintf("any(f.custom_field%d)", i))
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CFD%d}}", i),
				fmt.Sprintf("any(f.custom_field_detail%d)", i))
		}

		// Proc gộp theo AccountingObjectID; fact chỉ có code/name nên dùng code
		// làm khoá thay thế. Hai đối tượng trùng mã sẽ bị gộp — hiếm, nhưng
		// đáng biết.
		sql = strings.ReplaceAll(sql, "{{GROUP_BY_CLAUSE}}",
			"GROUP BY f.reference_id, f.account_number, f.account_corresponding, "+
				"f.accounting_object_code")

	} else {
		// ── CHẾ ĐỘ CHI TIẾT ─────────────────────────────────────────────────
		sql = strings.ReplaceAll(sql, "{{REF_ID_EXPR}}", "f.reference_id")
		sql = strings.ReplaceAll(sql, "{{TYPE_ID_EXPR}}", "f.type_id")
		// GL.Date chưa có trong fact — tạm dùng posted_date.
		// Xem PENDING_fact_rebuild.md §6: voucher_date là cột cần bổ sung.
		sql = strings.ReplaceAll(sql, "{{DATE_EXPR}}", "f.posted_date")
		sql = strings.ReplaceAll(sql, "{{POSTED_DATE_EXPR}}", "f.posted_date")
		sql = strings.ReplaceAll(sql, "{{NO_EXPR}}", "f."+noCol)
		sql = strings.ReplaceAll(sql, "{{REASON_EXPR}}", "f.reason")
		sql = strings.ReplaceAll(sql, "{{JOURNAL_MEMO_EXPR}}", "f.reason")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_EXPR}}", "f.account_number")
		sql = strings.ReplaceAll(sql, "{{ACCOUNT_CORRESPONDING_EXPR}}", "f.account_corresponding")

		sql = strings.ReplaceAll(sql, "{{DEBIT_EXPR}}", "coalesce("+debitShow+", 0)")
		sql = strings.ReplaceAll(sql, "{{CREDIT_EXPR}}", "coalesce("+creditShow+", 0)")
		sql = strings.ReplaceAll(sql, "{{DEBIT_QD_EXPR}}", "coalesce(f.debit_amount, 0)")
		sql = strings.ReplaceAll(sql, "{{CREDIT_QD_EXPR}}", "coalesce(f.credit_amount, 0)")

		sql = strings.ReplaceAll(sql, "{{EXCHANGE_RATE_EXPR}}", "coalesce(f.exchange_rate, 0)")
		sql = strings.ReplaceAll(sql, "{{ORDER_PRIORITY_EXPR}}", "f.order_priority")
		sql = strings.ReplaceAll(sql, "{{AO_CODE_EXPR}}", "f.accounting_object_code")
		sql = strings.ReplaceAll(sql, "{{AO_NAME_EXPR}}", "f.accounting_object_name")

		for i := 1; i <= 5; i++ {
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CF%d}}", i),
				fmt.Sprintf("f.custom_field%d", i))
			sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CFD%d}}", i),
				fmt.Sprintf("f.custom_field_detail%d", i))
		}

		sql = strings.ReplaceAll(sql, "{{GROUP_BY_CLAUSE}}", "")
	}

	return applyCommonPlaceholders(sql, companyIDs, accounts, p)
}

func buildOpeningQuery(companyIDs, accounts []string, p QueryParams) string {
	return applyCommonPlaceholders(openingTemplate, companyIDs, accounts, p)
}

func applyCommonPlaceholders(
	sql string, companyIDs, accounts []string, p QueryParams,
) string {
	accList := quoteJoin(accounts)

	sql = strings.ReplaceAll(sql, "{{COMPANY_IDS}}", quoteJoin(companyIDs))
	sql = strings.ReplaceAll(sql, "{{FROM_DATE}}", p.FromDate)
	sql = strings.ReplaceAll(sql, "{{TO_DATE}}", p.ToDate)
	sql = strings.ReplaceAll(sql, "{{TYPE_LEDGER}}", fmt.Sprintf("%d", p.TypeLedger))
	sql = strings.ReplaceAll(sql, "{{ACCOUNT_NUMBERS}}", accList)
	sql = strings.ReplaceAll(sql, "{{ACCOUNT_ORDER}}", accList)
	sql = strings.ReplaceAll(sql, "{{CURRENCY_FILTER}}", currencyFilter(p.CurrencyID))
	sql = strings.ReplaceAll(sql, "{{CLUSTER_FILTER}}", clusterFilter(p.ClusterID))
	// query_opening.sql không có placeholder này; ReplaceAll bỏ qua nếu không
	// tìm thấy nên gọi ở đây là an toàn.
	sql = strings.ReplaceAll(sql, "{{GROUP_BY_CLAUSE}}", "")
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

// expandAccounts mo rong danh sach xuong toan bo tai khoan LA thuoc cay cua
// cac tai khoan duoc chon.
//
// Nguoi dung chon "111" thi bao cao phai hien ca "1111", "1112", "1113".
// bridge_account_hierarchy da phang hoa san quan he to tien/hau due nen khong
// can CTE de quy nhu proc goc.
//
// Bridge chua build xong thi dung chinh danh sach goc — giu dung hanh vi ban
// Java cu, tha hien thieu tai khoan con con hon tra ve rong.
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
	if len(out) == 0 {
		return parents, nil
	}
	return cleanAccounts(out), nil
}

// loadOpeningBalances — cột nguyên tệ vào Debit/Credit, quy đổi vào
// DebitQD/CreditQD. KHÔNG đổi theo typeShowCurrency: xem ghi chú ở
// query_opening.sql.
func (s *Service) loadOpeningBalances(
	ctx context.Context, companyIDs, accounts []string, p QueryParams,
) (map[string]balance, error) {
	rows, err := s.conn.Query(ctx, buildOpeningQuery(companyIDs, accounts, p))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]balance)
	for rows.Next() {
		var acc string
		var dOC, cOC, dQD, cQD decimal.Decimal
		if err := rows.Scan(&acc, &dOC, &cOC, &dQD, &cQD); err != nil {
			return nil, err
		}
		out[acc] = balance{Debit: dOC, Credit: cOC, DebitQD: dQD, CreditQD: cQD}
	}
	return out, rows.Err()
}

// =============================================================================
// SCAN
// =============================================================================

// rawRow là dòng đọc từ SQL — đã tách sẵn thu/chi bằng UNION ALL.
type rawRow struct {
	RefID                string
	TypeID               int32
	VoucherDate          time.Time
	PostedDate           time.Time
	ReceiptRefNo         string
	PaymentRefNo         string
	CAType               int32
	Reason               string
	JournalMemo          string
	Account              string
	AccountCorresponding string

	PhatSinhNo   decimal.Decimal
	PhatSinhCo   decimal.Decimal
	PhatSinhNoQD decimal.Decimal
	PhatSinhCoQD decimal.Decimal

	ExchangeRate  decimal.Decimal
	OrderPriority int32

	AccountingObjectCode string
	AccountingObjectName string
	CustomField          [5]string
	CustomFieldDetail    [5]string
}

// scanRow đọc một dòng kết quả.
//
// QUY TẮC: cột Nullable BẮT BUỘC scan vào con trỏ, cột không Nullable BẮT BUỘC
// scan vào giá trị. Sai chiều nào cũng lỗi lúc chạy.
//
// Các cột tiền đã coalesce trong SQL nên không bao giờ NULL → scan vào giá trị.
// receipt_ref_no / payment_ref_no dùng ” thay NULL trong UNION ALL nên cũng là
// giá trị.
func scanRow(rows driverRows) (rawRow, error) {
	var (
		refID                *uuid.UUID
		typeID               *int32
		voucherDate          time.Time
		postedDate           time.Time
		receiptRefNo         string
		paymentRefNo         string
		caType               int32
		reason               *string
		journalMemo          *string
		account              string
		accountCorresponding *string

		phatSinhNo   decimal.Decimal
		phatSinhCo   decimal.Decimal
		phatSinhNoQD decimal.Decimal
		phatSinhCoQD decimal.Decimal
		exchangeRate decimal.Decimal

		orderPriority  *int32
		aoCode, aoName *string
		cf             [5]*string
		cfd            [5]*string
	)

	if err := rows.Scan(
		&refID, &typeID, &voucherDate, &postedDate,
		&receiptRefNo, &paymentRefNo, &caType,
		&reason, &journalMemo, &account, &accountCorresponding,
		&phatSinhNo, &phatSinhCo, &phatSinhNoQD, &phatSinhCoQD,
		&exchangeRate, &orderPriority,
		&aoCode, &aoName,
		&cf[0], &cf[1], &cf[2], &cf[3], &cf[4],
		&cfd[0], &cfd[1], &cfd[2], &cfd[3], &cfd[4],
	); err != nil {
		return rawRow{}, err
	}

	r := rawRow{
		RefID:                uuidOrEmpty(refID),
		TypeID:               int32OrZero(typeID),
		VoucherDate:          voucherDate,
		PostedDate:           postedDate,
		ReceiptRefNo:         receiptRefNo,
		PaymentRefNo:         paymentRefNo,
		CAType:               caType,
		Reason:               strOrEmpty(reason),
		JournalMemo:          strOrEmpty(journalMemo),
		Account:              account,
		AccountCorresponding: strOrEmpty(accountCorresponding),
		PhatSinhNo:           phatSinhNo,
		PhatSinhCo:           phatSinhCo,
		PhatSinhNoQD:         phatSinhNoQD,
		PhatSinhCoQD:         phatSinhCoQD,
		ExchangeRate:         exchangeRate,
		OrderPriority:        int32OrZero(orderPriority),
		AccountingObjectCode: strOrEmpty(aoCode),
		AccountingObjectName: strOrEmpty(aoName),
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

	// Ngày PHẢI parse được, không chỉ kiểm rỗng — chúng đi thẳng vào chuỗi SQL.
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
	if p.TypeLedger != 0 && p.TypeLedger != 1 {
		return fmt.Errorf("typeLedger phai la 0 (tai chinh) hoac 1 (quan tri)")
	}
	if p.TypeShowCurrency != 0 && p.TypeShowCurrency != 1 {
		return fmt.Errorf("typeShowCurrency phai la 0 (quy doi) hoac 1 (nguyen te)")
	}
	if p.GroupTheSameItem != 0 && p.GroupTheSameItem != 1 {
		return fmt.Errorf("groupTheSameItem phai la 0 hoac 1")
	}
	return nil
}

func currencyFilter(currencyID string) string {
	if currencyID == "" || currencyID == "TH" {
		return ""
	}
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

func uuidOrEmpty(u *uuid.UUID) string {
	if u == nil {
		return ""
	}
	return u.String()
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
