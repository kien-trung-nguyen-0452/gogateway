// Package so_chi_tiet_cong_no_phai_thu — sổ chi tiết công nợ phải thu.
//
// Nguồn tham chiếu: Proc_Chi_Tiet_Cong_No_Phai_Thu (bản không phân trang; Java
// getChiTietCongNoPhaiThu). Bản _Dong (phân trang, CheckOpeningBalanceMethod) chưa làm.
//
// Cấu trúc proc:
//
//   - #tblAccountNumber: tài khoản công nợ, khớp bút toán theo TIỀN TỐ
//     (GLD.Account LIKE '131%') nên 1311, 1312... gộp vào cùng 131.
//   - Số dư đầu kỳ (OrderType 0) = SummaryOpeningBalance các kỳ trước + GL từ đầu
//     tháng đến trước FromDate, mỗi (đối tượng, tài khoản) một dòng; bỏ nếu bằng 0.
//   - Phát sinh (OrderType 1) trong [FromDate, ToDate]; @IsSimilarSum = 1 thì cộng gộp
//     (query_detail_grouped.sql) còn 0 thì mỗi dòng GL một dòng sổ
//     (query_detail_ungrouped.sql).
//   - "Tính số tồn": số dư cuối cộng dồn theo từng (đối tượng, tài khoản) — walker.
//   - @GetAccountHasData = 1: chỉ giữ đối tượng có phát sinh trong kỳ.
//
// Bộ lọc đối tượng (resolveObjectScope): @ParamCheckAll = 1 → mọi đối tượng TRỪ danh
// sách @ListID, kèm lọc mã/tên/địa chỉ/nhóm; = 0 → chỉ đối tượng trong @ListID (các
// bộ lọc mã/tên/địa chỉ/nhóm KHÔNG áp dụng, do thứ tự ưu tiên AND/OR trong proc).
// Luôn chỉ lấy đối tượng IsActive = 1 và ObjectType IN (1, 2).
//
// KHÁC BIỆT SO VỚI PROC (đã biết, chưa xử lý)
//
//  1. Số dư đầu kỳ cộng thẳng từ fact (mọi bút toán trước FromDate) thay cho
//     SummaryOpeningBalance — xem query_opening_branch.sql.
//  2. Bắt buộc có accountNumber. Proc cho phép bỏ trống để lấy mọi tài khoản có
//     AccountList.DetailType LIKE '%1%'; dim_account chưa có cột DetailType.
//  3. Chỉ một tài khoản mỗi lần gọi nên khoá cộng dồn chỉ là đối tượng; proc cộng dồn
//     theo (đối tượng, tài khoản).
//  4. Thứ tự dòng cùng (đối tượng, ngày hạch toán): proc lúc tính số tồn dùng
//     (ngày chứng từ, loại, số CT, ...) còn lúc xuất lại sắp theo (số CT, ...). Ở đây
//     xuất đúng thứ tự cộng dồn để số dư từng dòng luôn khớp thứ tự hiển thị.
//  5. Ký tự '%' '_' trong bộ lọc mã/tên/địa chỉ là ký tự thường, không phải wildcard
//     LIKE; so khớp không phân biệt hoa thường như collation CI_AS.
//  6. Sắp theo mã đối tượng không phân biệt hoa thường theo bytes UTF-8, không theo
//     collation tiếng Việt của SQL Server.
//
// FALLBACK STAGING — giống so-chi-tiet-cac-tai-khoan
// --------------------------------------------------
// useStaging bật đọc từ eb_staging.stg_general_ledger(_detail) thay vì
// eb_dwh.fact_gl_entry_line khi pipeline fact bị lỗi. Xem staging_source.sql và
// config.SoChiTietCongNoPhaiThuUseStaging.
package so_chi_tiet_cong_no_phai_thu

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

//go:embed query.sql
var queryTemplate string

//go:embed query_opening_branch.sql
var openingBranchTemplate string

//go:embed query_detail_ungrouped.sql
var detailUngroupedTemplate string

//go:embed query_detail_grouped.sql
var detailGroupedTemplate string

//go:embed staging_source.sql
var stagingSourceExpr string

// factSourceExpr là giá trị {{GL_SOURCE_*}} mặc định — đọc thẳng fact table.
const factSourceExpr = "eb_dwh.fact_gl_entry_line AS f FINAL"

var (
	uuidPattern     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	clusterPattern  = regexp.MustCompile(`^[0-9A-Za-z._-]{1,50}$`)
	accountPattern  = regexp.MustCompile(`^[0-9A-Za-z.]{1,25}$`)
	currencyPattern = regexp.MustCompile(`^[0-9A-Za-z]{1,10}$`)
)

// maxFilterLen chặn chuỗi lọc dài bất thường — chúng đi vào câu SQL (đã escape).
const maxFilterLen = 200

type Service struct {
	conn clickhouse.Conn

	// useStaging: true = đọc từ eb_staging.stg_general_ledger(_detail) thay vì
	// eb_dwh.fact_gl_entry_line — xem staging_source.sql. Dùng tạm khi pipeline fact lỗi.
	useStaging bool

	// stagingCompanies: CompanyID (chữ thường) chạy đường staging dù useStaging
	// = false — nhóm end tester, xem config.SoChiTietCongNoPhaiThuStagingCompanyIDs.
	stagingCompanies map[string]struct{}
}

func NewService(conn clickhouse.Conn, useStaging bool, stagingCompanyIDs []string) *Service {
	source := "fact (eb_dwh.fact_gl_entry_line)"
	if useStaging {
		source = "staging (eb_staging.stg_general_ledger...)"
	}
	log.Printf("so-chi-tiet-cong-no-phai-thu: GL source = %s [biến môi trường SO_CHI_TIET_CONG_NO_PHAI_THU_DATA_SOURCE]", source)
	staging := make(map[string]struct{}, len(stagingCompanyIDs))
	for _, id := range stagingCompanyIDs {
		staging[strings.ToLower(id)] = struct{}{}
	}
	if len(staging) > 0 {
		log.Printf("so-chi-tiet-cong-no-phai-thu: %d công ty chạy staging [SO_CHI_TIET_CONG_NO_PHAI_THU_STAGING_COMPANY_IDS]", len(staging))
	}
	return &Service{conn: conn, useStaging: useStaging, stagingCompanies: staging}
}

// stagingFor: request này đọc từ staging khi bật toàn cục, hoặc khi công ty
// chính nằm trong danh sách tester.
func (s *Service) stagingFor(p QueryParams) bool {
	if s.useStaging {
		return true
	}
	_, ok := s.stagingCompanies[strings.ToLower(p.PrimaryCompanyID)]
	return ok
}

type driverRows interface {
	Scan(dest ...any) error
}

// QueryParams ánh xạ tham số của Proc_Chi_Tiet_Cong_No_Phai_Thu.
type QueryParams struct {
	CompanyIDs       []string // đã swap UUID phía Java
	PrimaryCompanyID string
	FromDate         string // YYYY-MM-DD
	ToDate           string // YYYY-MM-DD
	IsFinancialBook  int    // 1 = sổ tài chính, 0 = sổ quản trị (@currentBook)
	IsDependent      int    // 1 = gộp đơn vị phụ thuộc
	GroupSameItem    int    // 1 = cộng gộp bút toán giống nhau (@IsSimilarSum)
	AccountNumber    string // tài khoản công nợ, khớp tiền tố
	CurrencyID       string // "TH" hoặc rỗng = mọi loại tiền

	CheckAll   int      // @ParamCheckAll: 1 = mọi đối tượng trừ ObjectIDs; 0 = chỉ ObjectIDs
	ObjectIDs  []string // đã swap UUID phía Java
	ObjectCode string   // bộ lọc, chỉ áp dụng khi CheckAll = 1
	ObjectName string
	ObjectAddr string
	GroupID    string // UUID đã swap

	GetAccountHasData int    // 1 = chỉ giữ đối tượng có phát sinh trong kỳ
	ClusterID         string // multi-cluster, rỗng = không lọc
}

// =============================================================================
// ENTRYPOINT
// =============================================================================

// GetSoChiTiet gom toàn bộ kết quả vào slice. Chỉ dùng cho báo cáo nhỏ — ở quy
// mô lớn phải dùng StreamSoChiTiet.
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

func (s *Service) StreamSoChiTiet(
	ctx context.Context, p QueryParams, onRow func(Row) error,
) (int64, error) {
	return s.StreamSoChiTietWithTotal(ctx, p, nil, onRow)
}

// StreamSoChiTietWithTotal giống StreamSoChiTiet, thêm onTotal: gọi ĐÚNG MỘT LẦN,
// TRƯỚC dòng đầu tiên, với tổng số dòng báo cáo sẽ phát ra (kể cả dòng số dư đầu
// kỳ) — để client hiện tiến độ "Đã tải x/y dòng" ngay từ đầu.
//
// Câu đếm chạy SONG SONG với truy vấn chính và bọc CHÍNH truy vấn đó nên không bao
// giờ lệch bộ lọc. Câu đếm lỗi thì chỉ ghi log và bỏ qua onTotal. onTotal = nil thì
// không chạy câu đếm.
func (s *Service) StreamSoChiTietWithTotal(
	ctx context.Context, p QueryParams, onTotal func(int64) error, onRow func(Row) error,
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
	kind, err := s.resolveAccountKind(ctx, p, companyIDs)
	if err != nil {
		return 0, fmt.Errorf("resolve account kind: %w", err)
	}

	staging := s.stagingFor(p)

	// Truy vấn chính khởi chạy ngay, song song với câu đếm.
	mainCtx, cancelMain := context.WithCancel(ctx)
	defer cancelMain()
	mainCh := make(chan detailResult, 1)
	go func() {
		r, e := s.openRows(mainCtx, buildQuery(companyIDs, p, typeLedger, noCol, staging, true))
		mainCh <- detailResult{rows: r, err: e}
	}()

	countCtx, cancelCount := context.WithCancel(ctx)
	defer cancelCount()
	var countCh chan countResult
	if onTotal != nil {
		countCh = make(chan countResult, 1)
		go func() {
			n, e := s.countRows(countCtx, buildCountQuery(companyIDs, p, typeLedger, noCol, staging))
			countCh <- countResult{n: n, err: e}
		}()
	}

	res := <-mainCh
	if res.err != nil {
		return 0, fmt.Errorf("query error: %w", res.err)
	}
	rows := res.rows
	defer rows.Close()

	if countCh != nil {
		c := <-countCh
		if c.err != nil {
			// Không có tổng thì client chỉ mất phần "/y" — không đáng làm hỏng báo cáo.
			log.Printf("so-chi-tiet-cong-no-phai-thu: bỏ qua tổng số dòng, câu đếm lỗi: %v", c.err)
		} else if c.n > 0 {
			if err := onTotal(c.n); err != nil {
				return 0, fmt.Errorf("lỗi gửi tổng số dòng: %w", err)
			}
		}
	}

	w := &walker{account: p.AccountNumber, kind: kind, onRow: onRow}
	for rows.Next() {
		// Client ngắt giữa chừng thì dừng ngay, không quét nốt phần còn lại.
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
		}

		r, err := scanRow(rows)
		if err != nil {
			return 0, fmt.Errorf("scan error: %w", err)
		}
		if err := w.push(r); err != nil {
			return 0, fmt.Errorf("lỗi xử lý dòng: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %w", err)
	}

	return time.Since(start).Milliseconds(), nil
}

// =============================================================================
// WALKER — "Tính số tồn" cuối proc
// =============================================================================

// walker cộng dồn số dư theo từng đối tượng. Truy vấn trả dòng đã sắp theo đối tượng,
// trong mỗi đối tượng dòng số dư đầu kỳ (nếu có) đứng đầu.
//
// Quy ước của proc: số dư lưu theo chiều Có dương, tức net = Có − Nợ. Dòng đầu kỳ đặt
// net bằng tổng Có − tổng Nợ của mọi bút toán trước kỳ; mỗi dòng phát sinh cộng
// Có − Nợ. Cặp cột Nợ/Có cuối kỳ suy ra từ net theo loại tài khoản (applyKindClosing).
type walker struct {
	account string
	kind    int32
	onRow   func(Row) error

	currentObject string
	net, netNT    decimal.Decimal
}

func (w *walker) push(r Row) error {
	if r.AccountingObjectID != w.currentObject || r.OrderType == OrderTypeOpening {
		w.currentObject = r.AccountingObjectID
		w.net, w.netNT = decimal.Zero, decimal.Zero
	}

	r.AccountNumber = w.account
	switch r.OrderType {
	case OrderTypeOpening:
		// Truy vấn đặt tổng Nợ/Có trước kỳ vào các cột phát sinh; dòng đầu kỳ của
		// proc có phát sinh bằng 0 và chỉ mang số dư.
		w.net = r.CreditAmount.Sub(r.DebitAmount.Decimal)
		w.netNT = r.CreditAmountNT.Sub(r.DebitAmountNT.Decimal)
		r.Description = OpeningDescription
		r.DebitAmount, r.CreditAmount = D(decimal.Zero), D(decimal.Zero)
		r.DebitAmountNT, r.CreditAmountNT = D(decimal.Zero), D(decimal.Zero)
	default:
		w.net = w.net.Add(r.CreditAmount.Sub(r.DebitAmount.Decimal))
		w.netNT = w.netNT.Add(r.CreditAmountNT.Sub(r.DebitAmountNT.Decimal))
	}
	applyKindClosing(&r, w.net, w.netNT, w.kind)
	return w.onRow(r)
}

// applyKindClosing quy đổi số dư thuần (Có − Nợ) sang cặp cột Nợ/Có theo loại tài khoản.
func applyKindClosing(r *Row, net, netNT decimal.Decimal, kind int32) {
	zero := D(decimal.Zero)
	switch kind {
	case KindDebit:
		r.ClosingDebitAmount, r.ClosingDebitAmountNT = D(net.Neg()), D(netNT.Neg())
		r.ClosingCreditAmount, r.ClosingCreditAmountNT = zero, zero
	case KindCredit:
		r.ClosingDebitAmount, r.ClosingDebitAmountNT = zero, zero
		r.ClosingCreditAmount, r.ClosingCreditAmountNT = D(net), D(netNT)
	default:
		r.ClosingDebitAmount = D(decimal.Max(net.Neg(), decimal.Zero))
		r.ClosingDebitAmountNT = D(decimal.Max(netNT.Neg(), decimal.Zero))
		r.ClosingCreditAmount = D(decimal.Max(net, decimal.Zero))
		r.ClosingCreditAmountNT = D(decimal.Max(netNT, decimal.Zero))
	}
}

// =============================================================================
// XÂY TRUY VẤN
// =============================================================================

const (
	// Thứ tự cộng dồn của proc (nhánh gộp): đối tượng, dòng đầu kỳ, ngày hạch toán,
	// ngày chứng từ, số chứng từ, hoá đơn, OrderPriority. NULL đứng trước như SQL
	// Server. Khoá phụ ở cuối để thứ tự tất định giữa các lần chạy.
	orderBy = "ORDER BY lowerUTF8(object_code), object_code, object_id, order_type,\n" +
		"         posted_date ASC NULLS FIRST, voucher_date ASC NULLS FIRST, no,\n" +
		"         invoice_date ASC NULLS FIRST, invoice_no, order_priority, tiebreak,\n" +
		"         account_corresponding"

	querySettings = "SETTINGS distributed_product_mode = 'local'"
)

// buildQuery dựng câu truy vấn chính (ordered = true) hoặc phần ruột để đếm.
func buildQuery(
	companyIDs []string, p QueryParams, typeLedger int, noCol string,
	useStaging bool, ordered bool,
) string {
	detail := detailUngroupedTemplate
	if p.GroupSameItem == 1 {
		detail = detailGroupedTemplate
	}
	order, settings := orderBy, querySettings
	if !ordered {
		order, settings = "", ""
	}

	// Thay các khối dài (có thể chứa placeholder con) TRƯỚC, rồi mới đến placeholder chung.
	sql := queryTemplate
	for k, v := range map[string]string{
		"{{AO_CTE}}":         buildAOCTE(p),
		"{{OPENING_BRANCH}}": openingBranchTemplate,
		"{{DETAIL_BRANCH}}":  detail,
		"{{ORDER_BY}}":       order,
		"{{QUERY_SETTINGS}}": settings,
	} {
		sql = strings.ReplaceAll(sql, k, v)
	}

	hasData := ""
	if p.GetAccountHasData == 1 {
		// Số dư đầu kỳ chỉ giữ đối tượng có phát sinh trong kỳ (proc: @listAccountHasData).
		hasData = "AND f.accounting_object_id IN (\n" +
			"        SELECT f.accounting_object_id FROM {{GL_SOURCE_RANGE}}\n" +
			"        WHERE f.posted_date >= toDate('{{FROM_DATE}}') AND f.posted_date <= toDate('{{TO_DATE}}')\n" +
			"          {{BASE_FILTER}}\n" +
			"          AND (coalesce(f.debit_amount, 0) != 0 OR coalesce(f.credit_amount, 0) != 0\n" +
			"            OR coalesce(f.debit_amount_original, 0) != 0 OR coalesce(f.credit_amount_original, 0) != 0)\n" +
			"      )"
	}
	sql = strings.ReplaceAll(sql, "{{HAS_DATA_FILTER}}", hasData)
	sql = strings.ReplaceAll(sql, "{{BASE_FILTER}}", baseFilter(p, typeLedger))
	sql = strings.ReplaceAll(sql, "{{NO_COL}}", noCol)

	return applyCommonPlaceholders(sql, companyIDs, p, useStaging)
}

// buildCountQuery đếm số dòng mà truy vấn chính trả — bọc CHÍNH truy vấn đó (bỏ
// ORDER BY/SETTINGS) để không bao giờ lệch bộ lọc với dòng thật.
func buildCountQuery(
	companyIDs []string, p QueryParams, typeLedger int, noCol string, useStaging bool,
) string {
	return "SELECT count() FROM (\n" +
		buildQuery(companyIDs, p, typeLedger, noCol, useStaging, false) +
		"\n) " + querySettings
}

// baseFilter là phần WHERE dùng chung cho mọi lần đọc GL (alias `f`), trừ ngày.
func baseFilter(p QueryParams, typeLedger int) string {
	return fmt.Sprintf(`AND f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
      -- type_ledger = 2 là bút toán dùng chung cho cả sổ tài chính lẫn quản trị.
      AND (f.type_ledger = %d OR f.type_ledger = 2)
      -- Khớp tiền tố như proc (GLD.Account LIKE '131%%'); đã qua accountPattern.
      AND f.account_number LIKE '%s%%'
      -- Fact là ReplacingMergeTree(__version, is_deleted): dòng đã xoá phải bị loại.
      AND f.is_deleted = 0
      %s
      %s`, typeLedger, p.AccountNumber, currencyFilter(p.CurrencyID), clusterFilter(p.ClusterID))
}

// buildAOCTE dựng CTE `ao`: các đối tượng thoả bộ lọc của proc. Tra theo công ty thay vì
// dictionary — dictionary là cache có độ trễ, không tự cập nhật khi đối tượng đổi mã/tên
// (cùng lý do với so-chi-tiet-cac-tai-khoan).
func buildAOCTE(p QueryParams) string {
	conds := []string{
		"argMax(__deleted, __source_ts_ms) = 0",
		"argMax(is_active, __source_ts_ms) = 1",
		"argMax(ifNull(object_type, 0), __source_ts_ms) IN (1, 2)",
	}
	list := "CAST([" + quoteJoin(p.ObjectIDs) + "] AS Array(UUID))"
	if p.CheckAll == 1 {
		if len(p.ObjectIDs) > 0 {
			conds = append(conds, "id NOT IN "+list)
		}
		if p.ObjectCode != "" {
			conds = append(conds, "positionCaseInsensitiveUTF8(code, "+sqlString(p.ObjectCode)+") > 0")
		}
		if p.ObjectName != "" {
			conds = append(conds, "positionCaseInsensitiveUTF8(name, "+sqlString(p.ObjectName)+") > 0")
		}
		if p.ObjectAddr != "" {
			// Proc cho địa chỉ NULL qua bộ lọc: (addr IS NOT NULL AND LIKE) OR addr IS NULL.
			conds = append(conds, "(address IS NULL OR positionCaseInsensitiveUTF8(address, "+sqlString(p.ObjectAddr)+") > 0)")
		}
		if p.GroupID != "" {
			conds = append(conds, "argMax(account_object_group_id, __source_ts_ms) = toUUID('"+p.GroupID+"')")
		}
	} else {
		conds = append(conds, "id IN "+list)
	}
	return `WITH ao AS
    (
        SELECT id,
               argMax(ifNull(accounting_object_code, ''), __source_ts_ms) AS code,
               argMax(ifNull(accounting_object_name, ''), __source_ts_ms) AS name,
               argMax(accounting_object_address, __source_ts_ms)          AS address
        FROM eb.accounting_object
        WHERE company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
        GROUP BY id
        HAVING ` + strings.Join(conds, "\n           AND ") + `
    )`
}

// applyCommonPlaceholders điền các placeholder dùng chung. {{GL_SOURCE_*}} phải được
// thay TRƯỚC vì staging_source.sql cũng chứa placeholder.
func applyCommonPlaceholders(sql string, companyIDs []string, p QueryParams, useStaging bool) string {
	srcRange, srcOpening := factSourceExpr, factSourceExpr
	if useStaging {
		// Cận ngày của nhánh staging: đẩy CHÍNH vị từ ngày của query vào truy vấn con
		// trước JOIN để cắt phân vùng. Phát sinh lấy [FROM, TO]; số dư đầu kỳ lấy mọi thứ
		// TRƯỚC FROM (và không bao giờ trước 1990 — xem query_opening_branch.sql).
		srcRange = strings.ReplaceAll(stagingSourceExpr, "{{STAGING_DATE_BOUND}}",
			"AND toDate(gl.PostedDate) >= toDate('"+p.FromDate+"') "+
				"AND toDate(gl.PostedDate) <= toDate('"+p.ToDate+"')")
		srcOpening = strings.ReplaceAll(stagingSourceExpr, "{{STAGING_DATE_BOUND}}",
			"AND toDate(gl.PostedDate) >= toDate('1990-01-01') "+
				"AND toDate(gl.PostedDate) < toDate('"+p.FromDate+"')")
	}
	sql = strings.ReplaceAll(sql, "{{GL_SOURCE_RANGE}}", srcRange)
	sql = strings.ReplaceAll(sql, "{{GL_SOURCE_OPENING}}", srcOpening)

	sql = strings.ReplaceAll(sql, "{{COMPANY_IDS}}", quoteJoin(companyIDs))
	sql = strings.ReplaceAll(sql, "{{ACCOUNT_NUMBER}}", p.AccountNumber)
	sql = strings.ReplaceAll(sql, "{{FROM_DATE}}", p.FromDate)
	sql = strings.ReplaceAll(sql, "{{TO_DATE}}", p.ToDate)
	return sql
}

// =============================================================================
// TRUY VẤN PHỤ TRỢ
// =============================================================================

// resolveCompanyScope mở rộng danh sách công ty theo đơn vị phụ thuộc — thay cho
// Func_getCompany(@CompanyID, @isDependent) của proc.
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

// resolveAccountKind lấy loại tài khoản (Nợ/Có/lưỡng tính) từ dim_account — thay cho
// AccountList.AccountGroupKind của proc. Ưu tiên công ty chính; không tìm thấy thì coi
// là lưỡng tính (cùng quy ước với so-chi-tiet-cac-tai-khoan khi kind NULL).
func (s *Service) resolveAccountKind(ctx context.Context, p QueryParams, companyIDs []string) (int32, error) {
	sql := fmt.Sprintf(`
		SELECT account_group_kind
		FROM eb_dwh.dim_account
		WHERE account_number = '%s'
		  AND company_id IN CAST([%s] AS Array(UUID))
		  AND is_current = 1 AND is_deleted = 0
		ORDER BY company_id = toUUID('%s') DESC
		LIMIT 1`, p.AccountNumber, quoteJoin(companyIDs), p.PrimaryCompanyID)

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return KindBoth, err
	}
	defer rows.Close()

	if rows.Next() {
		var kind *int32
		if err := rows.Scan(&kind); err != nil {
			return KindBoth, err
		}
		if kind != nil {
			return *kind, rows.Err()
		}
	}
	return KindBoth, rows.Err()
}

type countResult struct {
	n   int64
	err error
}

func (s *Service) countRows(ctx context.Context, sql string) (int64, error) {
	var n uint64
	err := withJoinFallback(ctx, func(c context.Context) error {
		n = 0
		rows, err := s.conn.Query(c, sql)
		if err != nil {
			return err
		}
		defer rows.Close()
		if rows.Next() {
			if err := rows.Scan(&n); err != nil {
				return err
			}
		}
		return rows.Err()
	})
	return int64(n), err
}

// detailResult: kết quả khởi chạy truy vấn chính ở goroutine riêng.
type detailResult struct {
	rows *primedRows
	err  error
}

// ── TỐI ƯU JOIN ──────────────────────────────────────────────────────────────
//
// Cùng cơ chế với so-nhat-ky-chung / so-chi-tiet-cac-tai-khoan: thuật toán JOIN mặc
// định của ClickHouse dựng cả hash table trong RAM và vỡ (MEMORY_LIMIT_EXCEEDED) với
// công ty lớn.
//
//   - Thử parallel_hash trước: nhanh hơn grace_hash ~3 lần nhưng KHÔNG spill ra đĩa,
//     vượt max_bytes_in_join thì lỗi.
//   - Gặp lỗi bộ nhớ/giới hạn JOIN thì chạy lại ĐÚNG câu đó bằng grace_hash (chia bucket
//     và spill ra đĩa — chậm hơn nhưng không bao giờ vỡ).
//   - allow_statistics_optimize = 0: nạp thống kê part để tối ưu thứ tự JOIN tốn ~1,5 s
//     mỗi truy vấn trên bảng nhiều part, bất kể lượng dữ liệu.
//
// Chỉ đổi cách thực thi, KHÔNG đổi kết quả. Truy vấn nào cũng JOIN với `ao` nên luôn áp
// dụng.
//
// Là biến chứ không phải hằng để test ép được đường fallback.
var (
	joinMaxBytesFast int64 = 4 << 30 // parallel_hash: RAM node đã được cấp thêm
	joinMaxBytesSafe int64 = 1 << 30 // grace_hash: chừa chỗ cho các query song song
)

func joinSettings(ctx context.Context, algorithm string, maxBytes int64) context.Context {
	return clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{
		"join_algorithm":            algorithm,
		"max_bytes_in_join":         maxBytes,
		"allow_statistics_optimize": 0,
	}))
}

// Mã lỗi ClickHouse khi bảng hash của JOIN vượt giới hạn / hết RAM truy vấn.
const (
	chErrSetSizeLimitExceeded = 191
	chErrMemoryLimitExceeded  = 241
)

func isJoinMemoryError(err error) bool {
	var ex *clickhouse.Exception
	return errors.As(err, &ex) && (ex.Code == chErrSetSizeLimitExceeded || ex.Code == chErrMemoryLimitExceeded)
}

// withJoinFallback chạy fn với parallel_hash, và chỉ khi vỡ bộ nhớ JOIN mới chạy lại
// với grace_hash. fn phải đặt lại mọi trạng thái nó tích luỹ vì có thể chạy hai lần.
func withJoinFallback(ctx context.Context, fn func(context.Context) error) error {
	err := fn(joinSettings(ctx, "parallel_hash", joinMaxBytesFast))
	if err != nil && isJoinMemoryError(err) {
		log.Printf("so-chi-tiet-cong-no-phai-thu: parallel_hash lỗi bộ nhớ (%v), chạy lại bằng grace_hash", err)
		return fn(joinSettings(ctx, "grace_hash", joinMaxBytesSafe))
	}
	return err
}

// primedRows bọc driver.Rows đã đọc sẵn khối đầu tiên. Lỗi vỡ bộ nhớ JOIN chỉ lộ ra ở
// khối đầu tiên (lúc dựng hash table), SAU khi Query() đã trả về nên không bắt được bằng
// err của Query(): phải đọc thử một dòng rồi mới biết có cần chạy lại.
type primedRows struct {
	rows     driver.Rows
	firstOK  bool
	consumed bool
}

func (p *primedRows) Next() bool {
	if !p.consumed {
		p.consumed = true
		return p.firstOK
	}
	return p.rows.Next()
}

func (p *primedRows) Scan(dest ...any) error { return p.rows.Scan(dest...) }
func (p *primedRows) Err() error             { return p.rows.Err() }
func (p *primedRows) Close() error           { return p.rows.Close() }

// openRows mở truy vấn chính dạng stream, có fallback như withJoinFallback. Sau khi thành
// công, rows đã được đọc trước dòng đầu tiên (nếu có) nhưng người gọi vẫn dùng
// Next()/Scan() bình thường.
func (s *Service) openRows(ctx context.Context, sql string) (*primedRows, error) {
	open := func(c context.Context) (*primedRows, error) {
		rows, err := s.conn.Query(c, sql)
		if err != nil {
			return nil, err
		}
		p := &primedRows{rows: rows, firstOK: rows.Next()}
		if !p.firstOK {
			if err := rows.Err(); err != nil {
				_ = rows.Close()
				return nil, err
			}
		}
		return p, nil
	}
	p, err := open(joinSettings(ctx, "parallel_hash", joinMaxBytesFast))
	if err != nil && isJoinMemoryError(err) {
		log.Printf("so-chi-tiet-cong-no-phai-thu: parallel_hash lỗi bộ nhớ (%v), chạy lại bằng grace_hash", err)
		return open(joinSettings(ctx, "grace_hash", joinMaxBytesSafe))
	}
	return p, err
}

// =============================================================================
// SCAN + VALIDATE + HELPERS
// =============================================================================

// scanRow đọc một dòng theo ĐÚNG thứ tự cột của query.sql.
func scanRow(rows driverRows) (Row, error) {
	var (
		orderType     int32
		objectID      *uuid.UUID
		objCode       string
		objName       string
		referenceID   *uuid.UUID
		typeID        *int32
		postedDate    *time.Time
		voucherDate   *time.Time
		no            string
		invoiceDate   *time.Time // chỉ để sắp xếp, không xuất
		invoiceNo     string     // chỉ để sắp xếp, không xuất
		description   string
		corr          string
		debitNT       decimal.Decimal
		debit         decimal.Decimal
		creditNT      decimal.Decimal
		credit        decimal.Decimal
		orderPriority int32
		tiebreak      string // chỉ để sắp xếp, không xuất
	)
	if err := rows.Scan(
		&orderType, &objectID, &objCode, &objName,
		&referenceID, &typeID, &postedDate, &voucherDate, &no,
		&invoiceDate, &invoiceNo, &description, &corr,
		&debitNT, &debit, &creditNT, &credit,
		&orderPriority, &tiebreak,
	); err != nil {
		return Row{}, err
	}

	r := Row{
		OrderType:                  orderType,
		AccountingObjectID:         uuidOrEmpty(objectID),
		AccountingObjectCode:       objCode,
		AccountingObjectName:       objName,
		ReferenceID:                uuidOrEmpty(referenceID),
		TypeID:                     int32OrZero(typeID),
		No:                         no,
		Description:                description,
		CorrespondingAccountNumber: corr,
		DebitAmountNT:              D(debitNT),
		DebitAmount:                D(debit),
		CreditAmountNT:             D(creditNT),
		CreditAmount:               D(credit),
		OrderPriority:              orderPriority,
	}
	if postedDate != nil {
		r.PostedDate = *postedDate
	}
	if voucherDate != nil {
		r.Date = *voucherDate
	}
	return r, nil
}

func validateParams(p QueryParams) error {
	if len(p.CompanyIDs) == 0 {
		return fmt.Errorf("companyIds không được rỗng")
	}
	if p.PrimaryCompanyID == "" {
		return fmt.Errorf("primaryCompanyId không được rỗng")
	}
	ids := append([]string{}, p.CompanyIDs...)
	ids = append(ids, p.PrimaryCompanyID)
	ids = append(ids, p.ObjectIDs...)
	if p.GroupID != "" {
		ids = append(ids, p.GroupID)
	}
	for _, id := range ids {
		if !uuidPattern.MatchString(id) {
			return fmt.Errorf("ID không đúng định dạng UUID: %s", id)
		}
	}
	// Ngày PHẢI parse được, không chỉ kiểm rỗng: chúng đi thẳng vào chuỗi SQL.
	from, err := time.Parse("2006-01-02", p.FromDate)
	if err != nil {
		return fmt.Errorf("fromDate phải dạng YYYY-MM-DD: %q", p.FromDate)
	}
	to, err := time.Parse("2006-01-02", p.ToDate)
	if err != nil {
		return fmt.Errorf("toDate phải dạng YYYY-MM-DD: %q", p.ToDate)
	}
	if to.Before(from) {
		return fmt.Errorf("toDate sớm hơn fromDate")
	}
	if !accountPattern.MatchString(p.AccountNumber) {
		return fmt.Errorf("accountNumber không hợp lệ: %q", p.AccountNumber)
	}
	if p.CurrencyID != "" && !currencyPattern.MatchString(p.CurrencyID) {
		return fmt.Errorf("currencyId không hợp lệ: %q", p.CurrencyID)
	}
	if p.ClusterID != "" && !clusterPattern.MatchString(p.ClusterID) {
		return fmt.Errorf("clusterId không hợp lệ: %q", p.ClusterID)
	}
	for name, v := range map[string]string{
		"accountingObjectCode":    p.ObjectCode,
		"accountingObjectName":    p.ObjectName,
		"accountingObjectAddress": p.ObjectAddr,
	} {
		if len(v) > maxFilterLen {
			return fmt.Errorf("%s dài quá %d ký tự", name, maxFilterLen)
		}
	}
	for name, v := range map[string]int{
		"isFinancialBook":   p.IsFinancialBook,
		"isDependent":       p.IsDependent,
		"groupSameItem":     p.GroupSameItem,
		"checkAll":          p.CheckAll,
		"getAccountHasData": p.GetAccountHasData,
	} {
		if v != 0 && v != 1 {
			return errors.New(name + " phải là 0 hoặc 1")
		}
	}
	return nil
}

// currencyFilter và clusterFilter dùng bí danh `f` của mọi lần đọc GL.
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
	// clusterID đã qua clusterPattern trong validateParams.
	return fmt.Sprintf("AND f.cluster_id = '%s'", clusterID)
}

// sqlString đặt chuỗi người dùng vào literal ClickHouse: escape '\' và '\”.
func sqlString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
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
	q := make([]string, len(ids))
	for i, id := range ids {
		q[i] = fmt.Sprintf("'%s'", id)
	}
	return strings.Join(q, ", ")
}

// sqlServerUUIDSwap đảo byte 3 nhóm đầu của UUID (time_low 4B, time_mid 2B,
// time_hi_and_version 2B) — bù cho cách SQL Server UNIQUEIDENTIFIER lưu byte
// khác chuẩn RFC4122. company_id/object id đã được PHÍA JAVA swap trước khi gửi vào nên
// WHERE khớp đúng cột UUID trong ClickHouse; nhưng id ĐỌC RA chưa ai swap lại, nếu trả
// thẳng thì reflink ở frontend trỏ sai chứng từ. Hàm tự nghịch đảo.
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

func int32OrZero(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
