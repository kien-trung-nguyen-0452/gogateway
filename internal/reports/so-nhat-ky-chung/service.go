// Package so_nhat_ky_chung — sổ nhật ký chung (SoNhatKyChung).
//
// Nguồn tham chiếu: Proc_SO_NHAT_KY_CHUNG (2.335 dòng, 8 nhánh).
//
// Proc gốc lặp lại gần như y hệt tám lần cho tổ hợp
// (@IsDefaultType × @isDependent × @GroupTheSameItem). Ở đây gộp thành ba tham số
// độc lập và hai mẫu truy vấn:
//
//   - @isDependent: chỉ đổi DANH SÁCH CÔNG TY (resolveCompanyScope), không đổi
//     câu truy vấn — proc cũng chỉ thay `CompanyID = @CompanyID` bằng
//     `CompanyID IN Func_getCompany(...)`.
//   - @GroupTheSameItem: chọn query_grouped.sql (mỗi bút toán tách hai dòng Nợ/Có,
//     cộng gộp) hay query_detail.sql (mỗi dòng fact là một dòng sổ).
//   - @IsDefaultType: mẫu mặc định KHÔNG lấy hoá đơn, đối tượng, khoản mục và
//     trường tuỳ chỉnh (proc trả NULL cho các cột đó) — xem buildExprs.
//
// Khác sổ chi tiết tài khoản: sổ này không phân nhóm theo tài khoản và không có
// số dư luỹ kế từng dòng, nên KHÔNG cần walker. Query đã ORDER BY đúng thứ tự
// proc; dòng "Số lũy kế kỳ trước chuyển sang" (nếu bật) được phát ĐẦU TIÊN.
//
// KHÁC BIỆT CÒN LẠI SO VỚI PROC (đã biết, chưa xử lý)
//
//  1. Hoá đơn: proc ghi đè InvoiceNo/Date/Series bằng PPServiceDetail /
//     PPInvoiceDetail khi DetailID trùng. DWH chưa có hai bảng này (raw_pp_*
//     đang rỗng) nên dùng invoice_* của fact.
//  2. Số luỹ kế: proc đọc SummaryOpeningBalance; ở đây cộng thẳng từ fact — xem
//     query_opening.sql.
//  3. Dòng gộp chỉ có một bên Nợ hoặc Có: proc trả NULL cho bên kia, ở đây là 0.
//
// FALLBACK STAGING — giống so-chi-tiet-cac-tai-khoan
// --------------------------------------------------
// useStaging bật đọc từ eb_staging.stg_general_ledger(_detail) thay vì
// eb_dwh.fact_gl_entry_line khi pipeline fact bị lỗi, không cần deploy lại code.
// Xem staging_source.sql và config.SoNhatKyChungUseStaging.
package so_nhat_ky_chung

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

//go:embed query_detail.sql
var detailTemplate string

//go:embed query_grouped.sql
var groupedTemplate string

//go:embed query_opening.sql
var openingTemplate string

//go:embed staging_source.sql
var stagingSourceExpr string

// factSourceExpr là giá trị {{GL_SOURCE}} mặc định — đọc thẳng fact table.
const factSourceExpr = "eb_dwh.fact_gl_entry_line AS f FINAL"

var (
	uuidPattern    = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	clusterPattern = regexp.MustCompile(`^[0-9A-Za-z._-]{1,50}$`)
)

type Service struct {
	conn clickhouse.Conn

	// useStaging: true = đọc từ eb_staging.stg_general_ledger(_detail) thay vì
	// eb_dwh.fact_gl_entry_line — xem staging_source.sql và
	// config.SoNhatKyChungUseStaging. Dùng tạm khi pipeline fact lỗi.
	useStaging bool

	// stagingCompanies: CompanyID (chữ thường) chạy đường staging dù useStaging
	// = false — nhóm end tester, xem config.SoNhatKyChungStagingCompanyIDs.
	stagingCompanies map[string]struct{}
}

func NewService(conn clickhouse.Conn, useStaging bool, stagingCompanyIDs []string) *Service {
	source := "fact (eb_dwh.fact_gl_entry_line)"
	if useStaging {
		source = "staging (eb_staging.stg_general_ledger...)"
	}
	log.Printf("so-nhat-ky-chung: GL source = %s [biến môi trường SO_NHAT_KY_CHUNG_DATA_SOURCE]", source)
	staging := make(map[string]struct{}, len(stagingCompanyIDs))
	for _, id := range stagingCompanyIDs {
		staging[strings.ToLower(id)] = struct{}{}
	}
	if len(staging) > 0 {
		log.Printf("so-nhat-ky-chung: %d công ty chạy staging [SO_NHAT_KY_CHUNG_STAGING_COMPANY_IDS]", len(staging))
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

// QueryParams ánh xạ tham số của Proc_SO_NHAT_KY_CHUNG.
type QueryParams struct {
	CompanyIDs       []string // đã swap UUID phía Java
	PrimaryCompanyID string
	FromDate         string // YYYY-MM-DD
	ToDate           string // YYYY-MM-DD
	IsFinancialBook  int    // 1 = sổ tài chính, 0 = sổ quản trị
	IsDependent      int    // 1 = gộp đơn vị phụ thuộc
	GroupSameItem    int    // 1 = cộng gộp bút toán giống nhau
	ShowAccumAmount  int    // 1 = thêm dòng "Số lũy kế kỳ trước chuyển sang"
	IsDefaultType    bool   // true = mẫu mặc định (không cột phân tích)
	ClusterID        string // multi-cluster, rỗng = không lọc
}

// =============================================================================
// ENTRYPOINT
// =============================================================================

// GetSoNhatKyChung gom toàn bộ kết quả vào slice. Chỉ dùng cho báo cáo nhỏ —
// sổ nhật ký cả năm có thể hàng triệu dòng; ở quy mô đó phải dùng
// StreamSoNhatKyChung.
func (s *Service) GetSoNhatKyChung(ctx context.Context, p QueryParams) ([]Row, int64, error) {
	var result []Row
	elapsedMs, err := s.StreamSoNhatKyChung(ctx, p, func(row Row) error {
		result = append(result, row)
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return result, elapsedMs, nil
}

func (s *Service) StreamSoNhatKyChung(
	ctx context.Context, p QueryParams, onRow func(Row) error,
) (int64, error) {
	return s.StreamSoNhatKyChungWithTotal(ctx, p, nil, onRow)
}

// StreamSoNhatKyChungWithTotal giống StreamSoNhatKyChung, thêm onTotal: gọi ĐÚNG
// MỘT LẦN, TRƯỚC dòng đầu tiên, với tổng số dòng báo cáo sẽ phát ra (kể cả dòng
// lũy kế) — để client hiện tiến độ "Đã tải x/y dòng" ngay từ đầu.
//
// Câu đếm chạy SONG SONG với truy vấn chi tiết và số lũy kế. Câu đếm lỗi thì chỉ
// ghi log và bỏ qua onTotal, báo cáo vẫn chạy bình thường. onTotal = nil thì
// không chạy câu đếm.
func (s *Service) StreamSoNhatKyChungWithTotal(
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

	staging := s.stagingFor(p)
	// Có JOIN thật sự thì mới cần cấu hình riêng (xem joinSettings): nhánh staging luôn
	// JOIN gl với gld; nhánh fact chỉ JOIN khi dùng mẫu đầy đủ (đối tượng, chứng từ).
	hasJoin := staging || !p.IsDefaultType
	detailSQL := buildDetailQuery(companyIDs, p, typeLedger, noCol, staging)

	// Truy vấn chi tiết không phụ thuộc số lũy kế nên khởi chạy ngay, song song
	// với câu đếm và câu lũy kế. Dòng chi tiết chỉ được ĐỌC sau khi dòng lũy kế
	// (phát đầu tiên) đã xong.
	detailCtx, cancelDetail := context.WithCancel(ctx)
	defer cancelDetail()
	detailCh := make(chan detailResult, 1)
	go func() {
		r, e := s.openRows(detailCtx, detailSQL, hasJoin)
		detailCh <- detailResult{rows: r, err: e}
	}()

	countCtx, cancelCount := context.WithCancel(ctx)
	defer cancelCount()
	var countCh chan countResult
	if onTotal != nil {
		countCh = make(chan countResult, 1)
		go func() {
			n, e := s.countRows(countCtx, buildCountQuery(companyIDs, p, typeLedger, noCol, staging), hasJoin)
			countCh <- countResult{n: n, err: e}
		}()
	}

	var (
		acc    accum
		accErr error
		wg     sync.WaitGroup
	)
	if p.ShowAccumAmount == 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Số lũy kế chỉ JOIN ở nhánh staging (gl với gld).
			acc, accErr = s.loadAccum(ctx, buildOpeningQuery(companyIDs, p, typeLedger, staging), staging)
		}()
	}
	wg.Wait()
	if accErr != nil {
		cancelDetail()
		drainDetail(detailCh)
		return 0, fmt.Errorf("load accumulated amount: %w", accErr)
	}

	detail := <-detailCh
	if detail.err != nil {
		return 0, fmt.Errorf("query error: %w", detail.err)
	}
	rows := detail.rows
	defer rows.Close()

	if countCh != nil {
		c := <-countCh
		if c.err != nil {
			// Không có tổng thì client chỉ mất phần "/y" — không đáng làm hỏng báo cáo.
			log.Printf("so-nhat-ky-chung: bỏ qua tổng số dòng, câu đếm lỗi: %v", c.err)
		} else {
			total := c.n
			if p.ShowAccumAmount == 1 {
				total++
			}
			if total > 0 {
				if err := onTotal(total); err != nil {
					return 0, fmt.Errorf("lỗi gửi tổng số dòng: %w", err)
				}
			}
		}
	}

	if p.ShowAccumAmount == 1 {
		if err := onRow(accumRow(acc)); err != nil {
			return 0, fmt.Errorf("lỗi xử lý dòng lũy kế: %w", err)
		}
	}

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
		if err := onRow(r); err != nil {
			return 0, fmt.Errorf("lỗi xử lý dòng: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("row iteration error: %w", err)
	}

	return time.Since(start).Milliseconds(), nil
}

// accumRow dựng dòng "Số lũy kế kỳ trước chuyển sang". Proc để PostedDate =
// 1900-01-01 và OderType = 0 để dòng này luôn đứng đầu; ở đây dòng được phát
// trước mọi dòng khác nên không cần ngày giả — PostedDate để zero (rỗng ở proto).
func accumRow(a accum) Row {
	return Row{
		OrderType:      OrderTypeAccum,
		Description:    AccumDescription,
		DebitAmount:    D(a.Debit),
		CreditAmount:   D(a.Credit),
		DebitAmountNT:  D(a.DebitOrig),
		CreditAmountNT: D(a.CreditOrig),
	}
}

// =============================================================================
// XÂY TRUY VẤN
// =============================================================================

const (
	// Thứ tự của lệnh SELECT cuối proc: PostedDate, InvoiceNo, No, OrderPriority,
	// SortOrder. invoice_no/no đã ifNull('') nên NULL (rỗng) đứng trước như SQL
	// Server. Khoá phụ ở sau để thứ tự tất định giữa các lần chạy.
	orderByGrouped = "ORDER BY posted_date, invoice_no, no, order_priority, sort_order,\n" +
		"         reference_id, account_number, account_corresponding"
	orderByDetail = orderByGrouped + ", f.line_bk"

	querySettings = "SETTINGS distributed_product_mode = 'local'"

	zeroUUID = "toUUID('00000000-0000-0000-0000-000000000000')"

	// aoCTE tra mã/tên đối tượng theo công ty thay vì dictionary — cùng lý do với
	// so-chi-tiet-cac-tai-khoan: dictionary là cache có độ trễ, không tự cập nhật
	// khi đối tượng đổi mã/tên.
	aoCTE = `WITH ao AS
    (
        SELECT id,
               argMax(ifNull(accounting_object_code, ''), __source_ts_ms) AS code,
               argMax(ifNull(accounting_object_name, ''), __source_ts_ms) AS name
        FROM eb.accounting_object
        WHERE company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
        GROUP BY id
        HAVING argMax(__deleted, __source_ts_ms) = 0
    )`

	// csJoin tái hiện #CodeSummary của proc (chỉ nhánh gộp, mẫu đầy đủ): với mỗi
	// chứng từ, một chiều (khoản mục, phòng ban, ...) chỉ được hiện khi MỌI dòng
	// của chứng từ cùng một giá trị; khác nhau thì để trống. MIN/MAX bỏ qua NULL
	// như SQL Server nên min = max nghĩa là các giá trị khác NULL đều trùng nhau.
	// Phạm vi quét phải TRÙNG phạm vi #GL_GLDetailRaw của proc (không lọc số tiền).
	csJoin = `LEFT JOIN
    (
        SELECT f.reference_id AS reference_id,
               if(min(f.expense_item_id)     = max(f.expense_item_id),     min(f.expense_item_id),     NULL) AS expense_item_id,
               if(min(f.budget_item_id)      = max(f.budget_item_id),      min(f.budget_item_id),      NULL) AS budget_item_id,
               if(min(f.statistics_code_id)  = max(f.statistics_code_id),  min(f.statistics_code_id),  NULL) AS statistics_code_id,
               if(min(f.department_id)       = max(f.department_id),       min(f.department_id),       NULL) AS department_id,
               if(min(f.cost_set_id)         = max(f.cost_set_id),         min(f.cost_set_id),         NULL) AS cost_set_id,
               if(min(f.contract_id)         = max(f.contract_id),         min(f.contract_id),         NULL) AS contract_id,
               if(min(f.accounting_object_id) = max(f.accounting_object_id), min(f.accounting_object_id), NULL) AS accounting_object_id
        FROM {{GL_SOURCE}}
        WHERE f.company_id IN CAST([{{COMPANY_IDS}}] AS Array(UUID))
          AND f.posted_date >= toDate('{{FROM_DATE}}')
          AND f.posted_date <= toDate('{{TO_DATE}}')
          AND ifNull(f.account_corresponding, '') != ''
          AND (f.type_ledger = {{TYPE_LEDGER}} OR f.type_ledger = 2)
          AND f.is_deleted = 0
          {{CLUSTER_FILTER}}
        GROUP BY f.reference_id
    ) AS cs ON cs.reference_id = f.reference_id`
)

// exprs gom các biểu thức cột phân tích — chúng thay đổi theo @IsDefaultType và
// @GroupTheSameItem.
type exprs struct {
	withClause, aoJoin, csJoin, extraGroupBy string

	invoiceNo, invoiceSeries, invoiceDate string

	aoCode, aoName         string
	expenseCode, expenseNm string
	budgetCode, budgetNm   string
	orgCode, orgNm         string
	costSetCode, costSetNm string
	contract               string
	statCode, statNm       string

	cf, cfd [5]string
}

// dictExpr tra một thuộc tính của dictionary theo id (khoá phức tuple). id NULL
// hoặc không có trong dictionary đều ra chuỗi rỗng.
func dictExpr(dict, attr, idExpr string) string {
	return fmt.Sprintf("dictGetOrDefault('eb.%s', '%s', tuple(ifNull(%s, %s)), '')",
		dict, attr, idExpr, zeroUUID)
}

// buildExprs chọn biểu thức theo (isDefault, grouped). Mẫu mặc định: proc trả NULL
// cho hoá đơn (trừ InvoiceSeries ở nhánh gộp), đối tượng, khoản mục và trường tuỳ
// chỉnh — ở đây là chuỗi rỗng / NULL.
func buildExprs(p QueryParams) exprs {
	grouped := p.GroupSameItem == 1
	var e exprs

	const nullDate = "CAST(NULL AS Nullable(Date))"

	if p.IsDefaultType {
		e.invoiceNo, e.invoiceDate = "''", nullDate
		// Nhánh gộp mặc định vẫn nhóm và trả InvoiceSeries; nhánh không gộp trả NULL.
		if grouped {
			e.invoiceSeries = "ifNull(f.invoice_series, '')"
			e.extraGroupBy = ", f.invoice_series"
		} else {
			e.invoiceSeries = "''"
		}
		e.aoCode, e.aoName = "''", "''"
		e.expenseCode, e.expenseNm = "''", "''"
		e.budgetCode, e.budgetNm = "''", "''"
		e.orgCode, e.orgNm = "''", "''"
		e.costSetCode, e.costSetNm = "''", "''"
		e.contract = "''"
		e.statCode, e.statNm = "''", "''"
		for i := range e.cf {
			e.cf[i], e.cfd[i] = "''", "''"
		}
		return e
	}

	e.withClause = aoCTE
	e.invoiceNo = "ifNull(f.invoice_no, '')"
	e.invoiceSeries = "ifNull(f.invoice_series, '')"
	e.invoiceDate = "f.invoice_date"

	if grouped {
		// Chiều phân tích lấy từ #CodeSummary (một giá trị duy nhất theo chứng từ).
		// Chúng là hàm của reference_id nên bọc any() để không phải thêm vào GROUP BY.
		e.csJoin = csJoin
		e.aoJoin = "LEFT JOIN ao ON ao.id = cs.accounting_object_id"
		e.extraGroupBy = ", f.invoice_date, f.invoice_no, f.invoice_series"
		e.aoCode, e.aoName = "ifNull(any(ao.code), '')", "ifNull(any(ao.name), '')"
		id := func(col string) string { return "any(cs." + col + ")" }
		e.expenseCode = dictExpr("dict_expense_item", "expense_item_code", id("expense_item_id"))
		e.expenseNm = dictExpr("dict_expense_item", "expense_item_name", id("expense_item_id"))
		e.budgetCode = dictExpr("dict_budget_item", "budget_item_code", id("budget_item_id"))
		e.budgetNm = dictExpr("dict_budget_item", "budget_item_name", id("budget_item_id"))
		e.orgCode = dictExpr("dict_eb_organization_unit", "organization_unit_code", id("department_id"))
		e.orgNm = dictExpr("dict_eb_organization_unit", "organization_unit_name", id("department_id"))
		e.costSetCode = dictExpr("dict_cost_set", "cost_set_code", id("cost_set_id"))
		e.costSetNm = dictExpr("dict_cost_set", "cost_set_name", id("cost_set_id"))
		e.contract = dictExpr("dict_em_contract", "no", id("contract_id"))
		e.statCode = dictExpr("dict_statistics_code", "statistics_code", id("statistics_code_id"))
		e.statNm = dictExpr("dict_statistics_code", "statistics_code_name", id("statistics_code_id"))
		// CustomField1..5 là MAX() trong proc; CustomFieldDetail không có ở nhánh gộp.
		for i := range e.cf {
			e.cf[i] = fmt.Sprintf("ifNull(max(f.custom_field%d), '')", i+1)
			e.cfd[i] = "''"
		}
		return e
	}

	// Nhánh không gộp: chiều phân tích theo từng dòng.
	e.aoJoin = "LEFT JOIN ao ON ao.id = f.accounting_object_id"
	e.aoCode, e.aoName = "ifNull(ao.code, '')", "ifNull(ao.name, '')"
	e.expenseCode = dictExpr("dict_expense_item", "expense_item_code", "f.expense_item_id")
	e.expenseNm = dictExpr("dict_expense_item", "expense_item_name", "f.expense_item_id")
	e.budgetCode = dictExpr("dict_budget_item", "budget_item_code", "f.budget_item_id")
	e.budgetNm = dictExpr("dict_budget_item", "budget_item_name", "f.budget_item_id")
	e.orgCode = dictExpr("dict_eb_organization_unit", "organization_unit_code", "f.department_id")
	e.orgNm = dictExpr("dict_eb_organization_unit", "organization_unit_name", "f.department_id")
	e.costSetCode = dictExpr("dict_cost_set", "cost_set_code", "f.cost_set_id")
	e.costSetNm = dictExpr("dict_cost_set", "cost_set_name", "f.cost_set_id")
	e.contract = dictExpr("dict_em_contract", "no", "f.contract_id")
	e.statCode = dictExpr("dict_statistics_code", "statistics_code", "f.statistics_code_id")
	e.statNm = dictExpr("dict_statistics_code", "statistics_code_name", "f.statistics_code_id")
	for i := range e.cf {
		e.cf[i] = fmt.Sprintf("ifNull(f.custom_field%d, '')", i+1)
		e.cfd[i] = fmt.Sprintf("ifNull(f.custom_field_detail%d, '')", i+1)
	}
	return e
}

func buildDetailQuery(
	companyIDs []string, p QueryParams, typeLedger int, noCol string, useStaging bool,
) string {
	return buildQuery(companyIDs, p, typeLedger, noCol, useStaging, true)
}

// buildCountQuery đếm số dòng mà truy vấn chi tiết sẽ trả — bọc CHÍNH truy vấn đó
// (bỏ ORDER BY/SETTINGS) để không bao giờ lệch bộ lọc với dòng thật.
func buildCountQuery(
	companyIDs []string, p QueryParams, typeLedger int, noCol string, useStaging bool,
) string {
	return "SELECT count() FROM (\n" +
		buildQuery(companyIDs, p, typeLedger, noCol, useStaging, false) +
		"\n) " + querySettings
}

func buildQuery(
	companyIDs []string, p QueryParams, typeLedger int, noCol string,
	useStaging bool, ordered bool,
) string {
	e := buildExprs(p)

	sql, orderBy := detailTemplate, orderByDetail
	if p.GroupSameItem == 1 {
		sql, orderBy = groupedTemplate, orderByGrouped
	}
	settings := querySettings
	if !ordered {
		orderBy, settings = "", ""
	}

	// Thay các khối dài (có thể chứa placeholder con) TRƯỚC, rồi mới đến
	// placeholder chung ở applyCommonPlaceholders.
	for k, v := range map[string]string{
		"{{WITH_CLAUSE}}":          e.withClause,
		"{{AO_JOIN}}":              e.aoJoin,
		"{{CS_JOIN}}":              e.csJoin,
		"{{EXTRA_GROUP_BY}}":       e.extraGroupBy,
		"{{ORDER_BY}}":             orderBy,
		"{{QUERY_SETTINGS}}":       settings,
		"{{NO_COL}}":               noCol,
		"{{INVOICE_NO_EXPR}}":      e.invoiceNo,
		"{{INVOICE_SERIES_EXPR}}":  e.invoiceSeries,
		"{{INVOICE_DATE_EXPR}}":    e.invoiceDate,
		"{{AO_CODE_EXPR}}":         e.aoCode,
		"{{AO_NAME_EXPR}}":         e.aoName,
		"{{EXPENSE_CODE_EXPR}}":    e.expenseCode,
		"{{EXPENSE_NAME_EXPR}}":    e.expenseNm,
		"{{BUDGET_CODE_EXPR}}":     e.budgetCode,
		"{{BUDGET_NAME_EXPR}}":     e.budgetNm,
		"{{ORG_CODE_EXPR}}":        e.orgCode,
		"{{ORG_NAME_EXPR}}":        e.orgNm,
		"{{COST_SET_CODE_EXPR}}":   e.costSetCode,
		"{{COST_SET_NAME_EXPR}}":   e.costSetNm,
		"{{CONTRACT_EXPR}}":        e.contract,
		"{{STATISTICS_CODE_EXPR}}": e.statCode,
		"{{STATISTICS_NAME_EXPR}}": e.statNm,
	} {
		sql = strings.ReplaceAll(sql, k, v)
	}
	for i := 0; i < 5; i++ {
		sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CF%d}}", i+1), e.cf[i])
		sql = strings.ReplaceAll(sql, fmt.Sprintf("{{CFD%d}}", i+1), e.cfd[i])
	}
	return applyCommonPlaceholders(sql, companyIDs, p, typeLedger, useStaging, true)
}

func buildOpeningQuery(
	companyIDs []string, p QueryParams, typeLedger int, useStaging bool,
) string {
	return applyCommonPlaceholders(openingTemplate, companyIDs, p, typeLedger, useStaging, false)
}

// applyCommonPlaceholders điền các placeholder dùng chung cho cả ba truy vấn.
// {{GL_SOURCE}} phải được thay TRƯỚC vì staging_source.sql cũng chứa placeholder.
func applyCommonPlaceholders(
	sql string, companyIDs []string, p QueryParams, typeLedger int,
	useStaging bool, inRange bool,
) string {
	glSource := factSourceExpr
	if useStaging {
		glSource = stagingSourceExpr
	}
	sql = strings.ReplaceAll(sql, "{{GL_SOURCE}}", glSource)

	// Cận ngày của nhánh staging: đẩy CHÍNH vị từ ngày của query vào truy vấn con
	// trước JOIN để cắt phân vùng. Truy vấn chi tiết lấy [FROM, TO]; số lũy kế lấy
	// mọi thứ TRƯỚC FROM (và không bao giờ trước 1990 — xem query_opening.sql).
	bound := ""
	if useStaging {
		if inRange {
			bound = "AND toDate(gl.PostedDate) >= toDate('" + p.FromDate + "') " +
				"AND toDate(gl.PostedDate) <= toDate('" + p.ToDate + "')"
		} else {
			bound = "AND toDate(gl.PostedDate) >= toDate('1990-01-01') " +
				"AND toDate(gl.PostedDate) < toDate('" + p.FromDate + "')"
		}
	}
	sql = strings.ReplaceAll(sql, "{{STAGING_DATE_BOUND}}", bound)

	sql = strings.ReplaceAll(sql, "{{COMPANY_IDS}}", quoteJoin(companyIDs))
	sql = strings.ReplaceAll(sql, "{{FROM_DATE}}", p.FromDate)
	sql = strings.ReplaceAll(sql, "{{TO_DATE}}", p.ToDate)
	sql = strings.ReplaceAll(sql, "{{TYPE_LEDGER}}", fmt.Sprintf("%d", typeLedger))
	sql = strings.ReplaceAll(sql, "{{CLUSTER_FILTER}}", clusterFilter(p.ClusterID))
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

func (s *Service) loadAccum(ctx context.Context, sql string, hasJoin bool) (accum, error) {
	var a accum
	err := withJoinFallback(ctx, hasJoin, func(c context.Context) error {
		a = accum{}
		rows, err := s.conn.Query(c, sql)
		if err != nil {
			return err
		}
		defer rows.Close()
		// Aggregate không GROUP BY luôn trả đúng một dòng (0 nếu không có dữ liệu).
		if rows.Next() {
			if err := rows.Scan(&a.Debit, &a.Credit, &a.DebitOrig, &a.CreditOrig); err != nil {
				return err
			}
		}
		return rows.Err()
	})
	return a, err
}

type countResult struct {
	n   int64
	err error
}

func (s *Service) countRows(ctx context.Context, sql string, hasJoin bool) (int64, error) {
	var n uint64
	err := withJoinFallback(ctx, hasJoin, func(c context.Context) error {
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

// detailResult: kết quả khởi chạy truy vấn chi tiết ở goroutine riêng.
type detailResult struct {
	rows *primedRows
	err  error
}

// drainDetail đợi truy vấn chi tiết (đã bị hủy context) trả về rồi đóng rows,
// tránh rò kết nối khi thoát sớm.
func drainDetail(ch <-chan detailResult) {
	if r := <-ch; r.rows != nil {
		_ = r.rows.Close()
	}
}

// ── TỐI ƯU JOIN ──────────────────────────────────────────────────────────────
//
// Cùng cơ chế với so-chi-tiet-cac-tai-khoan (xem queryCtx/loadOpeningBalances ở đó, có
// số đo trên prod 14/10/2026): thuật toán JOIN mặc định của ClickHouse dựng cả hash table
// trong RAM và vỡ (MEMORY_LIMIT_EXCEEDED) với công ty lớn.
//
//   - Thử parallel_hash trước: nhanh hơn grace_hash ~3 lần nhưng KHÔNG spill ra đĩa,
//     vượt max_bytes_in_join thì lỗi.
//   - Gặp lỗi bộ nhớ/giới hạn JOIN thì chạy lại ĐÚNG câu đó bằng grace_hash (chia bucket
//     và spill ra đĩa — chậm hơn nhưng không bao giờ vỡ).
//   - allow_statistics_optimize = 0: nạp thống kê part để tối ưu thứ tự JOIN tốn ~1,5 s
//     mỗi truy vấn trên bảng nhiều part, bất kể lượng dữ liệu.
//
// Chỉ đổi cách thực thi, KHÔNG đổi kết quả. Truy vấn không JOIN (mẫu mặc định trên nhánh
// fact) giữ nguyên, không đặt setting nào.
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
// hasJoin = false thì chạy thẳng, không đặt setting.
func withJoinFallback(ctx context.Context, hasJoin bool, fn func(context.Context) error) error {
	if !hasJoin {
		return fn(ctx)
	}
	err := fn(joinSettings(ctx, "parallel_hash", joinMaxBytesFast))
	if err != nil && isJoinMemoryError(err) {
		log.Printf("so-nhat-ky-chung: parallel_hash lỗi bộ nhớ (%v), chạy lại bằng grace_hash", err)
		return fn(joinSettings(ctx, "grace_hash", joinMaxBytesSafe))
	}
	return err
}

// primedRows bọc driver.Rows đã đọc sẵn khối đầu tiên. Với truy vấn có JOIN, lỗi vỡ bộ
// nhớ chỉ lộ ra ở khối đầu tiên (lúc dựng hash table), SAU khi Query() đã trả về nên
// không bắt được bằng err của Query(): phải đọc thử một dòng rồi mới biết có cần chạy lại.
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

// openRows mở truy vấn chi tiết dạng stream, có fallback như withJoinFallback. Sau khi
// thành công, rows đã được đọc trước dòng đầu tiên (nếu có) nhưng người gọi vẫn dùng
// Next()/Scan() bình thường.
func (s *Service) openRows(ctx context.Context, sql string, hasJoin bool) (*primedRows, error) {
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
	if !hasJoin {
		return open(ctx)
	}
	p, err := open(joinSettings(ctx, "parallel_hash", joinMaxBytesFast))
	if err != nil && isJoinMemoryError(err) {
		log.Printf("so-nhat-ky-chung: parallel_hash lỗi bộ nhớ (%v), chạy lại bằng grace_hash", err)
		return open(joinSettings(ctx, "grace_hash", joinMaxBytesSafe))
	}
	return p, err
}

// =============================================================================
// SCAN + VALIDATE + HELPERS
// =============================================================================

// scanRow đọc một dòng theo ĐÚNG thứ tự cột của query_detail.sql / query_grouped.sql.
func scanRow(rows driverRows) (Row, error) {
	var (
		referenceID *uuid.UUID
		typeID      *int32
		postedDate  time.Time
		voucherDate *time.Time
		no          string
		invoiceNo   string
		invoiceSer  string
		invoiceDate *time.Time
		description string
		reason      string
		account     string
		corr        string

		exchangeRate decimal.Decimal
		debitNT      decimal.Decimal
		debit        decimal.Decimal
		creditNT     decimal.Decimal
		credit       decimal.Decimal

		aoCode, aoName     string
		expCode, expName   string
		budCode, budName   string
		orgCode, orgName   string
		csCode, csName     string
		contract           string
		statCode, statName string

		cf, cfd [5]string

		unreasonable  string
		sortOrder     int32
		orderPriority int32
	)

	if err := rows.Scan(
		&referenceID, &typeID, &postedDate, &voucherDate, &no,
		&invoiceNo, &invoiceSer, &invoiceDate,
		&description, &reason, &account, &corr,
		&exchangeRate, &debitNT, &debit, &creditNT, &credit,
		&aoCode, &aoName, &expCode, &expName, &budCode, &budName,
		&orgCode, &orgName, &csCode, &csName, &contract, &statCode, &statName,
		&cf[0], &cf[1], &cf[2], &cf[3], &cf[4],
		&cfd[0], &cfd[1], &cfd[2], &cfd[3], &cfd[4],
		&unreasonable, &sortOrder, &orderPriority,
	); err != nil {
		return Row{}, err
	}

	r := Row{
		OrderType:   OrderTypeDetail,
		ReferenceID: uuidOrEmpty(referenceID),
		TypeID:      int32OrZero(typeID),
		PostedDate:  postedDate,
		No:          no,

		InvoiceNo:     invoiceNo,
		InvoiceSeries: invoiceSer,

		Description:                description,
		Reason:                     reason,
		AccountNumber:              account,
		CorrespondingAccountNumber: corr,

		ExchangeRate:   D(exchangeRate),
		DebitAmountNT:  D(debitNT),
		DebitAmount:    D(debit),
		CreditAmountNT: D(creditNT),
		CreditAmount:   D(credit),

		AccountingObjectCode: aoCode,
		AccountingObjectName: aoName,
		ExpenseItemCode:      expCode,
		ExpenseItemName:      expName,
		BudgetItemCode:       budCode,
		BudgetItemName:       budName,
		OrganizationUnitCode: orgCode,
		OrganizationUnitName: orgName,
		CostSetCode:          csCode,
		CostSetName:          csName,
		EMContract:           contract,
		StatisticsCode:       statCode,
		StatisticsCodeName:   statName,

		CustomField:       cf,
		CustomFieldDetail: cfd,

		IsUnreasonableCost: unreasonable,
		SortOrder:          sortOrder,
		OrderPriority:      orderPriority,
	}
	if voucherDate != nil {
		r.Date = *voucherDate
	}
	if invoiceDate != nil {
		r.InvoiceDate = *invoiceDate
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
	if p.ClusterID != "" && !clusterPattern.MatchString(p.ClusterID) {
		return fmt.Errorf("clusterId không hợp lệ: %q", p.ClusterID)
	}
	for name, v := range map[string]int{
		"isFinancialBook": p.IsFinancialBook,
		"isDependent":     p.IsDependent,
		"groupSameItem":   p.GroupSameItem,
		"showAccumAmount": p.ShowAccumAmount,
	} {
		if v != 0 && v != 1 {
			return errors.New(name + " phải là 0 hoặc 1")
		}
	}
	return nil
}

func clusterFilter(clusterID string) string {
	if clusterID == "" {
		return ""
	}
	// clusterID đã qua clusterPattern trong validateParams.
	return fmt.Sprintf("AND f.cluster_id = '%s'", clusterID)
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
// khác chuẩn RFC4122. company_id đã được PHÍA JAVA swap trước khi gửi vào nên
// WHERE khớp đúng cột UUID trong ClickHouse; nhưng reference_id ĐỌC RA chưa ai
// swap lại, nếu trả thẳng thì reflink ở frontend trỏ sai chứng từ. Hàm tự nghịch
// đảo. Xem so-chi-tiet-cac-tai-khoan.sqlServerUUIDSwap.
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
