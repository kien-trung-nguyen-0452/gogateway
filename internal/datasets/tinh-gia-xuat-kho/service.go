package tinh_gia_xuat_kho

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

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// dateTimePattern khớp "2026-08-01" hoặc "2026-08-01 00:00:00".
// Câu SQL được dựng bằng cách thay chuỗi, nên phải chặn định dạng ở đây
// để không bị chèn lệnh.
var dateTimePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$`)

func NewService(conn clickhouse.Conn) *Service {
	return &Service{conn: conn}
}

// GetTinhGiaXuatKho đọc dữ liệu thô phục vụ tính giá xuất kho.
//
// Trả về danh sách dòng theo đúng thứ tự ClickHouse sắp xếp, kèm thời gian
// chạy query tính bằng mili giây.
func (s *Service) GetTinhGiaXuatKho(ctx context.Context, p QueryParams) ([]Row, int64, error) {
	if err := validateParams(p); err != nil {
		return nil, 0, err
	}

	sql := buildQuery(p)

	start := time.Now()

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, 0, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var result []Row
	for rows.Next() {
		row, err := scanRow(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan error: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("row iteration error: %w", err)
	}

	return result, time.Since(start).Milliseconds(), nil
}

// buildQuery thay tham số vào query.sql.
//
// Hai bộ lọc kho và VTHH để rỗng khi người dùng không chọn, khi đó câu SQL
// không có điều kiện tương ứng nghĩa là lấy tất cả.
func buildQuery(p QueryParams) string {
	repoFilter := ""
	if len(p.RepositoryIDs) > 0 {
		repoFilter = fmt.Sprintf(
			"AND RepositoryID IN CAST([%s] AS Array(UUID))",
			quoteJoin(p.RepositoryIDs),
		)
	}

	materialFilter := ""
	if len(p.MaterialGoodsIDs) > 0 {
		materialFilter = fmt.Sprintf(
			"AND MaterialGoodsID IN CAST([%s] AS Array(UUID))",
			quoteJoin(p.MaterialGoodsIDs),
		)
	}

	sql := queryTemplate
	sql = strings.ReplaceAll(sql, "{{COMPANY_ID}}", p.CompanyID)
	sql = strings.ReplaceAll(sql, "{{TYPE_LEDGER}}", fmt.Sprintf("%d", p.TypeLedger))
	sql = strings.ReplaceAll(sql, "{{FROM_DATE}}", normalizeDateTime(p.FromDate, "00:00:00"))
	sql = strings.ReplaceAll(sql, "{{TO_DATE}}", normalizeDateTime(p.ToDate, "23:59:59"))
	sql = strings.ReplaceAll(sql, "{{REPOSITORY_FILTER}}", repoFilter)
	sql = strings.ReplaceAll(sql, "{{MATERIAL_GOODS_FILTER}}", materialFilter)
	return sql
}

// scanRow đọc một dòng kết quả ClickHouse thành Row.
//
// Thứ tự tham số phải khớp đúng thứ tự cột trong query.sql.
func scanRow(rows driverRows) (Row, error) {
	var (
		// col[0]  UInt8 — 0 là tồn đầu kỳ, 1 là phát sinh trong kỳ
		rowKind uint8
		// col[1]  UUID — mã VTHH
		materialGoodsID uuid.UUID
		// col[2]  UUID — mã kho
		repositoryID uuid.UUID
		// col[3]  UUID — query đã ifNull nên không còn Nullable
		detailID uuid.UUID
		// col[4]  UUID — ID chứng từ gốc
		referenceID uuid.UUID
		// col[5]  Int32 — query đã ifNull
		typeID int32
		// col[6]  DateTime — ngày hạch toán
		postedDate time.Time
		// col[7]  Decimal128(10) — số lượng nhập theo đơn vị chính
		mainIWQuantity decimal.Decimal
		// col[8]  Decimal128(10) — số lượng xuất theo đơn vị chính
		mainOWQuantity decimal.Decimal
		// col[9]  Decimal128(10) — giá trị nhập
		iwAmount decimal.Decimal
		// col[10] Decimal128(10) — giá trị xuất
		owAmount decimal.Decimal
	)

	if err := rows.Scan(
		&rowKind,
		&materialGoodsID, &repositoryID,
		&detailID, &referenceID, &typeID, &postedDate,
		&mainIWQuantity, &mainOWQuantity, &iwAmount, &owAmount,
	); err != nil {
		return Row{}, fmt.Errorf("scan error: %w", err)
	}

	return Row{
		RowKind:         int8(rowKind),
		MaterialGoodsID: materialGoodsID.String(),
		RepositoryID:    repositoryID.String(),
		DetailID:        detailID.String(),
		ReferenceID:     referenceID.String(),
		TypeID:          typeID,
		PostedDate:      postedDate,
		MainIWQuantity:  mainIWQuantity,
		MainOWQuantity:  mainOWQuantity,
		IWAmount:        iwAmount,
		OWAmount:        owAmount,
	}, nil
}

// ── Hàm phụ trợ ─────────────────────────────────────────────────────────────

// normalizeDateTime cho phép truyền "2026-08-01" và tự bù phần giờ.
// FromDate bù 00:00:00, ToDate bù 23:59:59 để không sót chứng từ cuối kỳ.
func normalizeDateTime(v, defaultTime string) string {
	if len(v) == 10 {
		return v + " " + defaultTime
	}
	return v
}

// validateParams kiểm tra tham số trước khi dựng câu SQL.
//
// Ngoài việc báo lỗi rõ ràng cho người gọi, đây còn là lớp chặn chèn lệnh SQL
// vì câu truy vấn được dựng bằng cách thay chuỗi.
func validateParams(p QueryParams) error {
	if p.CompanyID == "" {
		return fmt.Errorf("companyId không được rỗng")
	}
	if p.TypeLedger != 0 && p.TypeLedger != 1 {
		return fmt.Errorf("typeLedger phải là 0 (sổ tài chính) hoặc 1 (sổ quản trị)")
	}
	if p.FromDate == "" || p.ToDate == "" {
		return fmt.Errorf("fromDate và toDate không được rỗng")
	}
	if !dateTimePattern.MatchString(p.FromDate) {
		return fmt.Errorf("fromDate sai định dạng, cần 'YYYY-MM-DD' hoặc 'YYYY-MM-DD HH:MM:SS': %s", p.FromDate)
	}
	if !dateTimePattern.MatchString(p.ToDate) {
		return fmt.Errorf("toDate sai định dạng, cần 'YYYY-MM-DD' hoặc 'YYYY-MM-DD HH:MM:SS': %s", p.ToDate)
	}
	if normalizeDateTime(p.FromDate, "00:00:00") > normalizeDateTime(p.ToDate, "23:59:59") {
		return fmt.Errorf("fromDate phải nhỏ hơn hoặc bằng toDate")
	}

	allIDs := []string{p.CompanyID}
	allIDs = append(allIDs, p.RepositoryIDs...)
	allIDs = append(allIDs, p.MaterialGoodsIDs...)
	for _, id := range allIDs {
		if !uuidPattern.MatchString(id) {
			return fmt.Errorf("ID không đúng định dạng UUID: %s", id)
		}
	}
	return nil
}

// quoteJoin nối danh sách ID thành chuỗi dạng 'a', 'b', 'c' để đưa vào mệnh
// đề IN của ClickHouse.
func quoteJoin(ids []string) string {
	if len(ids) == 0 {
		return ""
	}
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = fmt.Sprintf("'%s'", id)
	}
	return strings.Join(quoted, ", ")
}
