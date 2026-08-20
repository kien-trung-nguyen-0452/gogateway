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

// datePattern định dạng "2026-08-01".
var datePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)

// dateTimePattern định dạng "2026-08-01 00:00:00".
var dateTimePattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`)

func NewService(conn clickhouse.Conn) *Service {
	return &Service{conn: conn}
}

// GetTinhGiaXuatKho đọc dữ liệu thô phục vụ tính giá xuất kho.
func (s *Service) GetTinhGiaXuatKho(ctx context.Context, p RequestBody) (*Response, error) {
	if err := validateParams(p); err != nil {
		return nil, err
	}

	sql := buildQuery(p)

	start := time.Now()

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var result []Row
	for rows.Next() {
		row, err := scanRow(rows)
		if err != nil {
			return nil, fmt.Errorf("scan error: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	response := &Response{
		RowCount:  len(result),
		ElapsedMs: time.Since(start).Milliseconds(),
		Data:      result,
	}
	return response, nil
}

// buildQuery thay tham số vào query.sql.
func buildQuery(requestBody RequestBody) string {
	repositoryFilter := ""
	if len(requestBody.RepositoryIDs) > 0 {
		repositoryFilter = fmt.Sprintf(
			"AND RepositoryID IN CAST([%s] AS Array(UUID))",
			quoteJoin(requestBody.RepositoryIDs),
		)
	}

	materialFilter := ""
	if len(requestBody.MaterialGoodsIDs) > 0 {
		materialFilter = fmt.Sprintf(
			"AND MaterialGoodsID IN CAST([%s] AS Array(UUID))",
			quoteJoin(requestBody.MaterialGoodsIDs),
		)
	}

	sql := queryTemplate
	sql = strings.ReplaceAll(sql, "{{COMPANY_ID}}", requestBody.CompanyID)
	sql = strings.ReplaceAll(sql, "{{TYPE_LEDGER}}", fmt.Sprintf("%d", requestBody.TypeLedger))
	sql = strings.ReplaceAll(sql, "{{FROM_DATE}}", requestBody.FromDate)
	sql = strings.ReplaceAll(sql, "{{TO_DATE}}", requestBody.ToDate)
	sql = strings.ReplaceAll(sql, "{{REPOSITORY_FILTER}}", repositoryFilter)
	sql = strings.ReplaceAll(sql, "{{MATERIAL_GOODS_FILTER}}", materialFilter)
	return sql
}

/*
scanRow đọc một dòng kết quả ClickHouse thành Row.
- rowKind: 			col[0]  UInt8 — 0 là tồn đầu kỳ, 1 là phát sinh trong kỳ.
- materialGoodsID: 	col[1]  UUID — mã VTHH.
- repositoryID: 	col[2]  UUID — mã kho.
- detailID: 		col[3]  UUID — query đã ifNull nên không còn Nullable.
- referenceID: 		col[4]  UUID — ID chứng từ gốc.
- typeID: 			col[5]  Int32 — query đã ifNull.
- postedDate: 		col[6]  DateTime — ngày hạch toán.
- mainIWQuantity: 	col[7]  Decimal128(10) — số lượng nhập theo đơn vị chính.
- mainOWQuantity: 	col[8]  Decimal128(10) — số lượng xuất theo đơn vị chính.
- iwAmount: 		col[9]  Decimal128(10) — giá trị nhập.
- owAmount: 		col[10] Decimal128(10) — giá trị xuất.
*/
func scanRow(rows driverRows) (Row, error) {
	var (
		rowKind         uint8
		materialGoodsID uuid.UUID
		repositoryID    uuid.UUID
		detailID        uuid.UUID
		referenceID     uuid.UUID
		typeID          int32
		postedDate      time.Time
		mainIWQuantity  decimal.Decimal
		mainOWQuantity  decimal.Decimal
		iwAmount        decimal.Decimal
		owAmount        decimal.Decimal
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

// validateParams kiểm tra tham số trước khi dựng câu SQL.
func validateParams(requestBody RequestBody) error {
	if requestBody.CompanyID == "" {
		return fmt.Errorf("CompanyID null")
	}
	if requestBody.TypeLedger != 0 && requestBody.TypeLedger != 1 {
		return fmt.Errorf("TypeLedger = 0 or = 1")
	}
	if requestBody.FromDate == "" || requestBody.ToDate == "" {
		return fmt.Errorf("FromDate và ToDate null")
	}
	if !dateTimePattern.MatchString(requestBody.FromDate) {
		return fmt.Errorf("FromDate invalid format, 'YYYY-MM-DD HH:MM:SS': %s", requestBody.FromDate)
	}
	if !dateTimePattern.MatchString(requestBody.ToDate) {
		return fmt.Errorf("ToDate invalid format, 'YYYY-MM-DD HH:MM:SS': %s", requestBody.ToDate)
	}
	if requestBody.FromDate > requestBody.ToDate {
		return fmt.Errorf("FromDate must be <= ToDate")
	}

	allIDs := []string{requestBody.CompanyID}
	allIDs = append(allIDs, requestBody.RepositoryIDs...)
	allIDs = append(allIDs, requestBody.MaterialGoodsIDs...)
	for _, id := range allIDs {
		if !uuidPattern.MatchString(id) {
			return fmt.Errorf("ID invalid format UUID: %s", id)
		}
	}
	return nil
}

// quoteJoin nối danh sách ID thành chuỗi dạng 'a', 'b', 'c' để đưa vào mệnh đề IN của ClickHouse.
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
