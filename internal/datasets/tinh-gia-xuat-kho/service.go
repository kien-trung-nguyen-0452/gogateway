package tinh_gia_xuat_kho

import (
	"context"
	_ "embed"
	"fmt"
	"log"
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

// GetTinhGiaXuatKho đọc dữ liệu thô phục vụ tính giá xuất kho, trả thẳng danh sách dòng sổ kho.
func (s *Service) GetTinhGiaXuatKho(ctx context.Context, p RequestBody) ([]Row, error) {
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

	// Khởi tạo slice rỗng để JSON encode ra [] thay vì null khi không có dòng nào
	result := make([]Row, 0)
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

	log.Printf("repository-ledger: %d dòng, %d ms", len(result), time.Since(start).Milliseconds())
	return result, nil
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
- isOpeningStock: 	col[0]  Bool — true là tồn đầu kỳ, false là phát sinh trong kỳ.
- materialGoodsID: 	col[1]  UUID — mã VTHH.
- repositoryID: 	col[2]  UUID — mã kho.
- detailID: 		col[3]  Nullable(UUID) — dòng tồn đầu kỳ để NULL vì không thuộc chứng từ nào.
- referenceID: 		col[4]  Nullable(UUID) — như trên.
- typeID: 			col[5]  Nullable(Int32) — như trên.
- postedDate: 		col[6]  Nullable(DateTime) — như trên.
- mainIWQuantity: 	col[7]  Decimal128(10) — số lượng nhập theo đơn vị chính.
- mainOWQuantity: 	col[8]  Decimal128(10) — số lượng xuất theo đơn vị chính.
- iwAmount: 		col[9]  Decimal128(10) — giá trị nhập.
- owAmount: 		col[10] Decimal128(10) — giá trị xuất.

Bốn cột Nullable quy về giá trị rỗng để hợp đồng JSON với ebinventory không đổi. Bên đó chỉ đọc
số lượng và trị giá của dòng tồn đầu kỳ, bốn cột này không được dùng tới.
*/
func scanRow(rows driverRows) (Row, error) {
	var (
		isOpeningStock  bool
		materialGoodsID uuid.UUID
		repositoryID    uuid.UUID
		detailID        *uuid.UUID
		referenceID     *uuid.UUID
		typeID          *int32
		postedDate      *time.Time
		mainIWQuantity  decimal.Decimal
		mainOWQuantity  decimal.Decimal
		iwAmount        decimal.Decimal
		owAmount        decimal.Decimal
	)

	if err := rows.Scan(
		&isOpeningStock,
		&materialGoodsID, &repositoryID,
		&detailID, &referenceID, &typeID, &postedDate,
		&mainIWQuantity, &mainOWQuantity, &iwAmount, &owAmount,
	); err != nil {
		return Row{}, fmt.Errorf("scan error: %w", err)
	}

	return Row{
		IsOpeningStock:  isOpeningStock,
		MaterialGoodsID: materialGoodsID.String(),
		RepositoryID:    repositoryID.String(),
		DetailID:        uuidOrEmpty(detailID),
		ReferenceID:     uuidOrEmpty(referenceID),
		TypeID:          int32OrZero(typeID),
		PostedDate:      timeOrEpoch(postedDate),
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

// uuidOrEmpty đổi UUID có thể NULL thành chuỗi, NULL trả về UUID toàn số 0.
func uuidOrEmpty(id *uuid.UUID) string {
	if id == nil {
		return uuid.Nil.String()
	}
	return id.String()
}

// int32OrZero đổi Int32 có thể NULL thành số, NULL trả về 0.
func int32OrZero(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}

// timeOrEpoch đổi DateTime có thể NULL thành thời điểm, NULL trả về mốc 1970-01-01 giống
// giá trị dòng tồn đầu kỳ vẫn dùng trước đây, để hợp đồng JSON với ebinventory không đổi.
func timeOrEpoch(value *time.Time) time.Time {
	if value == nil {
		return time.Unix(0, 0).UTC()
	}
	return *value
}
