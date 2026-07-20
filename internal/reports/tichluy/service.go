package tichluy

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

// Chi chap nhan dung dinh dang UUID chuan - BAT BUOC vi query duoc build
// bang string-replace (khong phai bind param), day la lop chan SQL injection.
var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

const (
	minYear = 2000
	maxYear = 2100
)

// Service chua business logic thuan tuy, KHONG phu thuoc vao transport
// (HTTP/gRPC). Day la diem then chot de sau nay migrate sang gRPC theo
// Strangler Fig: chi can viet them 1 grpc handler goi lai dung Service nay,
// khong phai sua logic ben trong.
// day la test thu
type Service struct {
	conn clickhouse.Conn
}

func NewService(conn clickhouse.Conn) *Service {
	return &Service{conn: conn}
}

func (s *Service) GetTichLuy(ctx context.Context, year int, companyIDs []string) ([]Row, int64, error) {
	if err := validateYear(year); err != nil {
		return nil, 0, err
	}
	if err := validateCompanyIDs(companyIDs); err != nil {
		return nil, 0, err
	}

	quoted := make([]string, len(companyIDs))
	for i, id := range companyIDs {
		quoted[i] = fmt.Sprintf("'%s'", id)
	}

	sql := strings.ReplaceAll(queryTemplate, "{{YEAR}}", fmt.Sprintf("%d", year))
	sql = strings.ReplaceAll(sql, "{{COMPANY_IDS}}", strings.Join(quoted, ", "))

	start := time.Now()

	rows, err := s.conn.Query(ctx, sql)
	if err != nil {
		return nil, 0, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	var result []Row
	for rows.Next() {
		var (
			repositoryID    uuid.UUID
			materialGoodsID uuid.UUID
			qty             decimal.Decimal
			amt             decimal.Decimal
			rowType         int32
			toDate          time.Time
			fromDate        time.Time
			typeLedger      int32
		)

		if err := rows.Scan(
			&repositoryID, &materialGoodsID,
			&qty, &amt,
			&rowType, &toDate, &fromDate, &typeLedger,
		); err != nil {
			return nil, 0, fmt.Errorf("scan error: %w", err)
		}

		result = append(result, Row{
			RepositoryID:       repositoryID.String(),
			MaterialGoodsID:    materialGoodsID.String(),
			CumulativeQuantity: qty,
			CumulativeAmount:   amt,
			Type:               rowType,
			ToDate:             toDate,
			FromDate:           fromDate,
			TypeLedger:         typeLedger,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("row iteration error: %w", err)
	}

	elapsedMs := time.Since(start).Milliseconds()
	return result, elapsedMs, nil
}

func validateYear(year int) error {
	if year < minYear || year > maxYear {
		return fmt.Errorf("year khong hop le: %d", year)
	}
	return nil
}

func validateCompanyIDs(ids []string) error {
	if len(ids) == 0 {
		return fmt.Errorf("companyIds khong duoc rong")
	}
	for _, id := range ids {
		if !uuidPattern.MatchString(id) {
			return fmt.Errorf("companyId khong dung dinh dang UUID: %s", id)
		}
	}
	return nil
}
