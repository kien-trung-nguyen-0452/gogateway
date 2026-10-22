package so_nhat_ky_chung

import (
	"fmt"
	"time"

	"google.golang.org/grpc"

	"softdream.vn/go-gateway/internal/grpcbatch"
	"softdream.vn/go-gateway/internal/so-nhat-ky-chung/pb"
)

// GRPCServer implement pb.SoNhatKyChungServiceServer.
type GRPCServer struct {
	pb.UnimplementedSoNhatKyChungServiceServer
	service *Service
}

func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

// GetSoNhatKyChung — RPC streaming từng dòng. Mỗi dòng từ callback được chuyển
// thẳng thành proto message và gửi qua stream.Send() ngay lập tức, không bao giờ
// giữ cả lô trong RAM.
func (g *GRPCServer) GetSoNhatKyChung(
	req *pb.SoNhatKyChungRequest,
	stream pb.SoNhatKyChungService_GetSoNhatKyChungServer,
) error {
	// Message đầu tiên chỉ mang total_rows (header) để client hiện "x/y dòng"
	// ngay từ đầu — xem ghi chú field total_rows trong proto.
	onTotal := func(total int64) error {
		return stream.Send(&pb.SoNhatKyChungRow{TotalRows: total})
	}
	_, err := g.service.StreamSoNhatKyChungWithTotal(stream.Context(), paramsFromRequest(req), onTotal, func(row Row) error {
		return stream.Send(rowToProto(row))
	})
	if err != nil {
		return fmt.Errorf("lỗi stream sổ nhật ký chung: %w", err)
	}
	return nil
}

// GetSoNhatKyChungBatch — cùng nội dung với GetSoNhatKyChung nhưng gom dòng thành
// lô và nén zstd (nếu client hỗ trợ). Cỡ lô và công tắc nén do grpcbatch quản lý
// tập trung. Thứ tự dòng giữ nguyên; lô cuối được flush sau khi stream xong.
func (g *GRPCServer) GetSoNhatKyChungBatch(
	req *pb.SoNhatKyChungRequest,
	stream grpc.ServerStreamingServer[pb.SoNhatKyChungBatch],
) error {
	b := grpcbatch.Begin(stream.Context(), func(rows []*pb.SoNhatKyChungRow) error {
		return stream.Send(&pb.SoNhatKyChungBatch{Rows: rows})
	})

	// Lô tổng số dòng đi trước mọi dòng dữ liệu (onTotal được gọi trước dòng
	// đầu tiên), nên không đụng vào lô đang gom.
	onTotal := func(total int64) error {
		return stream.Send(&pb.SoNhatKyChungBatch{TotalRows: total})
	}
	_, err := g.service.StreamSoNhatKyChungWithTotal(stream.Context(), paramsFromRequest(req), onTotal, func(row Row) error {
		return b.Add(rowToProto(row))
	})
	if err != nil {
		return fmt.Errorf("lỗi stream sổ nhật ký chung (batch): %w", err)
	}
	return b.Flush()
}

func paramsFromRequest(req *pb.SoNhatKyChungRequest) QueryParams {
	return QueryParams{
		CompanyIDs:       req.GetCompanyIds(),
		PrimaryCompanyID: req.GetPrimaryCompanyId(),
		FromDate:         req.GetFromDate(),
		ToDate:           req.GetToDate(),
		IsFinancialBook:  int(req.GetIsFinancialBook()),
		IsDependent:      int(req.GetIsDependent()),
		GroupSameItem:    int(req.GetGroupSameItem()),
		ShowAccumAmount:  int(req.GetIsShowAccumAmount()),
		IsDefaultType:    req.GetIsDefaultType(),
		ClusterID:        req.GetClusterId(),
	}
}

// rowToProto xuất số liệu DẠNG STRING qua gRPC: Java nhận rồi dựng BigDecimal,
// giữ trọn độ chính xác Decimal(25,10). HTTP NDJSON (handler.go) thì xuất số JSON
// vì frontend dùng pipe ebcurrency — hai consumer khác nhau nên định dạng khác nhau.
func rowToProto(row Row) *pb.SoNhatKyChungRow {
	return &pb.SoNhatKyChungRow{
		OrderType: row.OrderType,

		ReferenceId: row.ReferenceID,
		TypeId:      row.TypeID,

		PostedDate: formatDate(row.PostedDate),
		Date:       formatDate(row.Date),
		No:         row.No,

		InvoiceNo:     row.InvoiceNo,
		InvoiceSeries: row.InvoiceSeries,
		InvoiceDate:   formatDate(row.InvoiceDate),

		Description: row.Description,
		Reason:      row.Reason,

		AccountNumber:              row.AccountNumber,
		CorrespondingAccountNumber: row.CorrespondingAccountNumber,

		ExchangeRate:   row.ExchangeRate.String(),
		DebitAmountNt:  row.DebitAmountNT.String(),
		DebitAmount:    row.DebitAmount.String(),
		CreditAmountNt: row.CreditAmountNT.String(),
		CreditAmount:   row.CreditAmount.String(),

		AccountingObjectCode: row.AccountingObjectCode,
		AccountingObjectName: row.AccountingObjectName,
		ExpenseItemCode:      row.ExpenseItemCode,
		ExpenseItemName:      row.ExpenseItemName,
		BudgetItemCode:       row.BudgetItemCode,
		BudgetItemName:       row.BudgetItemName,
		OrganizationUnitCode: row.OrganizationUnitCode,
		OrganizationUnitName: row.OrganizationUnitName,
		CostSetCode:          row.CostSetCode,
		CostSetName:          row.CostSetName,
		EmContract:           row.EMContract,
		StatisticsCode:       row.StatisticsCode,
		StatisticsCodeName:   row.StatisticsCodeName,

		CustomField:       row.CustomField[:],
		CustomFieldDetail: row.CustomFieldDetail[:],

		IsUnreasonableCost: row.IsUnreasonableCost,

		SortOrder:     row.SortOrder,
		OrderPriority: row.OrderPriority,
	}
}

func formatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02")
}
