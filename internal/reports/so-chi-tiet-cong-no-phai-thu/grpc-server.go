package so_chi_tiet_cong_no_phai_thu

import (
	"fmt"
	"time"

	"google.golang.org/grpc"

	"softdream.vn/go-gateway/internal/grpcbatch"
	"softdream.vn/go-gateway/internal/so-chi-tiet-cong-no-phai-thu/pb"
)

// GRPCServer implement pb.SoChiTietCongNoPhaiThuServiceServer.
type GRPCServer struct {
	pb.UnimplementedSoChiTietCongNoPhaiThuServiceServer
	service *Service
}

func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

// GetSoChiTietCongNoPhaiThu — RPC streaming từng dòng. Mỗi dòng từ callback được chuyển
// thẳng thành proto message và gửi qua stream.Send() ngay lập tức, không bao giờ
// giữ cả lô trong RAM.
func (g *GRPCServer) GetSoChiTietCongNoPhaiThu(
	req *pb.SoChiTietCongNoPhaiThuRequest,
	stream pb.SoChiTietCongNoPhaiThuService_GetSoChiTietCongNoPhaiThuServer,
) error {
	// Message đầu tiên chỉ mang total_rows (header) để client hiện "x/y dòng"
	// ngay từ đầu — xem ghi chú field total_rows trong proto.
	onTotal := func(total int64) error {
		return stream.Send(&pb.SoChiTietCongNoPhaiThuRow{TotalRows: total})
	}
	_, err := g.service.StreamSoChiTietWithTotal(stream.Context(), paramsFromRequest(req), onTotal, func(row Row) error {
		return stream.Send(rowToProto(row))
	})
	if err != nil {
		return fmt.Errorf("lỗi stream sổ chi tiết công nợ phải thu: %w", err)
	}
	return nil
}

// GetSoChiTietCongNoPhaiThuBatch — cùng nội dung với GetSoChiTietCongNoPhaiThu nhưng gom
// dòng thành lô và nén zstd (nếu client hỗ trợ). Cỡ lô và công tắc nén do grpcbatch quản
// lý tập trung. Thứ tự dòng giữ nguyên; lô cuối được flush sau khi stream xong.
func (g *GRPCServer) GetSoChiTietCongNoPhaiThuBatch(
	req *pb.SoChiTietCongNoPhaiThuRequest,
	stream grpc.ServerStreamingServer[pb.SoChiTietCongNoPhaiThuBatch],
) error {
	b := grpcbatch.Begin(stream.Context(), func(rows []*pb.SoChiTietCongNoPhaiThuRow) error {
		return stream.Send(&pb.SoChiTietCongNoPhaiThuBatch{Rows: rows})
	})

	// Lô tổng số dòng đi trước mọi dòng dữ liệu (onTotal được gọi trước dòng
	// đầu tiên), nên không đụng vào lô đang gom.
	onTotal := func(total int64) error {
		return stream.Send(&pb.SoChiTietCongNoPhaiThuBatch{TotalRows: total})
	}
	_, err := g.service.StreamSoChiTietWithTotal(stream.Context(), paramsFromRequest(req), onTotal, func(row Row) error {
		return b.Add(rowToProto(row))
	})
	if err != nil {
		return fmt.Errorf("lỗi stream sổ chi tiết công nợ phải thu (batch): %w", err)
	}
	return b.Flush()
}

func paramsFromRequest(req *pb.SoChiTietCongNoPhaiThuRequest) QueryParams {
	return QueryParams{
		CompanyIDs:        req.GetCompanyIds(),
		PrimaryCompanyID:  req.GetPrimaryCompanyId(),
		FromDate:          req.GetFromDate(),
		ToDate:            req.GetToDate(),
		IsFinancialBook:   int(req.GetIsFinancialBook()),
		IsDependent:       int(req.GetIsDependent()),
		GroupSameItem:     int(req.GetGroupSameItem()),
		AccountNumber:     req.GetAccountNumber(),
		CurrencyID:        req.GetCurrencyId(),
		CheckAll:          int(req.GetCheckAll()),
		ObjectIDs:         req.GetObjectIds(),
		ObjectCode:        req.GetAccountingObjectCode(),
		ObjectName:        req.GetAccountingObjectName(),
		ObjectAddr:        req.GetAccountingObjectAddress(),
		GroupID:           req.GetAccountObjectGroupId(),
		GetAccountHasData: int(req.GetGetAccountHasData()),
		ClusterID:         req.GetClusterId(),
	}
}

// rowToProto xuất số liệu DẠNG STRING qua gRPC: Java nhận rồi dựng BigDecimal,
// giữ trọn độ chính xác Decimal(25,10). HTTP NDJSON (handler.go) thì xuất số JSON
// vì frontend dùng pipe ebcurrency — hai consumer khác nhau nên định dạng khác nhau.
func rowToProto(row Row) *pb.SoChiTietCongNoPhaiThuRow {
	return &pb.SoChiTietCongNoPhaiThuRow{
		OrderType: row.OrderType,

		AccountingObjectId:   row.AccountingObjectID,
		AccountingObjectCode: row.AccountingObjectCode,
		AccountingObjectName: row.AccountingObjectName,

		ReferenceId: row.ReferenceID,
		TypeId:      row.TypeID,

		PostedDate: formatDate(row.PostedDate),
		Date:       formatDate(row.Date),
		No:         row.No,

		Description: row.Description,

		AccountNumber:              row.AccountNumber,
		CorrespondingAccountNumber: row.CorrespondingAccountNumber,

		DebitAmountNt:  row.DebitAmountNT.String(),
		DebitAmount:    row.DebitAmount.String(),
		CreditAmountNt: row.CreditAmountNT.String(),
		CreditAmount:   row.CreditAmount.String(),

		ClosingDebitAmountNt:  row.ClosingDebitAmountNT.String(),
		ClosingDebitAmount:    row.ClosingDebitAmount.String(),
		ClosingCreditAmountNt: row.ClosingCreditAmountNT.String(),
		ClosingCreditAmount:   row.ClosingCreditAmount.String(),

		OrderPriority: row.OrderPriority,
	}
}

func formatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02")
}
