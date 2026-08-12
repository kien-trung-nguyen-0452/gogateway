package so_chi_tiet_tai_khoan

import (
	"fmt"
	"time"

	"softdream.vn/go-gateway/internal/so-chi-tiet-cac-tai-khoan/pb"
)

// GRPCServer implement pb.SoChiTietTaiKhoanServiceServer.
type GRPCServer struct {
	pb.UnimplementedSoChiTietTaiKhoanServiceServer
	service *Service
}

func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

// GetSoChiTietTaiKhoan — RPC streaming. Mỗi dòng từ callback được chuyển thẳng
// thành proto message và gửi qua stream.Send() ngay lập tức, không bao giờ giữ
// cả lô trong RAM.
func (g *GRPCServer) GetSoChiTietTaiKhoan(
	req *pb.SoChiTietTaiKhoanRequest,
	stream pb.SoChiTietTaiKhoanService_GetSoChiTietTaiKhoanServer,
) error {
	params := QueryParams{
		CompanyIDs:       req.GetCompanyIds(),
		PrimaryCompanyID: req.GetPrimaryCompanyId(),
		AccountNumbers:   req.GetAccountNumbers(),
		FromDate:         req.GetFromDate(),
		ToDate:           req.GetToDate(),
		CurrencyID:       req.GetCurrencyId(),
		IsFinancialBook:  int(req.GetIsFinancialBook()),
		IsDependent:      int(req.GetIsDependent()),
		GroupSameItem:    int(req.GetGroupSameItem()),
		IsParentNode:     req.GetIsParentNode(),
		ClusterID:        req.GetClusterId(),
	}

	_, err := g.service.StreamSoChiTiet(stream.Context(), params, func(row Row) error {
		return stream.Send(rowToProto(row))
	})
	if err != nil {
		return fmt.Errorf("loi stream so chi tiet tai khoan: %w", err)
	}
	return nil
}

// rowToProto xuất số liệu DẠNG STRING qua gRPC.
//
// Lưu ý khác biệt giữa hai đường ra:
//   - gRPC (đường này): string, vì Java nhận rồi dựng BigDecimal — giữ trọn
//     độ chính xác Decimal(25,10).
//   - HTTP NDJSON (handler.go): số JSON, vì frontend dùng pipe ebcurrency.
//
// Hai đường phục vụ hai consumer khác nhau nên định dạng khác nhau là đúng,
// không phải thiếu nhất quán.
func rowToProto(row Row) *pb.SoChiTietTaiKhoanRow {
	return &pb.SoChiTietTaiKhoanRow{
		OrderType:           row.OrderType,
		OrderNumber:         row.OrderNumber,
		Bold:                row.Bold,
		AccountNumber:       row.AccountNumber,
		AccountCategoryKind: row.AccountCategoryKind,

		AccountNameWithAccountNumber: row.AccountNameWithAccountNumber,

		KeyId:       row.KeyID,
		ReferenceId: row.ReferenceID,
		TypeId:      row.TypeID,

		PostedDate:  formatDate(row.PostedDate),
		InvoiceDate: formatDate(row.InvoiceDate),
		InvoiceNo:   row.InvoiceNo,
		No:          row.No,

		Reason:               row.Reason,
		JournalMemo:          row.JournalMemo,
		AccountCorresponding: row.AccountCorresponding,

		DebitAmount:          row.DebitAmount.String(),
		CreditAmount:         row.CreditAmount.String(),
		DebitAmountOriginal:  row.DebitAmountOriginal.String(),
		CreditAmountOriginal: row.CreditAmountOriginal.String(),

		ClosingDebitAmount:          row.ClosingDebitAmount.String(),
		ClosingCreditAmount:         row.ClosingCreditAmount.String(),
		ClosingDebitAmountOriginal:  row.ClosingDebitAmountOriginal.String(),
		ClosingCreditAmountOriginal: row.ClosingCreditAmountOriginal.String(),

		ExchangeRate:       row.ExchangeRate.String(),
		OrderPriority:      row.OrderPriority,
		IsUnreasonableCost: row.IsUnreasonableCost,

		AccountingObjectCode: row.AccountingObjectCode,
		AccountingObjectName: row.AccountingObjectName,

		CustomField:       row.CustomField[:],
		CustomFieldDetail: row.CustomFieldDetail[:],
	}
}

func formatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02")
}
