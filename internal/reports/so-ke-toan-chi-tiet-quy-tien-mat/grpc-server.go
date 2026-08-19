package so_ke_toan_chi_tiet_quy_tien_mat

import (
	"fmt"
	"time"
)

import (
	"softdream.vn/go-gateway/internal/so_ke_toan_chi_tiet_quy_tien_mat/pb"
)

// GRPCServer implement pb.SoKeToanChiTietQuyTienMatServiceServer.
type GRPCServer struct {
	pb.UnimplementedSoKeToanChiTietQuyTienMatServiceServer
	service *Service
}

func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

// GetSoKeToanChiTietQuyTienMat — RPC streaming.
//
// Mỗi dòng từ callback được chuyển thẳng thành proto message và gửi qua
// stream.Send() ngay lập tức, không bao giờ giữ cả lô trong RAM.
//
// THỨ TỰ DÒNG LÀ MỘT PHẦN CỦA TÍNH ĐÚNG ĐẮN. Cột so_ton là số dư luỹ kế, tính
// bằng máy trạng thái chạy dọc theo thứ tự nhận được từ ClickHouse. Client
// TUYỆT ĐỐI không được sort lại hay xử lý song song.
func (g *GRPCServer) GetSoKeToanChiTietQuyTienMat(
	req *pb.SoKeToanChiTietQuyTienMatRequest,
	stream pb.SoKeToanChiTietQuyTienMatService_GetSoKeToanChiTietQuyTienMatServer,
) error {
	// ĐẢO CHIỀU: proc nhận thẳng @TypeLedger với 0 = sổ tài chính (NoFBook),
	// còn proto dùng is_financial_book với 1 = sổ tài chính.
	//
	// Chọn nhầm chiều là toàn bộ báo cáo lấy sai sổ mà không có lỗi nào báo ra
	// — cùng loại bẫy với currentBook bên Sổ chi tiết các tài khoản.
	typeLedger := 1
	if req.GetIsFinancialBook() == 1 {
		typeLedger = 0
	}

	params := QueryParams{
		CompanyIDs:       req.GetCompanyIds(),
		PrimaryCompanyID: req.GetPrimaryCompanyId(),
		AccountNumbers:   req.GetAccountNumbers(),
		FromDate:         req.GetFromDate(),
		ToDate:           req.GetToDate(),
		CurrencyID:       req.GetCurrencyId(),
		TypeLedger:       typeLedger,
		TypeShowCurrency: int(req.GetTypeShowCurrency()),
		GroupTheSameItem: int(req.GetGroupSameItem()),
		IsDependent:      int(req.GetIsDependent()),
		ClusterID:        req.GetClusterId(),
	}

	_, err := g.service.StreamSoQuy(stream.Context(), params, func(row Row) error {
		return stream.Send(rowToProto(row))
	})
	if err != nil {
		return fmt.Errorf("loi stream so ke toan chi tiet quy tien mat: %w", err)
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
// Hai consumer khác nhau nên định dạng khác nhau là đúng, không phải thiếu
// nhất quán.
func rowToProto(row Row) *pb.SoKeToanChiTietQuyTienMatRow {
	return &pb.SoKeToanChiTietQuyTienMatRow{
		PositionOrder: row.PositionOrder,
		CaType:        row.CAType,

		ReferenceId: row.RefID,
		TypeId:      row.TypeID,

		Date:       formatDate(row.Date),
		PostedDate: formatDate(row.PostedDate),

		ReceiptRefNo: row.ReceiptRefNo,
		PaymentRefNo: row.PaymentRefNo,

		JournalMemo: row.JournalMemo,
		Reason:      row.Reason,
		Note:        row.Note,

		Account:              row.Account,
		AccountCorresponding: row.AccountCorresponding,
		OrderPriority:        row.OrderPriority,

		// Cột không hậu tố: theo type_show_currency.
		// Cột _qd: luôn quy đổi.
		PhatSinhNo: row.PhatSinhNo.String(),
		PhatSinhCo: row.PhatSinhCo.String(),
		SoTon:      row.SoTon.String(),

		PhatSinhNoQd: row.PhatSinhNoQD.String(),
		PhatSinhCoQd: row.PhatSinhCoQD.String(),
		SoTonQd:      row.SoTonQD.String(),

		ExchangeRate: row.ExchangeRate.String(),

		AccountingObjectCode: row.AccountingObjectCode,
		AccountingObjectName: row.AccountingObjectName,

		// Sáu nhóm dưới đây hiện LUÔN rỗng: fact_gl_entry_line chưa có các cột
		// khoá ngoại tương ứng (ExpenseItemID, BudgetItemID, DepartmentID,
		// CostSetID, ContractID, StatisticsCodeID).
		//
		// KHÔNG ảnh hưởng số liệu — nhánh gộp của proc bỏ hẳn chúng:
		//     group by GL.ReferenceID, Account, AccountCorresponding, AccountingObjectID
		// Chúng chỉ để hiển thị ở chế độ chi tiết.
		//
		// Xem PENDING_fact_rebuild.md — bổ sung cùng đợt dựng lại fact.
		ExpenseItemCode:      row.ExpenseItemCode,
		ExpenseItemName:      row.ExpenseItemName,
		BudgetItemCode:       row.BudgetItemCode,
		BudgetItemName:       row.BudgetItemName,
		OrganizationUnitCode: row.OrganizationUnitCode,
		OrganizationUnitName: row.OrganizationUnitName,
		CostSetCode:          row.CostSetCode,
		CostSetName:          row.CostSetName,
		ContractNo:           row.ContractNo,
		StatisticsCodeCode:   row.StatisticsCodeCode,
		StatisticsCodeName:   row.StatisticsCodeName,

		CustomField:       row.CustomField[:],
		CustomFieldDetail: row.CustomFieldDetail[:],

		// Frontend dùng để bold và ẩn số tiền theo cột.
		// position_order: 0 = tồn đầu kỳ, 1 = chi tiết, 2 = cộng nhóm.
		IsSumCol: row.PositionOrder != PositionDetail,
	}
}

func formatDate(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format("2006-01-02")
}
