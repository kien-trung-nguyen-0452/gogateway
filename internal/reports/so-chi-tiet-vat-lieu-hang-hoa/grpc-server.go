package so_chi_tiet_vat_lieu_hang_hoa

import (
	"fmt"

	"google.golang.org/grpc"

	"softdream.vn/go-gateway/internal/grpczstd"
	"softdream.vn/go-gateway/internal/so_chi_tiet_vat_lieu_hang_hoa/pb"
)

// batchSize la so dong toi da trong 1 message cua GetSoChiTietVatLieuBatch. Do
// tren du lieu that (1 trieu dong): tu 500 tro len ty le nen zstd va thoi gian
// gan nhu khong doi, duoi 250 dong nen kem di; xem so-chi-tiet-cac-tai-khoan.
const batchSize = 600

// GRPCServer - implement pb.SoChiTietVatLieuServiceServer (sinh tu
// proto/sochitietvatlieu.proto - xem proto/generate.sh de sinh code, KHONG
// sinh duoc trong sandbox nay do bi chan mang toi google.golang.org).
type GRPCServer struct {
	pb.UnimplementedSoChiTietVatLieuServiceServer
	service *Service
}

func NewGRPCServer(service *Service) *GRPCServer {
	return &GRPCServer{service: service}
}

// GetSoChiTietVatLieu - implement RPC streaming. Goi StreamSoChiTiet, MOI
// DONG tu callback duoc chuyen thang thanh proto message va gui qua
// stream.Send() NGAY LAP TUC - khong bao gio giu ca lo du lieu trong RAM
// cung luc. Day la diem mau chot giai quyet treo voi bao cao 500k+ dong.
func (g *GRPCServer) GetSoChiTietVatLieu(req *pb.SoChiTietVatLieuRequest, stream pb.SoChiTietVatLieuService_GetSoChiTietVatLieuServer) error {
	params := paramsFromRequest(req)

	_, err := g.service.StreamSoChiTiet(stream.Context(), params, func(row Row) error {
		return stream.Send(rowToProto(row))
	})
	if err != nil {
		return fmt.Errorf("loi stream so chi tiet vat lieu: %w", err)
	}
	return nil
}

// GetSoChiTietVatLieuBatch - cung noi dung voi GetSoChiTietVatLieu nhung gom
// batchSize dong vao 1 message va nen zstd (neu client ho tro). Thu tu dong giu
// nguyen; lo cuoi duoc flush sau khi StreamSoChiTiet xong.
func (g *GRPCServer) GetSoChiTietVatLieuBatch(req *pb.SoChiTietVatLieuRequest, stream grpc.ServerStreamingServer[pb.SoChiTietVatLieuBatch]) error {
	// Loi o day nghia la client khong khai bao ho tro zstd: van gui duoc,
	// chi la khong nen.
	_ = grpc.SetSendCompressor(stream.Context(), grpczstd.Name)

	// Send ma hoa message truoc khi tra ve nen tai su dung duoc buffer.
	batch := &pb.SoChiTietVatLieuBatch{Rows: make([]*pb.SoChiTietVatLieuRow, 0, batchSize)}
	flush := func() error {
		if len(batch.Rows) == 0 {
			return nil
		}
		err := stream.Send(batch)
		clear(batch.Rows)
		batch.Rows = batch.Rows[:0]
		return err
	}

	_, err := g.service.StreamSoChiTiet(stream.Context(), paramsFromRequest(req), func(row Row) error {
		batch.Rows = append(batch.Rows, rowToProto(row))
		if len(batch.Rows) >= batchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("loi stream so chi tiet vat lieu (batch): %w", err)
	}
	return flush()
}

func paramsFromRequest(req *pb.SoChiTietVatLieuRequest) QueryParams {
	return QueryParams{
		CompanyIDs:               req.GetCompanyIds(),
		PrimaryCompanyID:         req.GetPrimaryCompanyId(),
		RepositoryIDs:            req.GetRepositoryIds(),
		MaterialGoodsIDs:         req.GetMaterialGoodsIds(),
		OwnRepositoryIDs:         req.GetOwnRepositoryIds(),
		OtherRepositoryIDs:       req.GetOtherRepositoryIds(),
		IsCompanyBusinessTypeGas: int(req.GetIsCompanyBusinessTypeGas()),
		FromDate:                 req.GetFromDate(),
		ToDate:                   req.GetToDate(),
		GetAccountHasData:        req.GetGetAccountHasData(),
		ParamCheckAll:            req.GetParamCheckAll(),

		// UnitType = so thu tu don vi tinh chuyen doi (0 = khong quy doi).
		// TUYET DOI khong duoc bo sot: thieu dong nay thi UnitType nhan gia tri mac dinh
		// 0 cua Go, buildQuery thay {{UNIT_TYPE}} = 0, va query luon chay nhanh
		// "khong quy doi" du nguoi dung da chon don vi chuyen doi. Trieu chung rat kho
		// lan ra: cot DVT hien don vi goc, khong he co loi nao duoc bao.
		UnitType: int(req.GetUnitType()),
	}
}

// rowToProto - xuat so lieu DANG STRING (khong dung float/double) de KHONG
// mat do chinh xac qua Protobuf - Java tu parse BigDecimal tu chuoi nay.
func rowToProto(row Row) *pb.SoChiTietVatLieuRow {
	return &pb.SoChiTietVatLieuRow{
		IsDetail:             row.IsDetail,
		RepositoryId:         row.RepositoryID,
		RepositoryCode:       row.RepositoryCode,
		RepositoryName:       row.RepositoryName,
		MaterialGoodsId:      row.MaterialGoodsID,
		MaterialGoodsCode:    row.MaterialGoodsCode,
		MaterialGoodsName:    row.MaterialGoodsName,
		ReferenceId:          row.ReferenceID,
		DetailId:             row.DetailID,
		TypeId:               row.TypeID,
		PostedDate:           row.PostedDate.Format("2006-01-02T15:04:05Z07:00"),
		RefDate:              formatRefDate(row),
		RefNo:                row.RefNo,
		Reason:               row.Reason,
		AccountCorresponding: row.AccountCorresponding,
		CurrencyId:           row.CurrencyID,
		UnitId:               row.UnitID,
		UnitName:             row.UnitName,
		// Thieu dong nay thi Java nhan mainUnitName = "" du ClickHouse da tra ve du lieu:
		// query -> scanRow -> Row.MainUnitName deu co, nhung khong duoc dat vao proto message.
		MainUnitName:    row.MainUnitName,
		ConvertRate:     row.ConvertRate.String(),
		MainQuantity:    row.MainQuantity.String(),
		MainUnitPrice:   row.MainUnitPrice.String(),
		UnitPrice:       row.UnitPrice.String(),
		InwardQuantity:  row.InwardQuantity.String(),
		InwardAmount:    row.InwardAmount.String(),
		OutwardQuantity: row.OutwardQuantity.String(),
		OutwardAmount:   row.OutwardAmount.String(),
		ClosingQuantity: row.ClosingQuantity.String(),
		ClosingAmount:   row.ClosingAmount.String(),

		AccountingObjectId:      row.AccountingObjectID,
		AccountingObjectCode:    row.AccountingObjectCode,
		AccountingObjectName:    row.AccountingObjectName,
		AccountingObjectAddress: row.AccountingObjectAddress,
		TaxCode:                 row.TaxCode,

		ExpenseItemId:   row.ExpenseItemID,
		ExpenseItemCode: row.ExpenseItemCode,
		ExpenseItemName: row.ExpenseItemName,

		BudgetItemId:   row.BudgetItemID,
		BudgetItemCode: row.BudgetItemCode,
		BudgetItemName: row.BudgetItemName,

		DepartmentId:         row.DepartmentID,
		OrganizationUnitCode: row.OrganizationUnitCode,
		OrganizationUnitName: row.OrganizationUnitName,

		CostSetId:   row.CostSetID,
		CostSetCode: row.CostSetCode,
		CostSetName: row.CostSetName,

		EmContractId: row.EMContractID,
		ContractNo:   row.ContractNo,

		StatisticsCodeId:   row.StatisticsCodeID,
		StatisticsCode:     row.StatisticsCode,
		StatisticsCodeName: row.StatisticsCodeName,
	}
}

func formatRefDate(row Row) string {
	if row.RefDate.IsZero() {
		return ""
	}
	return row.RefDate.Format("2006-01-02T15:04:05Z07:00")
}
