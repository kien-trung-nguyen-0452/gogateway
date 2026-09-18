package so_chi_tiet_vat_lieu_hang_hoa

import (
	"fmt"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"

	"softdream.vn/go-gateway/internal/so_chi_tiet_vat_lieu_hang_hoa/pb"
)

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
	// grpc-go mac dinh CHI nen response theo dung encoding request da dung
	// (co che "mirror" - xem server.go:1697-1707 cua grpc-go). Request nay
	// chi vai field loc, nen Java hau nhu chac chan khong nen no - neu
	// khong goi SetSendCompressor o day, response 500k+ dong van se KHONG
	// duoc nen du da dang ky codec gzip.
	//
	// SetSendCompressor LOI neu client khong advertise gzip qua
	// grpc-accept-encoding (vd Java chua ho tro/chua cau hinh) - KHONG duoc
	// return loi do ra ngoai, se lam fail toan bo report cho client do. Chi
	// log lai va tiep tuc gui KHONG nen, dung nhu truoc khi doi.
	if err := grpc.SetSendCompressor(stream.Context(), gzip.Name); err != nil {
		log.Printf("so-chi-tiet-vat-lieu-hang-hoa: client khong ho tro gzip, gui khong nen: %v", err)
	}

	params := QueryParams{
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

	_, err := g.service.StreamSoChiTiet(stream.Context(), params, func(row Row) error {
		return stream.Send(rowToProto(row))
	})
	if err != nil {
		return fmt.Errorf("loi stream so chi tiet vat lieu: %w", err)
	}
	return nil
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
