package so_chi_tiet_vat_lieu_hang_hoa

import (
	"fmt"

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

		// LUU Y: proto hien chua co field ParamCheckAll - QueryParams that
		// cua ban co field nay nhung khong con dung trong buildQuery (da
		// gop logic o buoc sua service.go). Neu ParamCheckAll con y nghia
		// khac can giu, them field vao .proto va map lai o day.
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
		ConvertRate:          row.ConvertRate.String(),
		MainQuantity:         row.MainQuantity.String(),
		MainUnitPrice:        row.MainUnitPrice.String(),
		UnitPrice:            row.UnitPrice.String(),
		InwardQuantity:       row.InwardQuantity.String(),
		InwardAmount:         row.InwardAmount.String(),
		OutwardQuantity:      row.OutwardQuantity.String(),
		OutwardAmount:        row.OutwardAmount.String(),
		ClosingQuantity:      row.ClosingQuantity.String(),
		ClosingAmount:        row.ClosingAmount.String(),

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
