package api

import (
	"net/http"

	"softdream.vn/go-gateway/internal/datasets/tinh-gia-xuat-kho"
	"softdream.vn/go-gateway/internal/reports/so-chi-tiet-vat-lieu-hang-hoa"
	"softdream.vn/go-gateway/internal/reports/tichluy"
)

func NewRouter(
	tichLuyHandler *tichluy.Handler,
	soChiTietHandler *so_chi_tiet_vat_lieu_hang_hoa.Handler,
	tinhGiaHandler *tinh_gia_xuat_kho.Handler,
) *http.ServeMux {
	mux := http.NewServeMux()

	mux.Handle("/internal/v1/reports/tich-luy", tichLuyHandler)
	mux.Handle("/internal/v1/reports/so-chi-tiet-vat-lieu-hang-hoa", soChiTietHandler)
	mux.Handle("/internal/v1/datasets/tinh-gia-xuat-kho", tinhGiaHandler)

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK")) //nolint:errcheck
	})

	return mux
}
