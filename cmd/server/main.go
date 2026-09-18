package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"softdream.vn/go-gateway/internal/api"
	"softdream.vn/go-gateway/internal/chclient"
	"softdream.vn/go-gateway/internal/config"
	"softdream.vn/go-gateway/internal/reports/so-chi-tiet-vat-lieu-hang-hoa"
	quy "softdream.vn/go-gateway/internal/reports/so-ke-toan-chi-tiet-quy-tien-mat"
	"softdream.vn/go-gateway/internal/reports/tichluy"
	quypb "softdream.vn/go-gateway/internal/so_ke_toan_chi_tiet_quy_tien_mat/pb"

	// Hai package sinh tu proto DEU co ten "pb" nen phai dat alias, neu khong
	// trinh bien dich bao trung ten.
	vlpb "softdream.vn/go-gateway/internal/so_chi_tiet_vat_lieu_hang_hoa/pb"

	// MOI THEM - so chi tiet tai khoan
	tk "softdream.vn/go-gateway/internal/reports/so-chi-tiet-cac-tai-khoan"
	tkpb "softdream.vn/go-gateway/internal/so-chi-tiet-cac-tai-khoan/pb"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("loi cau hinh: %v", err)
	}

	conn, err := chclient.NewConn(cfg)
	if err != nil {
		log.Fatalf("loi ket noi ClickHouse: %v", err)
	}
	defer conn.Close()

	log.Printf("Da ket noi ClickHouse qua native protocol tai %s:%d", cfg.CHHost, cfg.CHPort)

	// ---- Cac service ------------------------------------------------------
	tichLuyService := tichluy.NewService(conn)
	tichLuyHandler := tichluy.NewHandler(tichLuyService)

	soChiTietService := so_chi_tiet_vat_lieu_hang_hoa.NewService(conn)
	soChiTietHandler := so_chi_tiet_vat_lieu_hang_hoa.NewHandler(soChiTietService)

	// MOI THEM
	// useStaging: xem config.SoChiTietTaiKhoanUseStaging - bat tam thoi bang
	// SO_CHI_TIET_TAI_KHOAN_DATA_SOURCE=staging khi pipeline fact bi loi.
	taiKhoanService := tk.NewService(conn, cfg.SoChiTietTaiKhoanUseStaging)
	taiKhoanHandler := tk.NewHandler(taiKhoanService)

	mux := api.NewRouter(tichLuyHandler, soChiTietHandler)

	//cashLedger
	// useStaging: xem config.SoKeToanChiTietQuyTienMatUseStaging - bat tam
	// thoi bang SO_KE_TOAN_CHI_TIET_QUY_TIEN_MAT_DATA_SOURCE=staging khi
	// pipeline fact loi, giong so-chi-tiet-cac-tai-khoan.
	quyService := quy.NewService(conn, cfg.SoKeToanChiTietQuyTienMatUseStaging)
	mux.Handle("/api/so-ke-toan-chi-tiet-quy-tien-mat", quy.NewHandler(quyService))

	// api.NewRouter chua nhan handler moi. Dang ky truc tiep o day de khong
	// phai sua chu ky ham dung chung; khi nao on dinh thi don vao NewRouter.
	mux.Handle("/api/so-chi-tiet-tai-khoan", taiKhoanHandler)

	// ---- HTTP server - CHAY TRONG GOROUTINE, khong bloc luong chinh -------
	// (truoc day goi truc tiep ListenAndServe() o day se CHAN LUON, khien
	// phan gRPC ben duoi khong bao gio chay toi)
	addr := ":" + cfg.HTTPPort
	httpSrv := &http.Server{
		Addr:    addr,
		Handler: mux,
		// Chan slowloris: client mo ket noi roi gui header nho giot se giu
		// thread vo thoi han neu khong co timeout nay.
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		log.Printf("HTTP server dang chay tai %s", addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	// ---- gRPC server (dung cho bao cao lon, streaming) --------------------
	lis, err := net.Listen("tcp", ":"+cfg.GRPCPort)
	if err != nil {
		log.Fatalf("loi listen gRPC: %v", err)
	}

	grpcServer := grpc.NewServer(
	// Bao cao lon co the vuot gioi han message mac dinh cua gRPC (4MB)
	// NEU con dung Unary RPC - nhung vi da dung STREAMING (moi message
	// la 1 dong, rat nho), khong can chinh MaxRecvMsgSize/MaxSendMsgSize.
	)

	grpcSvc := so_chi_tiet_vat_lieu_hang_hoa.NewGRPCServer(soChiTietService)
	vlpb.RegisterSoChiTietVatLieuServiceServer(grpcServer, grpcSvc)

	// MOI THEM - thieu dong nay se bao:
	//   UNIMPLEMENTED: unknown service sochitiettaikhoan.SoChiTietTaiKhoanService
	tkpb.RegisterSoChiTietTaiKhoanServiceServer(grpcServer, tk.NewGRPCServer(taiKhoanService))

	//CashLedger
	quypb.RegisterSoKeToanChiTietQuyTienMatServiceServer(grpcServer, quy.NewGRPCServer(quyService))

	// ---- Health check -----------------------------------------------------
	// Cho phep client va script deploy biet service san sang chua, thay vi
	// doan qua viec cong co mo hay khong.
	healthSrv := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthSrv)
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// ---- Reflection -------------------------------------------------------
	// Khong co dong nay thi `grpcurl -plaintext ... list` khong hoat dong.
	// Do la ly do rat kho chan doan khi mot service chua duoc dang ky —
	// client chi nhan UNIMPLEMENTED ma khong biet server dang co gi.
	//
	// CAN NHAC TAT O PROD neu service lo ra ngoai: reflection cho phep do
	// toan bo API.
	reflection.Register(grpcServer)

	go func() {
		log.Printf("gRPC server dang chay tai :%s", cfg.GRPCPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()

	// ---- Tat em -----------------------------------------------------------
	// Cac stream bao cao co the chay hang chuc giay. Cat dot ngot khien client
	// nhan EOF giua chung va khong phan biet duoc voi loi that.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Dang tat service...")
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)

	done := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(done)
	}()

	select {
	case <-done:
		log.Println("gRPC da dung em")
	case <-time.After(30 * time.Second):
		// Phai NHO HON TimeoutStopSec cua systemd (45s), neu khong systemd
		// SIGKILL truoc khi tien trinh kip dong em.
		log.Println("Het thoi gian cho, dung cung gRPC")
		grpcServer.Stop()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)

	log.Println("Da tat.")
}
