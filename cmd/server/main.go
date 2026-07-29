package main

import (
	"log"
	"net"
	"net/http"

	"google.golang.org/grpc"

	"softdream.vn/go-gateway/internal/api"
	"softdream.vn/go-gateway/internal/chclient"
	"softdream.vn/go-gateway/internal/config"
	"softdream.vn/go-gateway/internal/reports/so-chi-tiet-vat-lieu-hang-hoa"
	"softdream.vn/go-gateway/internal/reports/tichluy"
	"softdream.vn/go-gateway/internal/so_chi_tiet_vat_lieu_hang_hoa/pb" // MOI THEM - package sinh tu proto
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

	tichLuyService := tichluy.NewService(conn)
	tichLuyHandler := tichluy.NewHandler(tichLuyService)

	soChiTietService := so_chi_tiet_vat_lieu_hang_hoa.NewService(conn)
	soChiTietHandler := so_chi_tiet_vat_lieu_hang_hoa.NewHandler(soChiTietService)

	mux := api.NewRouter(tichLuyHandler, soChiTietHandler)

	// ---- HTTP server - CHAY TRONG GOROUTINE, khong bloc luong chinh -------
	// (truoc day goi truc tiep ListenAndServe() o day se CHAN LUON, khien
	// phan gRPC ben duoi khong bao gio chay toi)
	addr := ":" + cfg.HTTPPort
	go func() {
		log.Printf("HTTP server dang chay tai %s", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Fatal(err)
		}
	}()

	// ---- gRPC server (MOI - dung cho bao cao lon, streaming) -------------
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
	pb.RegisterSoChiTietVatLieuServiceServer(grpcServer, grpcSvc)

	log.Println("gRPC server dang chay tai :9090")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatal(err)
	}
}
