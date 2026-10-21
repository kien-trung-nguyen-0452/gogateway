// Package grpcbatch là điểm quản lý TẬP TRUNG việc gom dòng thành lô và nén zstd
// cho mọi báo cáo stream qua gRPC. Mọi sổ dùng chung một cỡ lô và một công tắc
// nén, chỉnh ở một chỗ (biến môi trường, xem config.GRPCBatchSize và
// config.GRPCCompression) mà không phải sửa từng sổ.
//
// THÊM MỘT SỔ MỚI chỉ cần ba việc.
//
// Việc 1, proto: thêm message lô và RPC trả về stream lô.
//
//	message XBatch { repeated XRow rows = 1; }
//	rpc GetXBatch(XRequest) returns (stream XBatch);
//
// Việc 2, handler (khoảng 8 dòng):
//
//	func (g *GRPCServer) GetXBatch(req *pb.XRequest, stream grpc.ServerStreamingServer[pb.XBatch]) error {
//		b := grpcbatch.Begin(stream.Context(), func(rows []*pb.XRow) error {
//			return stream.Send(&pb.XBatch{Rows: rows})
//		})
//		_, err := g.service.Stream(stream.Context(), paramsFromRequest(req), func(row Row) error {
//			return b.Add(rowToProto(row))
//		})
//		if err != nil {
//			return err
//		}
//		return b.Flush()
//	}
//
// Việc 3, Java (ebwebbe): gọi RPC lô rồi duyệt bằng GrpcBatch.forEach.
//
// Không cần đăng ký compressor hay tự viết vòng gom lô: Begin đã bật zstd (nếu
// client khai báo hỗ trợ), Batcher lo việc gom và flush.
//
// VÌ SAO phải gom lô mới nén: nén từng dòng ~334 B chỉ được ~1,15x và tốn CPU
// gấp ~30 lần; nén lô 500+ dòng được ~10x. Vì vậy KHÔNG bật nén tự động cho mọi
// RPC bằng interceptor.
package grpcbatch

import (
	"context"
	"log"
	"sync/atomic"

	"google.golang.org/grpc"

	"softdream.vn/go-gateway/internal/grpczstd"
)

const (
	// DefaultSize: đo trên dữ liệu thật (1 triệu dòng), từ 500 dòng trở lên tỷ lệ
	// nén và thời gian gần như không đổi; dưới 250 dòng nén kém đi.
	DefaultSize = 600

	// MaxSize chặn cỡ lô cấu hình sai: dòng thật lớn nhất đã gặp là 460 B nên
	// 5000 dòng khoảng 2,3 MB, còn dưới giới hạn mặc định 4 MB của gRPC.
	MaxSize = 5000
)

var (
	size     atomic.Int64
	compress atomic.Bool
)

func init() {
	size.Store(DefaultSize)
	compress.Store(true)
}

// Configure đặt cỡ lô và công tắc nén cho toàn bộ ứng dụng. Gọi một lần ở main,
// trước khi nhận request. batchSize ngoài khoảng 1..MaxSize thì dùng giá trị mặc
// định hoặc trần tương ứng.
func Configure(batchSize int, compression bool) {
	switch {
	case batchSize <= 0:
		batchSize = DefaultSize
	case batchSize > MaxSize:
		log.Printf("grpcbatch: cỡ lô %d vượt trần, dùng %d", batchSize, MaxSize)
		batchSize = MaxSize
	}
	size.Store(int64(batchSize))
	compress.Store(compression)
	log.Printf("grpcbatch: cỡ lô = %d dòng, nén zstd = %v [GRPC_BATCH_SIZE, GRPC_COMPRESSION]", batchSize, compression)
}

// Size trả về cỡ lô đang áp dụng.
func Size() int { return int(size.Load()) }

// Batcher gom từng dòng thành lô và gọi send khi đủ. Không an toàn cho nhiều
// goroutine: dùng trong đúng callback của một stream.
type Batcher[R any] struct {
	rows []*R
	size int
	send func(rows []*R) error
}

// Begin bật nén zstd cho stream này (nếu công tắc bật và client khai báo hỗ trợ)
// rồi trả về Batcher. Lỗi bật nén nghĩa là client không hỗ trợ zstd: vẫn gửi
// được, chỉ là không nén.
//
// send nhận slice dùng chung: Batcher tái sử dụng nó ngay sau khi send trả về,
// nên send KHÔNG được giữ lại slice. stream.Send mã hoá message trước khi trả về
// nên gửi thẳng &Batch{Rows: rows} là đúng.
func Begin[R any](ctx context.Context, send func(rows []*R) error) *Batcher[R] {
	if compress.Load() {
		_ = grpc.SetSendCompressor(ctx, grpczstd.Name)
	}
	n := Size()
	return &Batcher[R]{rows: make([]*R, 0, n), size: n, send: send}
}

// Add thêm một dòng, gửi lô nếu đã đủ cỡ.
func (b *Batcher[R]) Add(row *R) error {
	b.rows = append(b.rows, row)
	if len(b.rows) >= b.size {
		return b.Flush()
	}
	return nil
}

// Flush gửi phần còn lại. Phải gọi sau khi đã Add hết dòng.
func (b *Batcher[R]) Flush() error {
	if len(b.rows) == 0 {
		return nil
	}
	err := b.send(b.rows)
	clear(b.rows)
	b.rows = b.rows[:0]
	return err
}
