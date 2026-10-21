// Package grpczstd đăng ký compressor "zstd" cho gRPC (grpc-go không có sẵn).
//
// Chỉ có tác dụng khi message chứa nhiều dòng (lô): nén từng dòng ~334 B chỉ
// được ~1,15x, nén lô 500+ dòng được ~10x. Client phải khai báo hỗ trợ zstd
// (grpc-accept-encoding) thì server mới nén; client không hỗ trợ vẫn nhận dữ
// liệu không nén bình thường.
package grpczstd

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

// Name là giá trị dùng trong grpc-encoding / grpc-accept-encoding.
const Name = "zstd"

type compressor struct {
	encoders sync.Pool
	decoders sync.Pool
}

func init() {
	encoding.RegisterCompressor(&compressor{
		encoders: sync.Pool{New: func() any {
			// SpeedFastest + 1 luồng: đo thực tế nhanh hơn và nén chặt hơn
			// gzip BestSpeed; đồng thời tránh tạo goroutine mới cho mỗi message.
			e, _ := zstd.NewWriter(nil,
				zstd.WithEncoderLevel(zstd.SpeedFastest),
				zstd.WithEncoderConcurrency(1))
			return e
		}},
		decoders: sync.Pool{New: func() any {
			d, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
			return d
		}},
	})
}

func (*compressor) Name() string { return Name }

func (c *compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	e := c.encoders.Get().(*zstd.Encoder)
	e.Reset(w)
	return &pooledWriter{Encoder: e, put: func() { c.encoders.Put(e) }}, nil
}

func (c *compressor) Decompress(r io.Reader) (io.Reader, error) {
	d := c.decoders.Get().(*zstd.Decoder)
	if err := d.Reset(r); err != nil {
		c.decoders.Put(d)
		return nil, err
	}
	return &pooledReader{Decoder: d, put: func() { c.decoders.Put(d) }}, nil
}

type pooledWriter struct {
	*zstd.Encoder
	put func()
}

// Close flush frame rồi trả encoder về pool.
func (p *pooledWriter) Close() error {
	err := p.Encoder.Close()
	p.put()
	return err
}

type pooledReader struct {
	*zstd.Decoder
	put  func()
	done bool
}

// Read trả decoder về pool khi đọc hết (EOF) hoặc lỗi.
func (p *pooledReader) Read(b []byte) (int, error) {
	n, err := p.Decoder.Read(b)
	if err != nil && !p.done {
		p.done = true
		p.put()
	}
	return n, err
}
