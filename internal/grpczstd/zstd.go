// Package grpczstd dang ky compressor "zstd" cho gRPC (grpc-go khong co san).
//
// Chi co tac dung khi message chua nhieu dong (lo): nen tung dong 334 B chi
// duoc ~1,15x, nen lo 500+ dong duoc ~10x. Client phai khai bao ho tro zstd
// (grpc-accept-encoding) thi server moi nen; client khong ho tro van nhan du
// lieu khong nen binh thuong.
package grpczstd

import (
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

// Name la gia tri dung trong grpc-encoding / grpc-accept-encoding.
const Name = "zstd"

type compressor struct {
	encoders sync.Pool
	decoders sync.Pool
}

func init() {
	encoding.RegisterCompressor(&compressor{
		encoders: sync.Pool{New: func() any {
			// SpeedFastest + 1 luong: do thuc te nhanh hon va nen chat hon
			// gzip BestSpeed; dong thoi tranh tao goroutine moi cho moi message.
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

// Close flush frame roi tra encoder ve pool.
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

// Read tra decoder ve pool khi doc het (EOF) hoac loi.
func (p *pooledReader) Read(b []byte) (int, error) {
	n, err := p.Decoder.Read(b)
	if err != nil && !p.done {
		p.done = true
		p.put()
	}
	return n, err
}
