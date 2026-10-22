package grpcbatch

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"google.golang.org/grpc/encoding"
)

type row struct{ n int }

func collect(t *testing.T, size, total int) (batches [][]int) {
	t.Helper()
	b := &Batcher[row]{rows: make([]*row, 0, size), size: size, send: func(rows []*row) error {
		cur := make([]int, len(rows))
		for i, r := range rows {
			cur[i] = r.n
		}
		batches = append(batches, cur)
		return nil
	}}
	for i := 0; i < total; i++ {
		if err := b.Add(&row{i}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	return batches
}

func TestBatcherFlushesAtSizeAndKeepsOrder(t *testing.T) {
	got := collect(t, 3, 7)
	want := [][]int{{0, 1, 2}, {3, 4, 5}, {6}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBatcherExactMultipleHasNoEmptyTail(t *testing.T) {
	if got := collect(t, 3, 6); len(got) != 2 {
		t.Fatalf("got %v", got)
	}
	if got := collect(t, 3, 0); len(got) != 0 {
		t.Fatalf("report rỗng không được gửi lô nào, got %v", got)
	}
}

func TestBatcherPropagatesSendError(t *testing.T) {
	boom := errors.New("boom")
	b := &Batcher[row]{size: 1, send: func([]*row) error { return boom }}
	if err := b.Add(&row{1}); !errors.Is(err, boom) {
		t.Fatalf("got %v", err)
	}
}

func TestConfigureClampsSize(t *testing.T) {
	defer Configure(DefaultSize, true)
	for in, want := range map[int]int{0: DefaultSize, -5: DefaultSize, 1: 1, 1200: 1200, MaxSize + 1: MaxSize} {
		Configure(in, true)
		if Size() != want {
			t.Fatalf("Configure(%d): Size = %d, want %d", in, Size(), want)
		}
	}
}

// Compressor zstd phải đăng ký được và nén/giải nén khứ hồi đúng.
func TestZstdCompressorRoundTrip(t *testing.T) {
	c := encoding.GetCompressor("zstd")
	if c == nil {
		t.Fatal("compressor zstd chưa được đăng ký")
	}
	src := bytes.Repeat([]byte("so chi tiet tai khoan 131 - phai thu khach hang\n"), 2000)
	var buf bytes.Buffer
	w, err := c.Compress(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(src); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if buf.Len() >= len(src)/5 {
		t.Fatalf("nén kém bất thường: %d -> %d", len(src), buf.Len())
	}
	r, err := c.Decompress(&buf)
	if err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil || !bytes.Equal(out, src) {
		t.Fatalf("khứ hồi sai: err=%v", err)
	}
}
