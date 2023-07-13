package pgproto3

import (
	"github.com/pg-sharding/spqr/pkg/iobufpool"
	"io"
)

// ChunkReader is an interface to decouple github.com/jackc/chunkreader from this package.
type ChunkReader interface {
	// Next returns buf filled with the next n bytes. If an error (including a partial read) occurs,
	// buf must be nil. Next must preserve any partially read data. Next must not reuse buf.
	Next(n int) (buf []byte, err error)
}

// NewChunkReader creates and returns a new default ChunkReader.
func NewChunkReader(r io.Reader) ChunkReader {
	return newChunkReader(r, 0)
}

type chunkReader struct {
	r io.Reader

	buf    *[]byte
	rp, wp int // buf read position and write position

	minBufSize int
}

// newChunkReader creates and returns a new chunkReader for r with default configuration. If minBufSize is <= 0 it uses
// a default value.
func newChunkReader(r io.Reader, minBufSize int) *chunkReader {
	if minBufSize <= 0 {
		// By historical reasons Postgres currently has 8KB send buffer inside,
		// so here we want to have at least the same size buffer.
		// @see https://github.com/postgres/postgres/blob/249d64999615802752940e017ee5166e726bc7cd/src/backend/libpq/pqcomm.c#L134
		// @see https://www.postgresql.org/message-id/0cdc5485-cb3c-5e16-4a46-e3b2f7a41322%40ya.ru
		//
		// In addition, testing has found no benefit of any larger buffer.
		minBufSize = 8192
	}

	return &chunkReader{
		r:          r,
		minBufSize: minBufSize,
		buf:        iobufpool.Get(minBufSize),
	}
}

// Next returns buf filled with the next n bytes. buf is only valid until next call of Next. If an error occurs, buf
// will be nil.
func (r *chunkReader) Next(n int) (buf []byte, err error) {
	// Reset the buffer if it is empty
	if r.rp == r.wp {
		if len(*r.buf) != r.minBufSize {
			iobufpool.Put(r.buf)
			r.buf = iobufpool.Get(r.minBufSize)
		}
		r.rp = 0
		r.wp = 0
	}

	// n bytes already in buf
	if (r.wp - r.rp) >= n {
		buf = (*r.buf)[r.rp : r.rp+n : r.rp+n]
		r.rp += n
		return buf, err
	}

	// buf is smaller than requested number of bytes
	if len(*r.buf) < n {
		bigBuf := iobufpool.Get(n)
		r.wp = copy((*bigBuf), (*r.buf)[r.rp:r.wp])
		r.rp = 0
		iobufpool.Put(r.buf)
		r.buf = bigBuf
	}

	// buf is large enough, but need to shift filled area to start to make enough contiguous space
	minReadCount := n - (r.wp - r.rp)
	if (len(*r.buf) - r.wp) < minReadCount {
		r.wp = copy((*r.buf), (*r.buf)[r.rp:r.wp])
		r.rp = 0
	}

	// Read at least the required number of bytes from the underlying io.Reader
	readBytesCount, err := io.ReadAtLeast(r.r, (*r.buf)[r.wp:], minReadCount)
	r.wp += readBytesCount
	// fmt.Println("read", n)
	if err != nil {
		return nil, err
	}

	buf = (*r.buf)[r.rp : r.rp+n : r.rp+n]
	r.rp += n
	return buf, nil
}
