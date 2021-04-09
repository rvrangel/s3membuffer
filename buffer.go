package s3membuffer

import (
	"io"
	"sync"
	"time"
)

type s3DownloadBuffer struct {
	bufs          map[int64][]byte
	m             sync.Mutex
	bufSize       int64
	bufferMaxSize int64
	readPos       int64
	completed     bool
	err           error
}

func NewS3DownloadBufferNew(maxSize int64) *s3DownloadBuffer {
	return &s3DownloadBuffer{
		bufferMaxSize: maxSize,
		bufs:          make(map[int64][]byte, 10000),
	}
}

func (b *s3DownloadBuffer) checkIfAvailable(pos int64) bool {
	b.m.Lock()
	defer b.m.Unlock()

	if _, ok := b.bufs[b.readPos]; !ok {
		return false // chunk is not available yet
	}

	return true // ready to be read
}

func (b *s3DownloadBuffer) WriteAt(p []byte, pos int64) (int, error) {
	tmp := make([]byte, len(p))
	copy(tmp, p)

	for {
		if b.bufSize > b.bufferMaxSize {
			time.Sleep(time.Millisecond * 100)
		} else {
			break
		}
	}

	b.m.Lock()
	defer b.m.Unlock()

	b.bufs[pos] = tmp
	b.bufSize += int64(len(tmp))

	return len(p), nil
}

func (b *s3DownloadBuffer) Close() error {
	b.m.Lock()
	defer b.m.Unlock()

	b.completed = true

	return nil
}

func (b *s3DownloadBuffer) Read(p []byte) (int, error) {
	if len(b.bufs) == 0 {
		if b.completed {
			return 0, io.EOF
		}

		// wait for more data to be available
		time.Sleep(time.Millisecond * 50)

		return 0, nil
	}

	for {
		if b.checkIfAvailable(b.readPos) {
			break
		}

		// wait for the data we want to be available
		time.Sleep(time.Millisecond * 100)
	}

	b.m.Lock()
	defer b.m.Unlock()

	originalPos := b.readPos

	chunk := b.bufs[b.readPos]
	chunkSize := int64(len(chunk))

	b.readPos = b.readPos + chunkSize
	b.bufSize -= chunkSize

	n := int64(copy(p, chunk))

	// if the buffer provided is smaller than the chunk size, we need to put the remaining data in a new chunk
	if n < chunkSize {
		chunk = chunk[n:]

		b.readPos -= int64(len(chunk))
		b.bufs[b.readPos] = chunk
	}

	delete(b.bufs, originalPos)

	return int(n), nil
}
