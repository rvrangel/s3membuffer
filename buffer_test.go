package s3membuffer

import (
	"fmt"
	"testing"
)

func TestWriteAtBuffer(t *testing.T) {
	b := NewS3DownloadBufferNew(1024)

	_, err := b.WriteAt([]byte{3, 4, 5}, 2)
	if err != nil {
		t.Errorf("expected no error, but received %v", err)
	}

	_, err = b.WriteAt([]byte{1, 2}, 0)
	if err != nil {
		t.Errorf("expected no error, but received %v", err)
	}

	_, err = b.WriteAt([]byte{6, 7}, 5)
	if err != nil {
		t.Errorf("expected no error, but received %v", err)
	}

	b.Close()

	tests := []struct {
		n    int
		data []byte
	}{
		{2, []byte{1, 2}},
		{2, []byte{3, 4}},
		{1, []byte{5, 0}},
		{2, []byte{6, 7}},
	}

	for i, test := range tests {

		t.Run(fmt.Sprintf("test #%d", i+1), func(t *testing.T) {
			buf := make([]byte, 2)
			n, err := b.Read(buf)

			if err != nil {
				t.Error("got an error")
			}

			if n != test.n {
				t.Errorf("got %d bytes, expected %d bytes", n, test.n)
			}

			if string(buf) != string(test.data) {
				t.Errorf("got %v, expected %v", buf, test.data)
			}
		})
	}
}
