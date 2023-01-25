// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package remote

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net/http"
)

type ChunkedCompactWriter struct {
	writer  io.Writer
	flusher http.Flusher

	crc32    hash.Hash32
	writeBuf []byte
}

// NewChunkedCompactWriter constructs a ChunkedCompactWriter.
func NewChunkedCompactWriter(writeBuf []byte, w io.Writer, f http.Flusher) *ChunkedCompactWriter {
	return &ChunkedCompactWriter{writeBuf: writeBuf[:0], writer: w, flusher: f, crc32: crc32.New(castagnoliTable)}
}

func (w *ChunkedCompactWriter) Close() error {
	if len(w.writeBuf) == 0 {
		return nil
	}

	n, err := w.writer.Write(w.writeBuf)
	if err != nil {
		return err
	}
	if n != len(w.writeBuf) {
		return fmt.Errorf("short write: wrote %v but buffer is %v", n, len(w.writeBuf))
	}

	w.flusher.Flush()
	return nil
}

func (w *ChunkedCompactWriter) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}

	// len(b) + crc32 + binary.MaxVarintLen64
	requiredSpaceBytes := len(b) + 32/8 + binary.MaxVarintLen64

	leftSpaceBytes := cap(w.writeBuf) - len(w.writeBuf)

	if len(w.writeBuf) > 0 && leftSpaceBytes < requiredSpaceBytes {
		n, err := w.writer.Write(w.writeBuf)
		if err != nil {
			return n, err
		}
		if n != len(w.writeBuf) {
			return n, fmt.Errorf("short write: wrote %v but buf is %v", n, len(w.writeBuf))
		}
		w.flusher.Flush()
		w.writeBuf = w.writeBuf[:0]
	}

	var buf [binary.MaxVarintLen64]byte
	v := binary.PutUvarint(buf[:], uint64(len(b)))
	w.writeBuf = append(w.writeBuf, buf[:v]...)

	w.crc32.Reset()
	if _, err := w.crc32.Write(b); err != nil {
		return 0, err
	}
	w.writeBuf = binary.BigEndian.AppendUint32(w.writeBuf, w.crc32.Sum32())
	w.writeBuf = append(w.writeBuf, b...)

	return len(b), nil
}
