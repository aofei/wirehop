package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"testing"
)

type bufferedFrameSource struct {
	*bytes.Reader
	reads int
}

func (s *bufferedFrameSource) Read(buffer []byte) (int, error) {
	s.reads++
	return s.Reader.Read(buffer)
}

func FuzzReadBufferedFrame(f *testing.F) {
	for _, encoded := range [][]byte{
		{}, {byte(FramePing)}, {0x81}, {0}, {0, 0},
		{0x81, 0}, {0x81, 0x80}, {0x81, 0x80, 0x80},
	} {
		f.Add(encoded)
	}
	for _, size := range []int{32, 1452, 32768, MaxFrameContentSize} {
		encoded, err := MarshalFrame(Frame{Type: FrameData, Payload: bytes.Repeat([]byte{0xa5}, size)})
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
	}
	f.Fuzz(func(t *testing.T, encoded []byte) {
		if len(encoded) > 2*MaxEncodedFrameSize {
			t.Skip()
		}
		source := &bufferedFrameSource{Reader: bytes.NewReader(encoded)}
		reader := bufio.NewReaderSize(source, max(16, len(encoded)))
		reader.Peek(len(encoded))
		initialReads := source.reads
		var borrowed []Frame
		var expected []Frame
		for reader.Buffered() > 0 {
			before := reader.Buffered()
			position := len(encoded) - before
			want, expectedErr := ReadFrame(bytes.NewReader(encoded[position:]))
			frame, available, err := ReadBufferedFrame(reader)
			if source.reads != initialReads {
				t.Fatal("buffered decoding read from the underlying source")
			}
			if !available {
				if expectedErr == nil {
					t.Fatal("buffered decoding did not return a complete frame")
				}
				if reader.Buffered() != before {
					t.Fatal("incomplete or invalid frame consumed buffered bytes")
				}
				if err != nil {
					if !errors.Is(err, expectedErr) {
						t.Fatalf("buffered error %v differs from stream error %v", err, expectedErr)
					}
				} else if !errors.Is(expectedErr, io.ErrUnexpectedEOF) {
					t.Fatalf("buffered decoding suppressed stream error %v", expectedErr)
				}
				break
			}
			if err != nil || expectedErr != nil || frame.Type != want.Type || !bytes.Equal(frame.Payload, want.Payload) {
				t.Fatalf("buffered and stream decoding differ: %v, %v", err, expectedErr)
			}
			borrowed = append(borrowed, frame)
			expected = append(expected, want)
		}
		for index, frame := range borrowed {
			if !bytes.Equal(frame.Payload, expected[index].Payload) {
				t.Fatal("later buffered decoding changed a borrowed payload")
			}
		}
	})
}

func BenchmarkBufferedDataBatch(b *testing.B) {
	for _, payloadSize := range []int{32, 1452} {
		b.Run(strconv.Itoa(payloadSize), func(b *testing.B) {
			var encoded []byte
			for index := range 16 {
				var err error
				encoded, err = AppendDataFrame(encoded, Data{
					PacketID: 1_000_000_000 + uint64(index), DeadlineMicros: 86_405_000_000,
					Payload: make([]byte, payloadSize),
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			source := bytes.NewReader(encoded)
			reader := bufio.NewReaderSize(source, 32*1024)
			b.SetBytes(int64(16 * payloadSize))
			b.ReportAllocs()
			for b.Loop() {
				source.Reset(encoded)
				reader.Reset(source)
				reader.Peek(len(encoded))
				for range 16 {
					frame, ready, err := ReadBufferedFrame(reader)
					if err != nil || !ready {
						b.Fatalf("buffered frame: %t, %v", ready, err)
					}
					if _, err := ParseData(frame); err != nil {
						b.Fatal(err)
					}
					benchmarkFrameSink = frame
				}
			}
		})
	}
}
