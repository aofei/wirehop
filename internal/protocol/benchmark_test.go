package protocol

import (
	"bytes"
	"testing"
)

var benchmarkEncodingSink []byte

var benchmarkFrameSink Frame

func BenchmarkDataEncoding(b *testing.B) {
	data := Data{
		PacketID: 1, DeadlineMicros: 1, Payload: make([]byte, 1420),
	}
	b.Run("Direct", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data.Payload)))
		for b.Loop() {
			encoded, err := MarshalDataFrame(data)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkEncodingSink = encoded
		}
	})
	b.Run("Generic", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data.Payload)))
		for b.Loop() {
			frame, err := MarshalData(data)
			if err != nil {
				b.Fatal(err)
			}
			encoded, err := MarshalFrame(frame)
			if err != nil {
				b.Fatal(err)
			}
			benchmarkEncodingSink = encoded
		}
	})
}

func BenchmarkFrameReader(b *testing.B) {
	encoded, err := MarshalFrame(Frame{Type: FrameProbe, Payload: make([]byte, 1420)})
	if err != nil {
		b.Fatal(err)
	}
	reader := bytes.NewReader(encoded)
	frameReader := FrameReader{content: make([]byte, 0, 1420)}
	b.ReportAllocs()
	b.SetBytes(1420)
	for b.Loop() {
		reader.Reset(encoded)
		benchmarkFrameSink, err = frameReader.Read(reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendFrames(b *testing.B) {
	encoded, err := MarshalFrame(Frame{Type: FrameProbe, Payload: make([]byte, 1420)})
	if err != nil {
		b.Fatal(err)
	}
	frames := make([]Frame, 0, 1)
	b.ReportAllocs()
	b.SetBytes(1420)
	for b.Loop() {
		frames, err = AppendFrames(frames[:0], encoded)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkFrameSink = frames[0]
	}
}

func BenchmarkFrameSequence(b *testing.B) {
	encoded := make([]byte, 0, 16*1441)
	for range 16 {
		var err error
		encoded, err = AppendDataFrame(encoded, Data{
			PacketID: 1, DeadlineMicros: 1, Payload: make([]byte, 1420),
		})
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	for b.Loop() {
		sequence, err := ParseFrameSequence(encoded)
		if err != nil {
			b.Fatal(err)
		}
		for frame, ok := sequence.Next(); ok; frame, ok = sequence.Next() {
			benchmarkFrameSink = frame
		}
	}
}

func BenchmarkDataBatchEncoding(b *testing.B) {
	data := Data{
		PacketID: 1, DeadlineMicros: 1, Payload: make([]byte, 1420),
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(data.Payload) * 8))
	for b.Loop() {
		encoded := make([]byte, 0, 12*1024)
		for index := range 8 {
			data.PacketID = uint64(index + 1)
			var err error
			encoded, err = AppendDataFrame(encoded, data)
			if err != nil {
				b.Fatal(err)
			}
		}
		benchmarkEncodingSink = encoded
	}
}
