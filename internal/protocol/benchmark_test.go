package protocol

import (
	"bytes"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/target"
)

var benchmarkEncodingSink []byte

var benchmarkFrameSink Frame

func BenchmarkClientHelloEncoding(b *testing.B) {
	for _, tt := range []struct {
		name  string
		hello ClientHello
	}{
		{name: "Create", hello: testClientHello(HelloCreate, SessionID{}, target.MustParse("127.0.0.1:51820"))},
		{name: "Join", hello: testClientHello(HelloJoin, testSessionID(1), target.Endpoint{})},
	} {
		b.Run(tt.name, func(b *testing.B) {
			key := []byte("benchmark authentication key")
			b.ReportAllocs()
			for b.Loop() {
				if err := SignClientHello(&tt.hello, key); err != nil {
					b.Fatal(err)
				}
				encoded, err := MarshalClientHello(tt.hello)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkEncodingSink = encoded
			}
			b.ReportMetric(float64(len(benchmarkEncodingSink)), "wire-bytes/op")
		})
	}
}

func BenchmarkServerHelloEncoding(b *testing.B) {
	for _, tt := range []struct {
		name  string
		hello ServerHello
	}{
		{name: "Created", hello: ServerHello{Result: ServerSessionCreated, RequestNonce: testNonce(1),
			ServerUnixSeconds: 1_700_000_000, SessionID: testSessionID(1), SessionSecret: testSessionSecret(1),
			PathGroupID: 1, ReceiveMicros: 100, SendMicros: 110}},
		{name: "Accepted", hello: ServerHello{Result: ServerLaneAccepted, RequestNonce: testNonce(1),
			ServerUnixSeconds: 1_700_000_000, SessionID: testSessionID(1), PathGroupID: 1,
			ReceiveMicros: 100, SendMicros: 110}},
		{name: "Rejected", hello: ServerHello{Result: ServerRejected, RequestNonce: testNonce(1),
			ServerUnixSeconds: 1_700_000_000, ErrorCode: ErrorUnavailable, ErrorClass: ErrorRetryable,
			ErrorScope: ErrorScopeSession}},
		{name: "MaximumDiagnostic", hello: ServerHello{Result: ServerRejected, RequestNonce: testNonce(1),
			ServerUnixSeconds: 1_700_000_000, ErrorCode: ErrorUnavailable, ErrorClass: ErrorRetryable,
			ErrorScope: ErrorScopeSession, Diagnostic: strings.Repeat("x", MaxDiagnosticSize)}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			key := []byte("benchmark authentication key")
			b.ReportAllocs()
			for b.Loop() {
				if err := SignServerHello(&tt.hello, key); err != nil {
					b.Fatal(err)
				}
				encoded, err := MarshalServerHello(tt.hello)
				if err != nil {
					b.Fatal(err)
				}
				benchmarkEncodingSink = encoded
			}
			b.ReportMetric(float64(len(benchmarkEncodingSink)), "wire-bytes/op")
		})
	}
}

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
	encoded, err := MarshalFrame(Frame{Type: FrameDeliveryReport, Payload: make([]byte, 1420)})
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
	encoded, err := MarshalFrame(Frame{Type: FrameDeliveryReport, Payload: make([]byte, 1420)})
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
	data := Data{PacketID: 1, DeadlineMicros: 1, Payload: make([]byte, 1420)}
	size, err := DataFrameSize(data)
	if err != nil {
		b.Fatal(err)
	}
	encoded := make([]byte, 0, 16*size)
	for index := range 16 {
		data.PacketID = uint64(index + 1)
		encoded, err = AppendDataFrame(encoded, data)
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
