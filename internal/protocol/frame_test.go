package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"
	"testing/iotest"
)

func TestFrameRoundTrip(t *testing.T) {
	want := Frame{Type: FrameDeliveryReport, Payload: []byte{1, 2, 3}}
	encoded, err := MarshalFrame(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, []byte{byte(FrameDeliveryReport), 3, 1, 2, 3}) {
		t.Fatalf("MarshalFrame() = %v", encoded)
	}

	got, err := ReadFrame(bytes.NewReader(encoded))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadFrame() = %#v, want %#v", got, want)
	}

	frames, err := ParseFrames(append(append([]byte{}, encoded...), encoded...))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(frames, []Frame{want, want}) {
		t.Fatalf("ParseFrames() = %#v", frames)
	}

	empty, err := MarshalFrame(Frame{Type: FramePing})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(empty, []byte{byte(FramePing), 0}) {
		t.Fatalf("empty MarshalFrame() = %v", empty)
	}
}

func TestAppendFrame(t *testing.T) {
	destination := []byte{9}
	encoded, err := AppendFrame(destination, Frame{Type: FramePing, Payload: []byte{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, []byte{9, byte(FramePing), 2, 1, 2}) {
		t.Fatalf("AppendFrame() = %v", encoded)
	}
}

func TestAppendFrames(t *testing.T) {
	prefix := Frame{Type: FramePing, Payload: []byte{9}}
	encoded, err := MarshalFrame(Frame{Type: FrameDeliveryReport, Payload: []byte{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	frames, err := AppendFrames([]Frame{prefix}, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || !reflect.DeepEqual(frames[0], prefix) || frames[1].Type != FrameDeliveryReport ||
		!bytes.Equal(frames[1].Payload, []byte{1, 2}) {
		t.Fatalf("AppendFrames() = %#v", frames)
	}
	frames, err = AppendFrames(frames[:1], []byte{byte(FrameDeliveryReport)})
	if !errors.Is(err, ErrTrailingFrameData) || len(frames) != 1 || !reflect.DeepEqual(frames[0], prefix) {
		t.Fatalf("AppendFrames() after invalid message = %#v, %v", frames, err)
	}
}

func TestFrameSequence(t *testing.T) {
	first := Frame{Type: FramePing, Payload: []byte{1}}
	second := Frame{Type: FrameDeliveryReport, Payload: []byte{2, 3}}
	message, err := AppendFrame(nil, first)
	if err != nil {
		t.Fatal(err)
	}
	message, err = AppendFrame(message, second)
	if err != nil {
		t.Fatal(err)
	}
	sequence, err := ParseFrameSequence(message)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []Frame{first, second} {
		got, ok := sequence.Next()
		if !ok || !reflect.DeepEqual(got, want) {
			t.Fatalf("Next() = %#v, %t, want %#v, true", got, ok, want)
		}
	}
	if got, ok := sequence.Next(); ok || got.Type != 0 || got.Payload != nil {
		t.Fatalf("final Next() = %#v, %t", got, ok)
	}

	malformed := append(append([]byte(nil), message...), byte(FramePing))
	if _, err := ParseFrameSequence(malformed); !errors.Is(err, ErrTrailingFrameData) {
		t.Fatalf("ParseFrameSequence() error = %v, want %v", err, ErrTrailingFrameData)
	}
}

func TestFrameSequenceValidationAllocations(t *testing.T) {
	frame, err := MarshalFrame(Frame{Type: FramePing})
	if err != nil {
		t.Fatal(err)
	}
	message := make([]byte, 0, MaxEncodedFrameSize)
	for len(message)+len(frame) <= cap(message) {
		message = append(message, frame...)
	}
	allocations := testing.AllocsPerRun(100, func() {
		sequence, err := ParseFrameSequence(message)
		if err != nil {
			panic(err)
		}
		for _, ok := sequence.Next(); ok; _, ok = sequence.Next() {
		}
	})
	if allocations != 0 {
		t.Fatalf("frame sequence allocations = %v, want 0", allocations)
	}
}

func TestFrameReaderReusesBuffer(t *testing.T) {
	first, err := MarshalFrame(Frame{Type: FrameDeliveryReport, Payload: []byte{1, 2, 3, 4}})
	if err != nil {
		t.Fatal(err)
	}
	second, err := MarshalFrame(Frame{Type: FramePing, Payload: []byte{5, 6}})
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(append(first, second...))
	var frameReader FrameReader
	firstFrame, err := frameReader.Read(reader)
	if err != nil {
		t.Fatal(err)
	}
	firstByte := &firstFrame.Payload[0]
	frame, err := frameReader.Read(reader)
	if err != nil {
		t.Fatal(err)
	}
	if &frame.Payload[0] != firstByte || !bytes.Equal(frame.Payload, []byte{5, 6}) {
		t.Fatalf("reused frame = %#v, capacity %d", frame, cap(frameReader.content))
	}
}

func TestFrameReaderReleasesLargeBuffer(t *testing.T) {
	large, err := MarshalFrame(Frame{
		Type: FrameData, Payload: make([]byte, maximumRetainedFrameContentCapacity+1),
	})
	if err != nil {
		t.Fatal(err)
	}
	small, err := MarshalFrame(Frame{Type: FramePing, Payload: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(append(large, small...))
	var frameReader FrameReader
	frame, err := frameReader.Read(reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(frame.Payload) != maximumRetainedFrameContentCapacity+1 ||
		cap(frameReader.content) <= maximumRetainedFrameContentCapacity {
		t.Fatalf("large frame payload length = %d, buffer capacity = %d", len(frame.Payload), cap(frameReader.content))
	}
	frame, err = frameReader.Read(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frame.Payload, []byte{1}) || cap(frameReader.content) > maximumRetainedFrameContentCapacity {
		t.Fatalf("small frame = %#v, buffer capacity = %d", frame, cap(frameReader.content))
	}
	reader = bytes.NewReader(large)
	if _, err := frameReader.Read(reader); err != nil {
		t.Fatal(err)
	}
	if _, err := frameReader.Read(reader); !errors.Is(err, io.EOF) || cap(frameReader.content) != 0 {
		t.Fatalf("EOF retained large payload: error %v, capacity %d", err, cap(frameReader.content))
	}
}

func TestReadBufferedFrame(t *testing.T) {
	encoded, err := MarshalFrame(Frame{Type: FramePing, Payload: []byte{1, 2, 3}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name      string
		encoded   []byte
		buffered  int
		available bool
		wantErr   error
	}{
		{name: "Complete", encoded: encoded, buffered: len(encoded), available: true},
		{name: "ShortHeader", encoded: encoded[:1], buffered: 1},
		{name: "ShortContent", encoded: encoded[:len(encoded)-1], buffered: len(encoded) - 1},
		{name: "TooLarge", encoded: []byte{byte(FramePing), 0x94, 0x80, 0x04}, buffered: 4, wantErr: ErrFrameTooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			reader := bufio.NewReaderSize(bytes.NewReader(tt.encoded), len(encoded))
			reader.Peek(tt.buffered)
			frame, available, err := ReadBufferedFrame(reader)
			if !errors.Is(err, tt.wantErr) || available != tt.available {
				t.Fatalf("ReadBufferedFrame() = %#v, %t, %v, want availability %t and error %v", frame, available,
					err, tt.available, tt.wantErr)
			}
			if available && (frame.Type != FramePing || !bytes.Equal(frame.Payload, []byte{1, 2, 3})) {
				t.Fatalf("ReadBufferedFrame() frame = %#v", frame)
			}
		})
	}
}

func TestFrameErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		encoded []byte
		want    error
	}{
		{name: "TooLarge", encoded: []byte{byte(FramePing), 0x94, 0x80, 0x04}, want: ErrFrameTooLarge},
		{name: "UnknownType", encoded: []byte{255, 0}, want: ErrInvalidFrameType},
		{name: "ShortHeader", encoded: []byte{byte(FramePing), 0x80}, want: io.ErrUnexpectedEOF},
		{name: "ShortContent", encoded: []byte{byte(FramePing), 2, 1}, want: io.ErrUnexpectedEOF},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(tt.encoded))
			if !errors.Is(err, tt.want) {
				t.Fatalf("ReadFrame() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestParseFramesErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		message []byte
		want    error
	}{
		{name: "TrailingPrefix", message: []byte{0}, want: ErrTrailingFrameData},
		{name: "ShortContent", message: []byte{byte(FramePing), 2, 1}, want: ErrTrailingFrameData},
		{name: "UnknownType", message: []byte{255, 0}, want: ErrInvalidFrameType},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseFrames(tt.message)
			if !errors.Is(err, tt.want) {
				t.Fatalf("ParseFrames() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDataRoundTrip(t *testing.T) {
	if Version != 1 || MaxFrameContentSize != 65_553 || MaxEncodedFrameSize != 65_557 {
		t.Fatalf("protocol limits = version %d, content %d, encoded %d", Version, MaxFrameContentSize, MaxEncodedFrameSize)
	}
	want := Data{PacketID: 11, DeadlineMicros: 123456, Payload: []byte{1, 0, 0, 0, 9}}
	frame, err := MarshalData(want)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != FrameData || !bytes.Equal(frame.Payload, []byte{11, 124, 1, 0, 0, 0, 9}) {
		t.Fatalf("MarshalData() = %#v", frame)
	}
	want.DeadlineMicros = 124000
	got, err := ParseData(frame)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseData() = %#v, %v, want %#v", got, err, want)
	}
	direct, err := MarshalDataFrame(want)
	if err != nil {
		t.Fatal(err)
	}
	generic, err := MarshalFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(direct, generic) || !bytes.Equal(direct[:2], []byte{byte(FrameData), 7}) {
		t.Fatalf("MarshalDataFrame() = %v, want %v", direct, generic)
	}
	if size, err := DataFrameSize(want); err != nil || size != len(direct) {
		t.Fatalf("DataFrameSize() = %d, %v, want %d", size, err, len(direct))
	}
}

func TestDataErrors(t *testing.T) {
	valid := Data{PacketID: 1, DeadlineMicros: 1}
	for _, tt := range []struct {
		name string
		edit func(*Data)
		want error
	}{
		{name: "ZeroPacketID", edit: func(data *Data) { data.PacketID = 0 }, want: ErrInvalidDataFrame},
		{name: "ZeroDeadline", edit: func(data *Data) { data.DeadlineMicros = 0 }, want: ErrInvalidDataFrame},
		{name: "LargePayload", edit: func(data *Data) { data.Payload = make([]byte, MaxPacketSize+1) }, want: ErrInvalidDataFrame},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := valid
			tt.edit(&data)
			_, err := MarshalData(data)
			if !errors.Is(err, tt.want) {
				t.Fatalf("MarshalData() error = %v, want %v", err, tt.want)
			}
		})
	}
	prefix := []byte{9}
	got, err := AppendDataFrame(prefix, Data{})
	if !errors.Is(err, ErrInvalidDataFrame) || !bytes.Equal(got, prefix) {
		t.Fatalf("AppendDataFrame() after invalid data = %v, %v, want unchanged prefix", got, err)
	}
}

func FuzzParseFrames(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{byte(FramePing), 0})
	f.Add([]byte{byte(FrameDeliveryReport), 1, 1})
	f.Fuzz(func(t *testing.T, message []byte) {
		frames, parseErr := ParseFrames(message)
		reader := bytes.NewReader(message)
		var decoder FrameReader
		var encoded []byte
		count := 0
		for {
			frame, err := decoder.Read(reader)
			if err != nil {
				if parseErr == nil && (err != io.EOF || count != len(frames)) {
					t.Fatalf("stream and message decoding differ: count %d, error %v", count, err)
				}
				if parseErr != nil && err == io.EOF {
					t.Fatal("stream accepted a malformed message")
				}
				break
			}
			if parseErr == nil && (count >= len(frames) || frame.Type != frames[count].Type ||
				!bytes.Equal(frame.Payload, frames[count].Payload)) {
				t.Fatal("stream and message frames differ")
			}
			encoded, err = AppendFrame(encoded, frame)
			if err != nil {
				t.Fatalf("decoded frame cannot be encoded: %v", err)
			}
			count++
		}
		if parseErr == nil && !bytes.Equal(encoded, message) {
			t.Fatal("accepted sequence differs from its canonical encoding")
		}
	})
}

func FuzzParseData(f *testing.F) {
	seed, err := MarshalData(Data{
		PacketID: 1, DeadlineMicros: 1, Payload: []byte{4, 0, 0, 0},
	})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed.Payload)
	probe, err := MarshalData(Data{Payload: make([]byte, ProbePayloadSize)})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(probe.Payload)
	f.Add([]byte(nil))
	f.Fuzz(func(t *testing.T, payload []byte) {
		data, err := ParseData(Frame{Type: FrameData, Payload: payload})
		if err != nil {
			return
		}
		encoded, err := MarshalData(data)
		if err != nil {
			t.Fatalf("MarshalData(ParseData()) error = %v", err)
		}
		if !bytes.Equal(encoded.Payload, payload) {
			t.Fatal("data-frame round trip changed a valid payload")
		}
	})
}

func TestFrameReaderLengthBoundaries(t *testing.T) {
	for _, tt := range []struct {
		name   string
		size   int
		header []byte
	}{
		{name: "Empty", header: []byte{byte(FrameData), 0}},
		{name: "OneByte", size: 127, header: []byte{byte(FrameData), 0x7f}},
		{name: "TwoBytes", size: 128, header: []byte{byte(FrameData), 0x80, 1}},
		{name: "TwoByteMaximum", size: 16383, header: []byte{byte(FrameData), 0xff, 0x7f}},
		{name: "ThreeBytes", size: 16384, header: []byte{byte(FrameData), 0x80, 0x80, 1}},
		{name: "Maximum", size: 65553, header: []byte{byte(FrameData), 0x91, 0x80, 4}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte{0xa5}, tt.size)
			encoded, err := MarshalFrame(Frame{Type: FrameData, Payload: payload})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(encoded[:len(tt.header)], tt.header) || len(encoded) != tt.size+len(tt.header) {
				t.Fatalf("encoded size or header differs from vector: %d, %v", len(encoded), encoded[:len(tt.header)])
			}
			stream := iotest.OneByteReader(bytes.NewReader(append(bytes.Clone(encoded), byte(FrameDeliveryReport), 0)))
			var decoder FrameReader
			frame, err := decoder.Read(stream)
			if err != nil || frame.Type != FrameData || !bytes.Equal(frame.Payload, payload) {
				t.Fatalf("fragmented frame differs: %v", err)
			}
			next, err := decoder.Read(stream)
			if err != nil || next.Type != FrameDeliveryReport || len(next.Payload) != 0 {
				t.Fatalf("next frame = %#v, %v", next, err)
			}
		})
	}
}

func TestReadBufferedFramePartialHeader(t *testing.T) {
	encoded, err := MarshalFrame(Frame{Type: FrameData, Payload: make([]byte, 128)})
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut <= len(encoded); cut++ {
		reader := bufio.NewReaderSize(bytes.NewReader(encoded[:cut]), len(encoded))
		reader.Peek(cut)
		frame, ready, err := ReadBufferedFrame(reader)
		if err != nil || ready != (cut == len(encoded)) {
			t.Fatalf("cut %d: ready %t, error %v", cut, ready, err)
		}
		if !ready && reader.Buffered() != cut {
			t.Fatalf("partial frame consumed bytes at cut %d", cut)
		}
		if ready && len(frame.Payload) != 128 {
			t.Fatal("complete payload changed")
		}
	}
}

func TestReadBufferedFramePreservesBorrowedPrefix(t *testing.T) {
	want := []Frame{
		{Type: FrameData, Payload: bytes.Repeat([]byte{0xa5}, 1452)},
		{Type: FramePing, Payload: bytes.Repeat([]byte{0x5a}, 16)},
	}
	var encoded []byte
	for _, frame := range want {
		var err error
		encoded, err = AppendFrame(encoded, frame)
		if err != nil {
			t.Fatal(err)
		}
	}
	encoded = append(encoded, byte(FrameData), 128)
	reader := bufio.NewReaderSize(bytes.NewReader(encoded), len(encoded))
	buffered, err := reader.Peek(len(encoded))
	if err != nil {
		t.Fatal(err)
	}
	first, ready, err := ReadBufferedFrame(reader)
	if err != nil || !ready || &first.Payload[0] != &buffered[3] {
		t.Fatalf("first frame did not borrow buffered content: %t, %v", ready, err)
	}
	second, ready, err := ReadBufferedFrame(reader)
	if err != nil || !ready {
		t.Fatalf("second frame: %t, %v", ready, err)
	}
	if _, ready, err := ReadBufferedFrame(reader); err != nil || ready || reader.Buffered() != 2 {
		t.Fatalf("partial tail: %t, %v, %d bytes", ready, err, reader.Buffered())
	}
	if !reflect.DeepEqual([]Frame{first, second}, want) {
		t.Fatal("later buffered operations invalidated the borrowed prefix")
	}
}

func TestFrameCanonicalLength(t *testing.T) {
	for _, tt := range []struct {
		name   string
		header []byte
		want   error
	}{
		{name: "NonminimalZero", header: []byte{byte(FrameDeliveryReport), 0x80, 0}, want: ErrInvalidInteger},
		{name: "NonminimalOne", header: []byte{byte(FrameDeliveryReport), 0x81, 0, 9}, want: ErrInvalidInteger},
		{name: "TooLarge", header: []byte{byte(FrameData), 0x94, 0x80, 4}, want: ErrFrameTooLarge},
		{name: "TooLong", header: []byte{byte(FrameData), 0x80, 0x80, 0x80, 0}, want: ErrFrameTooLarge},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var decoder FrameReader
			if _, err := decoder.Read(bytes.NewReader(tt.header)); !errors.Is(err, tt.want) {
				t.Fatalf("Read() = %v", err)
			}
			if cap(decoder.content) != 0 {
				t.Fatal("malformed length allocated content storage")
			}
			if _, err := ParseFrameSequence(tt.header); !errors.Is(err, tt.want) {
				t.Fatalf("ParseFrameSequence() = %v", err)
			}
			reader := bufio.NewReader(bytes.NewReader(tt.header))
			reader.Peek(len(tt.header))
			if _, _, err := ReadBufferedFrame(reader); !errors.Is(err, tt.want) {
				t.Fatalf("ReadBufferedFrame() = %v", err)
			}
		})
	}
}

func TestDataFullWidthAndReusableEncoding(t *testing.T) {
	for _, tt := range []struct {
		name         string
		id, deadline uint64
		payloadSize  int
		overhead     int
	}{
		{name: "Small", id: 1, deadline: 1, payloadSize: 32, overhead: 4},
		{name: "Ordinary", id: 8_640_000_000, deadline: 604_805_000_000, payloadSize: 1452, overhead: 13},
		{name: "FullWidth", id: math.MaxUint64, deadline: math.MaxUint64 - math.MaxUint64%1000, payloadSize: MaxPacketSize, overhead: 22},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := Data{PacketID: tt.id, DeadlineMicros: tt.deadline, Payload: bytes.Repeat([]byte{0x96}, tt.payloadSize)}
			backing := bytes.Repeat([]byte{0x5a}, MaxEncodedFrameSize+1)
			encoded, err := AppendDataFrame(backing[:1], data)
			if err != nil {
				t.Fatal(err)
			}
			if &encoded[0] != &backing[0] || encoded[0] != 0x5a || len(encoded)-1 != tt.payloadSize+tt.overhead {
				t.Fatal("encoding changed prefix, capacity, or expected size")
			}
			frames, err := ParseFrames(encoded[1:])
			if err != nil || len(frames) != 1 {
				t.Fatalf("ParseFrames() = %v", err)
			}
			data.DeadlineMicros = ((data.DeadlineMicros-1)/1000 + 1) * 1000
			got, err := ParseData(frames[0])
			if err != nil || !reflect.DeepEqual(got, data) {
				t.Fatalf("ParseData() = %#v, %v", got, err)
			}
			if len(got.Payload) != 0 && &got.Payload[0] != &frames[0].Payload[len(frames[0].Payload)-len(got.Payload)] {
				t.Fatal("parsed datagram did not borrow the original payload")
			}
		})
	}
}

func TestParseDataMalformedIntegers(t *testing.T) {
	for _, payload := range [][]byte{
		nil, {1}, {0x81, 0, 1}, {1, 0x81, 0}, {1, 0x80},
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2, 1},
	} {
		if _, err := ParseData(Frame{Type: FrameData, Payload: payload}); !errors.Is(err, ErrInvalidDataFrame) {
			t.Fatalf("ParseData(%v) = %v", payload, err)
		}
	}
}

func TestAppendFrameOverlappingPayload(t *testing.T) {
	backing := bytes.Repeat([]byte{0xa5}, 32)
	copy(backing, []byte{9, 1, 2, 3, 4})
	encoded, err := AppendFrame(backing[:1], Frame{Type: FrameDeliveryReport, Payload: backing[1:5]})
	if err != nil || !bytes.Equal(encoded, []byte{9, byte(FrameDeliveryReport), 4, 1, 2, 3, 4}) {
		t.Fatalf("overlapping AppendFrame() = %v, %v", encoded, err)
	}
}

func TestAppendDataFrameOverlappingPayload(t *testing.T) {
	backing := bytes.Repeat([]byte{0xa5}, 32)
	copy(backing, []byte{9, 1, 2, 3, 4})
	encoded, err := AppendDataFrame(backing[:1], Data{PacketID: 1, DeadlineMicros: 2, Payload: backing[1:5]})
	if err != nil || !bytes.Equal(encoded, []byte{9, byte(FrameData), 6, 1, 1, 1, 2, 3, 4}) {
		t.Fatalf("overlapping AppendDataFrame() = %v, %v", encoded, err)
	}
}
