package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	want := Frame{Type: FrameProbe, Payload: []byte{1, 2, 3}}
	encoded, err := MarshalFrame(want)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, []byte{byte(FrameProbe), 0, 0, 0, 3, 1, 2, 3}) {
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
	if !bytes.Equal(empty, []byte{byte(FramePing), 0, 0, 0, 0}) {
		t.Fatalf("empty MarshalFrame() = %v", empty)
	}
}

func TestAppendFrame(t *testing.T) {
	destination := []byte{9}
	encoded, err := AppendFrame(destination, Frame{Type: FramePing, Payload: []byte{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, []byte{9, byte(FramePing), 0, 0, 0, 2, 1, 2}) {
		t.Fatalf("AppendFrame() = %v", encoded)
	}
}

func TestAppendFrames(t *testing.T) {
	prefix := Frame{Type: FramePing, Payload: []byte{9}}
	encoded, err := MarshalFrame(Frame{Type: FrameProbe, Payload: []byte{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	frames, err := AppendFrames([]Frame{prefix}, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || !reflect.DeepEqual(frames[0], prefix) || frames[1].Type != FrameProbe ||
		!bytes.Equal(frames[1].Payload, []byte{1, 2}) {
		t.Fatalf("AppendFrames() = %#v", frames)
	}
	frames, err = AppendFrames(frames[:1], []byte{byte(FrameProbe)})
	if !errors.Is(err, ErrTrailingFrameData) || len(frames) != 1 || !reflect.DeepEqual(frames[0], prefix) {
		t.Fatalf("AppendFrames() after invalid message = %#v, %v", frames, err)
	}
}

func TestFrameSequence(t *testing.T) {
	first := Frame{Type: FramePing, Payload: []byte{1}}
	second := Frame{Type: FrameProbe, Payload: []byte{2, 3}}
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
	first, err := MarshalFrame(Frame{Type: FrameProbe, Payload: []byte{1, 2, 3, 4}})
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
}

func TestFrameErrors(t *testing.T) {
	for _, tt := range []struct {
		name    string
		encoded []byte
		want    error
	}{
		{name: "TooLarge", encoded: []byte{byte(FramePing), 0, 1, 0, 16}, want: ErrFrameTooLarge},
		{name: "UnknownType", encoded: []byte{255, 0, 0, 0, 0}, want: ErrInvalidFrameType},
		{name: "ShortHeader", encoded: []byte{byte(FramePing), 0, 0, 0}, want: io.ErrUnexpectedEOF},
		{name: "ShortContent", encoded: []byte{byte(FramePing), 0, 0, 0, 2, 1}, want: io.ErrUnexpectedEOF},
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
		{name: "ShortContent", message: []byte{byte(FramePing), 0, 0, 0, 2, 1}, want: ErrTrailingFrameData},
		{name: "UnknownType", message: []byte{255, 0, 0, 0, 0}, want: ErrInvalidFrameType},
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
	if DataFrameOverhead != 21 || MaxFrameContentSize != 65_551 || MaxEncodedFrameSize != 65_556 {
		t.Fatalf("data layout constants = overhead %d, content %d, encoded %d",
			DataFrameOverhead, MaxFrameContentSize, MaxEncodedFrameSize)
	}
	want := Data{
		PacketID:       11,
		DeadlineMicros: 123456,
		Payload:        []byte{1, 0, 0, 0, 9},
	}
	frame, err := MarshalData(want)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Type != FrameData || len(frame.Payload) != dataHeaderSize+len(want.Payload) {
		t.Fatalf("MarshalData() = %#v", frame)
	}
	if got := binary.BigEndian.Uint64(frame.Payload[0:8]); got != want.PacketID {
		t.Fatalf("encoded packet ID = %d", got)
	}
	if got := binary.BigEndian.Uint64(frame.Payload[8:16]); got != want.DeadlineMicros {
		t.Fatalf("encoded deadline = %d", got)
	}

	got, err := ParseData(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseData() = %#v, want %#v", got, want)
	}
	direct, err := MarshalDataFrame(want)
	if err != nil {
		t.Fatal(err)
	}
	generic, err := MarshalFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(direct, generic) {
		t.Fatalf("MarshalDataFrame() = %v, want %v", direct, generic)
	}
	if direct[0] != byte(FrameData) ||
		binary.BigEndian.Uint32(direct[1:frameHeaderSize]) != uint32(dataHeaderSize+len(want.Payload)) {
		t.Fatalf("data frame header = %v", direct[:frameHeaderSize])
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
	f.Add([]byte{byte(FramePing), 0, 0, 0, 0})
	f.Add([]byte{byte(FrameProbe), 0, 0, 0, 1, 1})
	f.Fuzz(func(t *testing.T, message []byte) {
		frames, err := ParseFrames(message)
		if err != nil {
			return
		}
		for _, frame := range frames {
			if !frame.Type.Valid() || len(frame.Payload) > MaxFrameContentSize {
				t.Fatalf("ParseFrames() returned invalid frame %#v", frame)
			}
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
