package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestDataCapacityProbe(t *testing.T) {
	padding := make([]byte, ProbePayloadSize)
	for index := range padding {
		padding[index] = byte(index)
	}
	data := Data{Payload: padding}
	frame, err := MarshalData(data)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := ParseData(frame)
	if err != nil || decoded.PacketID != 0 || decoded.DeadlineMicros != 0 || !bytes.Equal(decoded.Payload, padding) {
		t.Fatalf("probe round trip = %+v, %v", decoded, err)
	}
	encoded, err := AppendDataFrame([]byte{1, 2}, data)
	if err != nil {
		t.Fatal(err)
	}
	size, err := DataFrameSize(data)
	if err != nil || size != len(encoded)-2 || size != 4101 {
		t.Fatalf("probe size = %d, %v, encoded %d", size, err, len(encoded)-2)
	}
	parsedFrame, err := ReadFrame(bytes.NewReader(encoded[2:]))
	if err != nil || parsedFrame.Type != FrameData || !bytes.Equal(parsedFrame.Payload, frame.Payload) {
		t.Fatalf("probe envelope = %+v, %v", parsedFrame, err)
	}
	for _, test := range []struct {
		name string
		data Data
	}{
		{name: "NonzeroDeadline", data: Data{DeadlineMicros: 1, Payload: padding}},
		{name: "ShortPadding", data: Data{Payload: padding[:len(padding)-1]}},
		{name: "LongPadding", data: Data{Payload: make([]byte, ProbePayloadSize+1)}},
		{name: "MissingPadding"},
		{name: "MissingPacketDeadline", data: Data{PacketID: 1, Payload: padding}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := MarshalData(test.data); !errors.Is(err, ErrInvalidDataFrame) {
				t.Fatalf("invalid probe = %v", err)
			}
			if _, err := DataFrameSize(test.data); !errors.Is(err, ErrInvalidDataFrame) {
				t.Fatalf("invalid probe size = %v", err)
			}
		})
	}
}
