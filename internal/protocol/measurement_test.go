package protocol

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestMeasurementWireLayout(t *testing.T) {
	for _, test := range []struct {
		name    string
		value   Measurement
		encoded string
	}{
		{name: "Request", value: Measurement{Kind: MeasurementRequest}, encoded: "1801"},
		{name: "Begin", value: Measurement{Kind: MeasurementBegin, ID: 128, Targets: []LaneGeneration{{LaneID: 1, Generation: 1}}}, encoded: "68028001010101"},
		{name: "Ready", value: Measurement{Kind: MeasurementReady, ID: 1, Valid: true}, encoded: "38030101"},
		{name: "Probe", value: Measurement{Kind: MeasurementProbe, ID: 1, ProbeMicros: 127, HoldMicros: 128}, encoded: "5804017f8001"},
		{name: "Reply", value: Measurement{Kind: MeasurementReply, ID: 1, ProbeMicros: 127, ReceiveMicros: 128,
			ReplyMicros: 16384, HoldMicros: 16384}, encoded: "b80105017f8001808001808001"},
		{name: "Result", value: Measurement{Kind: MeasurementResult, ID: 128, Valid: true, ReceiveMicros: 16384}, encoded: "7806800101808001"},
		{name: "Rejected", value: Measurement{Kind: MeasurementResult, ID: 1}, encoded: "4806010000"},
	} {
		t.Run(test.name, func(t *testing.T) {
			literal, err := hex.DecodeString(test.encoded)
			if err != nil {
				t.Fatal(err)
			}
			frames, err := ParseFrames(literal)
			if err != nil || len(frames) != 1 {
				t.Fatalf("literal framing: %v, %v", frames, err)
			}
			value, err := ParseMeasurement(frames[0])
			if err != nil || !reflect.DeepEqual(value, test.value) {
				t.Fatalf("literal measurement = %+v, %v", value, err)
			}
			frame, err := MarshalMeasurement(test.value)
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := MarshalFrame(frame)
			if err != nil || !bytes.Equal(encoded, literal) {
				t.Fatalf("measurement encoding = %x, %v, want %x", encoded, err, literal)
			}
			for index := range len(frame.Payload) {
				if _, err := ParseMeasurement(Frame{Type: FrameMeasurement, Payload: frame.Payload[:index]}); err == nil {
					t.Fatalf("accepted truncated phase at byte %d", index)
				}
			}
			if _, err := ParseMeasurement(Frame{Type: FrameMeasurement, Payload: append(bytes.Clone(frame.Payload), 0)}); err == nil {
				t.Fatal("accepted trailing phase fields")
			}
		})
	}
}

func TestMeasurementValidation(t *testing.T) {
	for _, value := range []Measurement{
		{}, {Kind: 7, ID: 1}, {Kind: MeasurementBegin}, {Kind: MeasurementRequest, ID: 1},
		{Kind: MeasurementProbe, ID: 1},
		{Kind: MeasurementProbe, ID: 1, HoldMicros: MaxMeasurementHoldMicros + 1},
		{Kind: MeasurementReply, ID: 1, ReceiveMicros: 2, ReplyMicros: 1},
		{Kind: MeasurementResult, ID: 1, ReceiveMicros: 1},
		{Kind: MeasurementBegin, ID: 1, HoldMicros: 1},
		{Kind: MeasurementRequest, ProbeMicros: 1},
		{Kind: MeasurementProbe, ID: 1, HoldMicros: 1, Valid: true},
		{Kind: MeasurementReply, ID: 1, Valid: true},
	} {
		if _, err := MarshalMeasurement(value); !errors.Is(err, ErrInvalidControlFrame) {
			t.Fatalf("accepted invalid measurement %+v: %v", value, err)
		}
	}
	for _, payload := range [][]byte{
		{0}, {7}, {2, 0}, {2, 0x81, 0}, {2, 0x80}, {6, 1, 2, 0}, {6, 1, 0, 1},
		{4, 1, 0, 0}, {5, 1, 0, 2, 1, 1}, {0x81, 0},
	} {
		if _, err := ParseMeasurement(Frame{Type: FrameMeasurement, Payload: payload}); !errors.Is(err, ErrInvalidControlFrame) {
			t.Fatalf("accepted invalid measurement %x: %v", payload, err)
		}
	}
	if _, err := ParseMeasurement(Frame{Type: FramePing, Payload: []byte{1}}); err == nil {
		t.Fatal("accepted a different frame type")
	}
}

func TestMeasurementTargets(t *testing.T) {
	for _, targets := range [][]LaneGeneration{
		nil, {{LaneID: 0, Generation: 1}}, {{LaneID: 1}},
		{{LaneID: 1, Generation: 1}, {LaneID: 1, Generation: 2}},
		{{LaneID: 2, Generation: 1}, {LaneID: 1, Generation: 1}},
		make([]LaneGeneration, MaxMeasurementTargets+1),
	} {
		if _, err := MarshalMeasurement(Measurement{Kind: MeasurementBegin, ID: 1, Targets: targets}); err == nil {
			t.Fatal("accepted an empty, unordered, duplicate, or unbounded target set")
		}
	}
	targets := make([]LaneGeneration, MaxMeasurementTargets)
	for index := range targets {
		targets[index] = LaneGeneration{LaneID: LaneID(uint64(math.MaxUint64-MaxMeasurementTargets) + uint64(index)), Generation: math.MaxUint64}
	}
	frame, err := MarshalMeasurement(Measurement{Kind: MeasurementBegin, ID: math.MaxUint64, Targets: targets})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MarshalFrame(frame); err != nil || len(frame.Payload) > MaxFrameContentSize {
		t.Fatalf("maximum target set exceeds frame bound: %d, %v", len(frame.Payload), err)
	}
	parsed, err := ParseMeasurement(frame)
	if err != nil || !reflect.DeepEqual(parsed.Targets, targets) {
		t.Fatal("maximum full-width target set failed to round trip")
	}
	for _, fields := range [][]uint64{
		{2, 1, math.MaxUint64}, {2, 1, MaxMeasurementTargets + 1}, {2, 1, 0},
		{2, 1, 2, 1, 1, 1, 2}, {2, 1, 2, 2, 1, 1, 1}, {2, 1, 1, 1, 0},
	} {
		if _, err := ParseMeasurement(Frame{Type: FrameMeasurement, Payload: integerPayload(0, fields...)}); err == nil {
			t.Fatal("accepted an invalid wire target set")
		}
	}
}

func FuzzParseMeasurement(f *testing.F) {
	for _, payload := range [][]byte{{1}, {2, 1, 1, 1, 1}, {3, 1, 1}, {4, 1, 0, 1}, {5, 1, 0, 0, 0, 1}, {6, 1, 1, 0}, {6, 1, 0, 0}} {
		f.Add(payload)
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		value, err := ParseMeasurement(Frame{Type: FrameMeasurement, Payload: payload})
		if err != nil {
			return
		}
		frame, err := MarshalMeasurement(value)
		if err != nil || !bytes.Equal(frame.Payload, payload) {
			t.Fatalf("noncanonical accepted phase %x: %+v, %v", payload, value, err)
		}
	})
}
