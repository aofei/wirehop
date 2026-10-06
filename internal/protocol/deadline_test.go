package protocol

import (
	"encoding/binary"
	"errors"
	"math"
	"testing"
)

func TestDataDeadlineResolution(t *testing.T) {
	for _, micros := range []uint64{1, 999, 1000, 1001, 127000, 127001, 128000, math.MaxUint64 - math.MaxUint64%1000} {
		frame, err := MarshalData(Data{PacketID: 1, DeadlineMicros: micros})
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParseData(frame)
		if err != nil || got.DeadlineMicros < micros || got.DeadlineMicros-micros >= 1000 || got.DeadlineMicros%1000 != 0 {
			t.Fatalf("deadline %d encoded as %d, error %v", micros, got.DeadlineMicros, err)
		}
	}
	if _, err := MarshalData(Data{PacketID: 1, DeadlineMicros: math.MaxUint64}); !errors.Is(err, ErrInvalidDataFrame) {
		t.Fatal(err)
	}
	payload := binary.AppendUvarint([]byte{1}, math.MaxUint64/1000+1)
	if _, err := ParseData(Frame{Type: FrameData, Payload: payload}); !errors.Is(err, ErrInvalidDataFrame) {
		t.Fatal(err)
	}
}
