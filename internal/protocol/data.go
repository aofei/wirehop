package protocol

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"time"
)

const (
	// maximumDataHeaderSize bounds the packet ID and the representable millisecond deadline.
	maximumDataHeaderSize = binary.MaxVarintLen64 + 8
	// DeadlineResolutionMicros is the wire deadline precision. Encoding rounds up by at most one unit minus one.
	DeadlineResolutionMicros = uint64(1000)
	// MaxPacketSize is the largest UDP datagram carried by WireHop.
	MaxPacketSize = 65_535
	// MaxPacketLifetimeMicros is the absolute wire-protocol packet lifetime limit.
	MaxPacketLifetimeMicros = uint64(5 * time.Minute / time.Microsecond)
)

var (
	// ErrInvalidDataFrame indicates malformed data-frame fields or packet length.
	ErrInvalidDataFrame = errors.New("invalid data frame")
)

// Data is one WireGuard datagram and its cross-lane delivery metadata.
type Data struct {
	PacketID uint64
	// DeadlineMicros uses the runtime clock precision and rounds up to milliseconds on the wire.
	DeadlineMicros uint64
	Payload        []byte
}

// MarshalData returns a generic frame containing data.
func MarshalData(data Data) (Frame, error) {
	if err := validateData(data); err != nil {
		return Frame{}, err
	}

	payload := make([]byte, dataPayloadSize(data))
	encodeDataPayload(payload, data)
	return Frame{Type: FrameData, Payload: payload}, nil
}

// DataFrameSize validates data and returns its complete encoded wire size.
func DataFrameSize(data Data) (int, error) {
	if err := validateData(data); err != nil {
		return 0, err
	}
	return FrameSize(dataPayloadSize(data)), nil
}

// MarshalDataFrame returns one complete data-frame encoding without an intermediate payload copy.
func MarshalDataFrame(data Data) ([]byte, error) {
	return AppendDataFrame(nil, data)
}

// AppendDataFrame appends one complete data frame to destination.
func AppendDataFrame(destination []byte, data Data) ([]byte, error) {
	if err := validateData(data); err != nil {
		return destination, err
	}
	contentSize := dataPayloadSize(data)
	size := FrameSize(contentSize)
	start := len(destination)
	destination = slices.Grow(destination, size)
	destination = destination[:start+size]
	encoded := destination[start:]
	headerSize := size - contentSize
	encodeDataPayload(encoded[headerSize:], data)
	encoded[0] = byte(FrameData)
	binary.PutUvarint(encoded[1:], uint64(contentSize))
	return destination, nil
}

// validateData verifies all data-frame metadata and packet bounds.
func validateData(data Data) error {
	if data.PacketID == 0 || data.DeadlineMicros == 0 ||
		data.DeadlineMicros > math.MaxUint64-math.MaxUint64%DeadlineResolutionMicros || len(data.Payload) > MaxPacketSize {
		return ErrInvalidDataFrame
	}
	return nil
}

// encodeDataPayload writes data into a validated payload-sized destination.
func encodeDataPayload(payload []byte, data Data) {
	metadataSize := uvarintSize(data.PacketID) + uvarintSize(deadlineMillis(data.DeadlineMicros))
	copy(payload[metadataSize:], data.Payload)
	offset := binary.PutUvarint(payload, data.PacketID)
	binary.PutUvarint(payload[offset:], deadlineMillis(data.DeadlineMicros))
}

// dataPayloadSize returns the exact content length of validated data.
func dataPayloadSize(data Data) int {
	return uvarintSize(data.PacketID) + uvarintSize(deadlineMillis(data.DeadlineMicros)) + len(data.Payload)
}

// ParseData parses and validates one generic data frame.
func ParseData(frame Frame) (Data, error) {
	if frame.Type != FrameData {
		return Data{}, ErrInvalidDataFrame
	}
	var data Data
	payload, err := parseIntegers(frame.Payload, &data.PacketID, &data.DeadlineMicros)
	if err != nil {
		return Data{}, ErrInvalidDataFrame
	}
	if data.DeadlineMicros > math.MaxUint64/DeadlineResolutionMicros {
		return Data{}, ErrInvalidDataFrame
	}
	data.DeadlineMicros *= DeadlineResolutionMicros
	data.Payload = payload
	if err := validateData(data); err != nil {
		return Data{}, err
	}
	return data, nil
}

// deadlineMillis rounds a validated runtime deadline up without unsigned overflow.
func deadlineMillis(micros uint64) uint64 {
	return (micros-1)/DeadlineResolutionMicros + 1
}
