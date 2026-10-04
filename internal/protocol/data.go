package protocol

import (
	"encoding/binary"
	"errors"
	"slices"
)

const (
	// maximumDataHeaderSize bounds the two unsigned 64-bit metadata fields.
	maximumDataHeaderSize = 2 * binary.MaxVarintLen64
	// MaxPacketSize is the largest UDP datagram carried by WireHop.
	MaxPacketSize = 65_535
	// MaxPacketLifetimeMicros is the absolute wire-protocol packet lifetime limit.
	MaxPacketLifetimeMicros = 5 * 60 * 1_000_000
)

var (
	// ErrInvalidDataFrame indicates malformed data-frame fields or packet length.
	ErrInvalidDataFrame = errors.New("invalid data frame")
)

// Data is one WireGuard datagram and its cross-lane delivery metadata.
type Data struct {
	PacketID       uint64
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
	if data.PacketID == 0 || data.DeadlineMicros == 0 || len(data.Payload) > MaxPacketSize {
		return ErrInvalidDataFrame
	}
	return nil
}

// encodeDataPayload writes data into a validated payload-sized destination.
func encodeDataPayload(payload []byte, data Data) {
	metadataSize := uvarintSize(data.PacketID) + uvarintSize(data.DeadlineMicros)
	copy(payload[metadataSize:], data.Payload)
	offset := binary.PutUvarint(payload, data.PacketID)
	binary.PutUvarint(payload[offset:], data.DeadlineMicros)
}

// dataPayloadSize returns the exact content length of validated data.
func dataPayloadSize(data Data) int {
	return uvarintSize(data.PacketID) + uvarintSize(data.DeadlineMicros) + len(data.Payload)
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
	data.Payload = payload
	if err := validateData(data); err != nil {
		return Data{}, err
	}
	return data, nil
}
