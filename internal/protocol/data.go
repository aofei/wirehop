package protocol

import (
	"encoding/binary"
	"errors"
)

const (
	// dataHeaderSize is the encoded data-frame header size.
	dataHeaderSize = 16
	// DataFrameOverhead is the complete per-datagram frame overhead excluding the WireGuard payload.
	DataFrameOverhead = frameHeaderSize + dataHeaderSize
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

	payload := make([]byte, dataHeaderSize+len(data.Payload))
	encodeDataPayload(payload, data)
	return Frame{Type: FrameData, Payload: payload}, nil
}

// DataFrameSize validates data and returns its complete encoded wire size.
func DataFrameSize(data Data) (int, error) {
	if err := validateData(data); err != nil {
		return 0, err
	}
	return DataFrameOverhead + len(data.Payload), nil
}

// MarshalDataFrame returns one complete data-frame encoding without an intermediate payload copy.
func MarshalDataFrame(data Data) ([]byte, error) {
	return AppendDataFrame(nil, data)
}

// AppendDataFrame appends one complete data frame to destination.
func AppendDataFrame(destination []byte, data Data) ([]byte, error) {
	size, err := DataFrameSize(data)
	if err != nil {
		return destination, err
	}
	start := len(destination)
	destination = append(destination, make([]byte, size)...)
	encoded := destination[start:]
	encoded[0] = byte(FrameData)
	binary.BigEndian.PutUint32(encoded[1:frameHeaderSize], uint32(size-frameHeaderSize))
	encodeDataPayload(encoded[frameHeaderSize:], data)
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
	binary.BigEndian.PutUint64(payload[0:8], data.PacketID)
	binary.BigEndian.PutUint64(payload[8:16], data.DeadlineMicros)
	copy(payload[dataHeaderSize:], data.Payload)
}

// ParseData parses and validates one generic data frame.
func ParseData(frame Frame) (Data, error) {
	if frame.Type != FrameData || len(frame.Payload) < dataHeaderSize {
		return Data{}, ErrInvalidDataFrame
	}

	data := Data{
		PacketID:       binary.BigEndian.Uint64(frame.Payload[0:8]),
		DeadlineMicros: binary.BigEndian.Uint64(frame.Payload[8:16]),
		Payload:        frame.Payload[dataHeaderSize:],
	}
	if err := validateData(data); err != nil {
		return Data{}, err
	}
	return data, nil
}
