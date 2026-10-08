// Package protocol implements the versioned WireHop wire protocol.
package protocol

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"slices"
)

const (
	// Version is the current WireHop wire protocol version.
	Version uint16 = 1
	// maximumFrameHeaderSize bounds the packed type and content length through MaxFrameContentSize.
	maximumFrameHeaderSize = 3
	// MaxFrameContentSize is the largest valid type-specific frame content.
	MaxFrameContentSize = maximumDataHeaderSize + MaxPacketSize
	// MaxEncodedFrameSize is the largest valid frame including its common header.
	MaxEncodedFrameSize = maximumFrameHeaderSize + MaxFrameContentSize
	// maximumRetainedFrameContentCapacity preserves ordinary packets without retaining exceptional high-water marks.
	maximumRetainedFrameContentCapacity = 32 * 1024
)

var (
	// ErrInvalidFrameType indicates an unknown or unset frame type.
	ErrInvalidFrameType = errors.New("invalid frame type")
	// ErrFrameTooLarge indicates a frame above the absolute protocol limit.
	ErrFrameTooLarge = errors.New("frame too large")
	// ErrTrailingFrameData indicates bytes remaining after a complete frame sequence.
	ErrTrailingFrameData = errors.New("trailing frame data")
)

// FrameType identifies one data-plane or control-plane frame.
type FrameType uint8

const (
	// FrameData carries one WireGuard UDP datagram or fixed capacity padding.
	FrameData FrameType = iota + 1
	// FramePing requests a lane timing response.
	FramePing
	// FramePong responds to a lane timing request.
	FramePong
	// FrameClockSync updates the shared session clock mapping.
	FrameClockSync
	// FrameDeliveryReport reports cumulative peer parsing progress.
	FrameDeliveryReport
	// FrameSessionCreated accepts a newly created session.
	FrameSessionCreated
	// FrameLaneAccepted accepts a lane joined to an existing session.
	FrameLaneAccepted
	// FrameSessionClose explicitly closes a session.
	FrameSessionClose
	// FrameLaneAbandon coordinates generation-specific connection abandonment.
	FrameLaneAbandon
	// FrameError reports an in-session protocol or policy error.
	FrameError
)

// Valid reports whether the frame type is defined by this protocol version.
func (t FrameType) Valid() bool {
	return t >= FrameData && t <= FrameError
}

// Frame is one decoded WireHop frame. Callers must treat Payload as read-only and honor the lifetime documented by the
// decoder that returned it.
type Frame struct {
	Type    FrameType
	Payload []byte
}

// FrameReader incrementally decodes stream frames with connection-local reusable storage.
type FrameReader struct {
	header  [maximumFrameHeaderSize]byte
	content []byte
}

// FrameSequence iterates over one completely validated frame sequence without allocating per-frame metadata. The
// source message must remain unchanged while the sequence or any returned frame is in use.
type FrameSequence struct {
	message []byte
	offset  int
}

// MarshalFrame returns the typed and length-prefixed binary encoding of frame.
func MarshalFrame(frame Frame) ([]byte, error) {
	return AppendFrame(nil, frame)
}

// AppendFrame appends the typed and length-prefixed binary encoding of frame to destination.
func AppendFrame(destination []byte, frame Frame) ([]byte, error) {
	if !frame.Type.Valid() {
		return destination, ErrInvalidFrameType
	}
	if len(frame.Payload) > MaxFrameContentSize {
		return destination, ErrFrameTooLarge
	}

	offset := len(destination)
	size := FrameSize(len(frame.Payload))
	destination = slices.Grow(destination, size)
	destination = destination[:offset+size]
	headerSize := size - len(frame.Payload)
	copy(destination[offset+headerSize:], frame.Payload)
	binary.PutUvarint(destination[offset:], uint64(len(frame.Payload))<<4|uint64(frame.Type))
	return destination, nil
}

// FrameSize returns the complete canonical encoded size for a validated frame-content length.
func FrameSize(contentSize int) int {
	return uvarintSize(uint64(contentSize)<<4) + contentSize
}

// ReadFrame reads one complete typed and length-prefixed frame from reader.
func ReadFrame(reader io.Reader) (Frame, error) {
	var frameReader FrameReader
	return frameReader.Read(reader)
}

// Read reads one frame whose payload remains valid until the next Read or Reset call.
func (r *FrameReader) Read(reader io.Reader) (Frame, error) {
	r.Reset()
	if _, err := io.ReadFull(reader, r.header[:1]); err != nil {
		return Frame{}, err
	}
	headerSize := 1
	for r.header[headerSize-1] >= 0x80 && headerSize < len(r.header) {
		if _, err := io.ReadFull(reader, r.header[headerSize:headerSize+1]); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return Frame{}, err
		}
		headerSize++
	}
	length, _, err := frameHeader(r.header[:headerSize])
	if err != nil {
		return Frame{}, err
	}
	return r.readContent(reader, FrameType(r.header[0]&0x0f), length)
}

// readContent reads a validated frame body into reusable connection-local storage.
func (r *FrameReader) readContent(reader io.Reader, typeID FrameType, length int) (Frame, error) {
	if cap(r.content) < length {
		r.content = make([]byte, length)
	} else {
		r.content = r.content[:length]
	}
	if _, err := io.ReadFull(reader, r.content); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return Frame{}, err
	}
	return Frame{Type: typeID, Payload: r.content}, nil
}

// ReadBufferedFrame borrows one complete frame from reader without reading or refilling it. Returned payloads remain
// valid until an operation refills or resets reader. Further ReadBufferedFrame calls preserve previous payloads.
func ReadBufferedFrame(reader *bufio.Reader) (Frame, bool, error) {
	buffered := reader.Buffered()
	if buffered == 0 {
		return Frame{}, false, nil
	}
	encoded, err := reader.Peek(buffered)
	if err != nil {
		return Frame{}, false, err
	}
	contentLength, headerSize, err := frameHeader(encoded)
	if err != nil {
		if err == ErrTrailingFrameData {
			return Frame{}, false, nil
		}
		return Frame{}, false, err
	}
	encodedLength := headerSize + contentLength
	if buffered < encodedLength {
		return Frame{}, false, nil
	}
	frame := Frame{Type: FrameType(encoded[0] & 0x0f), Payload: encoded[headerSize:encodedLength]}
	reader.Discard(encodedLength)
	return frame, true, nil
}

// Reset invalidates the previous payload and releases exceptional historical capacity while preserving ordinary buffers.
func (r *FrameReader) Reset() {
	if cap(r.content) > maximumRetainedFrameContentCapacity {
		r.content = nil
	} else {
		r.content = r.content[:0]
	}
}

// ParseFrameSequence validates one complete frame sequence and returns an allocation-free iterator whose frame payloads
// alias message.
func ParseFrameSequence(message []byte) (FrameSequence, error) {
	for remaining := message; len(remaining) > 0; {
		contentLength, headerSize, err := frameHeader(remaining)
		if err != nil {
			return FrameSequence{}, err
		}
		encodedLength := headerSize + contentLength
		if encodedLength > len(remaining) {
			return FrameSequence{}, ErrTrailingFrameData
		}
		remaining = remaining[encodedLength:]
	}
	return FrameSequence{message: message}, nil
}

// Next returns the next frame whose payload aliases the validated message.
func (s *FrameSequence) Next() (Frame, bool) {
	if s.offset == len(s.message) {
		return Frame{}, false
	}
	message := s.message[s.offset:]
	header, headerSize := binary.Uvarint(message)
	encodedLength := headerSize + int(header>>4)
	frame := Frame{Type: FrameType(header & 0x0f), Payload: message[headerSize:encodedLength]}
	s.offset += encodedLength
	return frame, true
}

// ParseFrames parses complete frames whose payloads alias message.
func ParseFrames(message []byte) ([]Frame, error) {
	frames, err := AppendFrames(nil, message)
	if err != nil {
		return nil, err
	}
	return frames, nil
}

// AppendFrames appends complete frames whose payloads alias message. On error, it returns destination at its original
// length.
func AppendFrames(destination []Frame, message []byte) ([]Frame, error) {
	sequence, err := ParseFrameSequence(message)
	if err != nil {
		return destination, err
	}
	for frame, ok := sequence.Next(); ok; frame, ok = sequence.Next() {
		destination = append(destination, frame)
	}
	return destination, nil
}

// frameHeader validates one bounded header and returns its content length and header size.
func frameHeader(message []byte) (int, int, error) {
	if len(message) == 0 {
		return 0, 0, ErrTrailingFrameData
	}
	header := uint32(message[0])
	width := 1
	if header >= 0x80 {
		if len(message) < 2 {
			return 0, 0, ErrTrailingFrameData
		}
		header = header&0x7f | uint32(message[1])<<7
		width = 2
		if message[1] >= 0x80 {
			if len(message) < 3 {
				return 0, 0, ErrTrailingFrameData
			}
			if message[2] >= 0x80 {
				return 0, 0, ErrFrameTooLarge
			}
			header = header&0x3fff | uint32(message[2])<<14
			width = 3
		}
		if message[width-1] == 0 {
			return 0, 0, ErrInvalidInteger
		}
	}
	if !FrameType(header & 0x0f).Valid() {
		return 0, 0, ErrInvalidFrameType
	}
	contentLength := header >> 4
	if contentLength > MaxFrameContentSize {
		return 0, 0, ErrFrameTooLarge
	}
	return int(contentLength), width, nil
}
