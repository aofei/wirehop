// Package protocol implements the versioned WireHop wire protocol.
package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	// Version is the current WireHop wire protocol version.
	Version uint16 = 1
	// frameHeaderSize is the size of the frame type and content-length header.
	frameHeaderSize = 5
	// MaxFrameContentSize is the largest valid type-specific frame content.
	MaxFrameContentSize = dataHeaderSize + MaxPacketSize
	// MaxEncodedFrameSize is the largest valid frame including its common header.
	MaxEncodedFrameSize = frameHeaderSize + MaxFrameContentSize
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
	// FrameData carries one WireGuard UDP datagram.
	FrameData FrameType = iota + 1
	// FramePing requests a lane timing response.
	FramePing
	// FramePong responds to a lane timing request.
	FramePong
	// FrameClockSync updates the shared session clock mapping.
	FrameClockSync
	// FrameProbe measures lane delivery behavior without reaching the UDP target.
	FrameProbe
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
	header  [frameHeaderSize]byte
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
	destination = append(destination, make([]byte, frameHeaderSize+len(frame.Payload))...)
	destination[offset] = byte(frame.Type)
	binary.BigEndian.PutUint32(destination[offset+1:offset+frameHeaderSize], uint32(len(frame.Payload)))
	copy(destination[offset+frameHeaderSize:], frame.Payload)
	return destination, nil
}

// ReadFrame reads one complete typed and length-prefixed frame from reader.
func ReadFrame(reader io.Reader) (Frame, error) {
	var frameReader FrameReader
	return frameReader.Read(reader)
}

// Read reads one frame whose payload remains valid until the next Read call.
func (r *FrameReader) Read(reader io.Reader) (Frame, error) {
	if cap(r.content) > maximumRetainedFrameContentCapacity {
		r.content = nil
	}
	if _, err := io.ReadFull(reader, r.header[:]); err != nil {
		return Frame{}, err
	}
	typeID := FrameType(r.header[0])
	if !typeID.Valid() {
		return Frame{}, ErrInvalidFrameType
	}
	contentLength := binary.BigEndian.Uint32(r.header[1:frameHeaderSize])
	if contentLength > MaxFrameContentSize {
		return Frame{}, ErrFrameTooLarge
	}

	length := int(contentLength)
	if cap(r.content) < length {
		r.content = make([]byte, length)
	} else {
		r.content = r.content[:length]
	}
	if _, err := io.ReadFull(reader, r.content); err != nil {
		return Frame{}, err
	}
	return Frame{Type: typeID, Payload: r.content}, nil
}

// ParseFrameSequence validates one complete frame sequence and returns an allocation-free iterator whose frame payloads
// alias message.
func ParseFrameSequence(message []byte) (FrameSequence, error) {
	for remaining := message; len(remaining) > 0; {
		encodedLength, err := encodedFrameLength(remaining)
		if err != nil {
			return FrameSequence{}, err
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
	encodedLength := frameHeaderSize + int(binary.BigEndian.Uint32(message[1:frameHeaderSize]))
	frame := Frame{Type: FrameType(message[0]), Payload: message[frameHeaderSize:encodedLength]}
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

// encodedFrameLength validates the first encoded frame in message and returns its complete byte length.
func encodedFrameLength(message []byte) (int, error) {
	if len(message) < frameHeaderSize {
		return 0, ErrTrailingFrameData
	}
	if !FrameType(message[0]).Valid() {
		return 0, ErrInvalidFrameType
	}
	contentLength := binary.BigEndian.Uint32(message[1:frameHeaderSize])
	if contentLength > MaxFrameContentSize {
		return 0, ErrFrameTooLarge
	}
	encodedLength := frameHeaderSize + int(contentLength)
	if encodedLength > len(message) {
		return 0, ErrTrailingFrameData
	}
	return encodedLength, nil
}
