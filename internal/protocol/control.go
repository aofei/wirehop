package protocol

import (
	"encoding/binary"
	"errors"
)

var (
	// ErrInvalidControlFrame indicates malformed control-frame fields or length.
	ErrInvalidControlFrame = errors.New("invalid control frame")
)

// TimingPing requests one lane-local timing observation.
type TimingPing struct {
	ID         uint64
	SendMicros uint64
}

// MarshalTimingPing returns a timing ping frame.
func MarshalTimingPing(ping TimingPing) (Frame, error) {
	if ping.ID == 0 {
		return Frame{}, ErrInvalidControlFrame
	}
	payload := integerPayload(0, ping.ID, ping.SendMicros)
	return Frame{Type: FramePing, Payload: payload}, nil
}

// ParseTimingPing parses a timing ping frame.
func ParseTimingPing(frame Frame) (TimingPing, error) {
	if frame.Type != FramePing {
		return TimingPing{}, ErrInvalidControlFrame
	}
	var ping TimingPing
	remaining, err := parseIntegers(frame.Payload, &ping.ID, &ping.SendMicros)
	if err != nil || len(remaining) != 0 {
		return TimingPing{}, ErrInvalidControlFrame
	}
	if ping.ID == 0 {
		return TimingPing{}, ErrInvalidControlFrame
	}
	return ping, nil
}

// TimingPong responds with receiver timing for a prior ping.
type TimingPong struct {
	ID             uint64
	PingSendMicros uint64
	ReceiveMicros  uint64
	SendMicros     uint64
}

// MarshalTimingPong returns a timing pong frame.
func MarshalTimingPong(pong TimingPong) (Frame, error) {
	if pong.ID == 0 || pong.ReceiveMicros > pong.SendMicros {
		return Frame{}, ErrInvalidControlFrame
	}
	payload := integerPayload(0, pong.ID, pong.PingSendMicros, pong.ReceiveMicros, pong.SendMicros)
	return Frame{Type: FramePong, Payload: payload}, nil
}

// ParseTimingPong parses a timing pong frame.
func ParseTimingPong(frame Frame) (TimingPong, error) {
	if frame.Type != FramePong {
		return TimingPong{}, ErrInvalidControlFrame
	}
	var pong TimingPong
	remaining, err := parseIntegers(frame.Payload, &pong.ID, &pong.PingSendMicros, &pong.ReceiveMicros, &pong.SendMicros)
	if err != nil || len(remaining) != 0 {
		return TimingPong{}, ErrInvalidControlFrame
	}
	if pong.ID == 0 || pong.ReceiveMicros > pong.SendMicros {
		return TimingPong{}, ErrInvalidControlFrame
	}
	return pong, nil
}

// ClockSync carries one complete four-timestamp clock sample.
type ClockSync struct {
	ClientSendMicros    uint64
	ServerReceiveMicros uint64
	ServerSendMicros    uint64
	ClientReceiveMicros uint64
}

// MarshalClockSync returns a clock synchronization frame.
func MarshalClockSync(sync ClockSync) (Frame, error) {
	if sync.ClientReceiveMicros < sync.ClientSendMicros || sync.ServerSendMicros < sync.ServerReceiveMicros {
		return Frame{}, ErrInvalidControlFrame
	}
	payload := integerPayload(0, sync.ClientSendMicros, sync.ServerReceiveMicros, sync.ServerSendMicros, sync.ClientReceiveMicros)
	return Frame{Type: FrameClockSync, Payload: payload}, nil
}

// ParseClockSync parses a clock synchronization frame.
func ParseClockSync(frame Frame) (ClockSync, error) {
	if frame.Type != FrameClockSync {
		return ClockSync{}, ErrInvalidControlFrame
	}
	var sync ClockSync
	remaining, err := parseIntegers(frame.Payload, &sync.ClientSendMicros, &sync.ServerReceiveMicros, &sync.ServerSendMicros, &sync.ClientReceiveMicros)
	if err != nil || len(remaining) != 0 {
		return ClockSync{}, ErrInvalidControlFrame
	}
	if sync.ClientReceiveMicros < sync.ClientSendMicros || sync.ServerSendMicros < sync.ServerReceiveMicros {
		return ClockSync{}, ErrInvalidControlFrame
	}
	return sync, nil
}

// DeliveryReport reports cumulative parsing progress for one lane generation and direction.
type DeliveryReport struct {
	LaneID      LaneID
	Generation  uint64
	DataPackets uint64
	PingID      uint64
	// DelayMicros is the time from the newest reported parse progress to report construction by the carrier writer.
	DelayMicros uint64
}

// MarshalDeliveryReport returns a cumulative delivery report frame.
func MarshalDeliveryReport(report DeliveryReport) (Frame, error) {
	if report.LaneID.IsZero() || report.Generation == 0 {
		return Frame{}, ErrInvalidControlFrame
	}
	payload := integerPayload(0, uint64(report.LaneID), report.Generation, report.DataPackets, report.PingID, report.DelayMicros)
	return Frame{Type: FrameDeliveryReport, Payload: payload}, nil
}

// ParseDeliveryReport parses a cumulative delivery report frame.
func ParseDeliveryReport(frame Frame) (DeliveryReport, error) {
	if frame.Type != FrameDeliveryReport {
		return DeliveryReport{}, ErrInvalidControlFrame
	}
	var report DeliveryReport
	remaining, err := parseIntegers(frame.Payload, (*uint64)(&report.LaneID), &report.Generation, &report.DataPackets, &report.PingID, &report.DelayMicros)
	if err != nil || len(remaining) != 0 || report.LaneID.IsZero() || report.Generation == 0 {
		return DeliveryReport{}, ErrInvalidControlFrame
	}
	return report, nil
}

// SessionCreated carries session credentials and clock-bootstrap timestamps.
type SessionCreated struct {
	SessionID     SessionID
	SessionSecret SessionSecret
	PathGroupID   PathGroupID
	ReceiveMicros uint64
	SendMicros    uint64
}

// MarshalSessionCreated returns a session-created control frame.
func MarshalSessionCreated(created SessionCreated) (Frame, error) {
	if created.SessionID.IsZero() || created.SessionSecret == (SessionSecret{}) || created.PathGroupID.IsZero() ||
		created.ReceiveMicros > created.SendMicros {
		return Frame{}, ErrInvalidControlFrame
	}
	payload := integerPayload(48, uint64(created.PathGroupID), created.ReceiveMicros, created.SendMicros)
	copy(payload[:16], created.SessionID[:])
	copy(payload[16:48], created.SessionSecret[:])
	return Frame{Type: FrameSessionCreated, Payload: payload}, nil
}

// ParseSessionCreated parses a session-created control frame.
func ParseSessionCreated(frame Frame) (SessionCreated, error) {
	if frame.Type != FrameSessionCreated || len(frame.Payload) < 48 {
		return SessionCreated{}, ErrInvalidControlFrame
	}
	var created SessionCreated
	copy(created.SessionID[:], frame.Payload[:16])
	copy(created.SessionSecret[:], frame.Payload[16:48])
	remaining, err := parseIntegers(frame.Payload[48:], (*uint64)(&created.PathGroupID), &created.ReceiveMicros, &created.SendMicros)
	if err != nil || len(remaining) != 0 {
		return SessionCreated{}, ErrInvalidControlFrame
	}
	if created.SessionID.IsZero() || created.SessionSecret == (SessionSecret{}) || created.PathGroupID.IsZero() ||
		created.ReceiveMicros > created.SendMicros {
		return SessionCreated{}, ErrInvalidControlFrame
	}
	return created, nil
}

// LaneAccepted carries the effective path group and clock-bootstrap timestamps for a joined lane.
type LaneAccepted struct {
	SessionID     SessionID
	PathGroupID   PathGroupID
	ReceiveMicros uint64
	SendMicros    uint64
}

// MarshalLaneAccepted returns a lane-accepted control frame.
func MarshalLaneAccepted(accepted LaneAccepted) (Frame, error) {
	if accepted.SessionID.IsZero() || accepted.PathGroupID.IsZero() || accepted.ReceiveMicros > accepted.SendMicros {
		return Frame{}, ErrInvalidControlFrame
	}
	payload := integerPayload(16, uint64(accepted.PathGroupID), accepted.ReceiveMicros, accepted.SendMicros)
	copy(payload[:16], accepted.SessionID[:])
	return Frame{Type: FrameLaneAccepted, Payload: payload}, nil
}

// ParseLaneAccepted parses a lane-accepted control frame.
func ParseLaneAccepted(frame Frame) (LaneAccepted, error) {
	if frame.Type != FrameLaneAccepted || len(frame.Payload) < 16 {
		return LaneAccepted{}, ErrInvalidControlFrame
	}
	var accepted LaneAccepted
	copy(accepted.SessionID[:], frame.Payload[:16])
	remaining, err := parseIntegers(frame.Payload[16:], (*uint64)(&accepted.PathGroupID), &accepted.ReceiveMicros, &accepted.SendMicros)
	if err != nil || len(remaining) != 0 {
		return LaneAccepted{}, ErrInvalidControlFrame
	}
	if accepted.SessionID.IsZero() || accepted.PathGroupID.IsZero() || accepted.ReceiveMicros > accepted.SendMicros {
		return LaneAccepted{}, ErrInvalidControlFrame
	}
	return accepted, nil
}

// CloseReason identifies an intentional session-close cause.
type CloseReason uint8

const (
	// CloseClientShutdown identifies an intentional client process shutdown.
	CloseClientShutdown CloseReason = 1
)

// Valid reports whether the close reason is defined by this protocol version.
func (r CloseReason) Valid() bool {
	return r == CloseClientShutdown
}

// MarshalSessionClose returns an explicit session-close control frame.
func MarshalSessionClose(reason CloseReason) (Frame, error) {
	if !reason.Valid() {
		return Frame{}, ErrInvalidControlFrame
	}
	return Frame{Type: FrameSessionClose, Payload: []byte{byte(reason)}}, nil
}

// ParseSessionClose parses an explicit session-close control frame.
func ParseSessionClose(frame Frame) (CloseReason, error) {
	if frame.Type != FrameSessionClose || len(frame.Payload) != 1 {
		return 0, ErrInvalidControlFrame
	}
	reason := CloseReason(frame.Payload[0])
	if !reason.Valid() {
		return 0, ErrInvalidControlFrame
	}
	return reason, nil
}

// LaneGeneration identifies one stable lane and exact connection generation.
type LaneGeneration struct {
	LaneID     LaneID
	Generation uint64
}

// MarshalLaneAbandon returns a generation-specific lane-abandon frame.
func MarshalLaneAbandon(lane LaneGeneration) (Frame, error) {
	if lane.LaneID.IsZero() || lane.Generation == 0 {
		return Frame{}, ErrInvalidControlFrame
	}
	payload := integerPayload(0, uint64(lane.LaneID), lane.Generation)
	return Frame{Type: FrameLaneAbandon, Payload: payload}, nil
}

// ParseLaneAbandon parses a generation-specific lane-abandon frame.
func ParseLaneAbandon(frame Frame) (LaneGeneration, error) {
	if frame.Type != FrameLaneAbandon {
		return LaneGeneration{}, ErrInvalidControlFrame
	}
	var lane LaneGeneration
	remaining, err := parseIntegers(frame.Payload, (*uint64)(&lane.LaneID), &lane.Generation)
	if err != nil || len(remaining) != 0 {
		return LaneGeneration{}, ErrInvalidControlFrame
	}
	if lane.LaneID.IsZero() || lane.Generation == 0 {
		return LaneGeneration{}, ErrInvalidControlFrame
	}
	return lane, nil
}

// ErrorFrame carries a stable in-session error and bounded diagnostic.
type ErrorFrame struct {
	Code       ErrorCode
	Class      ErrorClass
	Scope      ErrorScope
	LaneID     LaneID
	Generation uint64
	Diagnostic string
}

// MarshalErrorFrame returns an in-session error frame.
func MarshalErrorFrame(value ErrorFrame) (Frame, error) {
	if !value.Code.Valid() || value.Code == ErrorClockSkew || !validErrorDisposition(value.Class, value.Scope) ||
		!validDiagnostic(value.Diagnostic) {
		return Frame{}, ErrInvalidControlFrame
	}
	if value.Scope == ErrorScopeLane && (value.LaneID.IsZero() || value.Generation == 0) {
		return Frame{}, ErrInvalidControlFrame
	}
	if value.Scope == ErrorScopeSession && (!value.LaneID.IsZero() || value.Generation != 0) {
		return Frame{}, ErrInvalidControlFrame
	}
	size := uvarintSize(uint64(value.Code)) + 2 + uvarintSize(uint64(value.LaneID)) + uvarintSize(value.Generation) + len(value.Diagnostic)
	payload := make([]byte, 0, size)
	payload = binary.AppendUvarint(payload, uint64(value.Code))
	payload = append(payload, byte(value.Class), byte(value.Scope))
	payload = binary.AppendUvarint(payload, uint64(value.LaneID))
	payload = binary.AppendUvarint(payload, value.Generation)
	payload = append(payload, value.Diagnostic...)
	return Frame{Type: FrameError, Payload: payload}, nil
}

// ParseErrorFrame parses an in-session error frame.
func ParseErrorFrame(frame Frame) (ErrorFrame, error) {
	if frame.Type != FrameError {
		return ErrorFrame{}, ErrInvalidControlFrame
	}
	code, width, err := parseUvarint(frame.Payload)
	if err != nil || code > uint64(ErrorClockSkew) || len(frame.Payload)-width < 2 {
		return ErrorFrame{}, ErrInvalidControlFrame
	}
	payload := frame.Payload[width:]
	value := ErrorFrame{Code: ErrorCode(code), Class: ErrorClass(payload[0]), Scope: ErrorScope(payload[1])}
	diagnostic, err := parseIntegers(payload[2:], (*uint64)(&value.LaneID), &value.Generation)
	if err != nil || len(diagnostic) > MaxDiagnosticSize {
		return ErrorFrame{}, ErrInvalidControlFrame
	}
	value.Diagnostic = string(diagnostic)
	if !value.Code.Valid() || value.Code == ErrorClockSkew || !validErrorDisposition(value.Class, value.Scope) ||
		!validDiagnostic(value.Diagnostic) {
		return ErrorFrame{}, ErrInvalidControlFrame
	}
	if value.Scope == ErrorScopeLane && (value.LaneID.IsZero() || value.Generation == 0) {
		return ErrorFrame{}, ErrInvalidControlFrame
	}
	if value.Scope == ErrorScopeSession && (!value.LaneID.IsZero() || value.Generation != 0) {
		return ErrorFrame{}, ErrInvalidControlFrame
	}
	return value, nil
}
