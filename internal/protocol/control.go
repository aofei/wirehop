package protocol

import (
	"encoding/binary"
	"errors"
)

const (
	// timingPingSize is the exact encoded ping payload size.
	timingPingSize = 16
	// timingPongSize is the exact encoded pong payload size.
	timingPongSize = 32
	// clockSyncSize is the exact encoded clock synchronization payload size.
	clockSyncSize = 32
	// deliveryReportSize is the exact encoded delivery report payload size.
	deliveryReportSize = 56
	// sessionCreatedSize is the exact encoded session-created payload size.
	sessionCreatedSize = 80
	// laneAcceptedSize is the exact encoded lane-accepted payload size.
	laneAcceptedSize = 48
	// laneGenerationControlSize is the exact encoded abandon payload size.
	laneGenerationControlSize = LaneIDSize + 8
	// MaxProbePayloadSize bounds opaque probe traffic in one frame.
	MaxProbePayloadSize = 1200
	// ProbeFrameOverhead is the complete per-probe frame overhead excluding opaque padding.
	ProbeFrameOverhead = frameHeaderSize + 8
)

var (
	// ErrInvalidControlFrame indicates malformed control-frame fields or length.
	ErrInvalidControlFrame = errors.New("invalid control frame")
	// ErrProbeTooLarge indicates probe padding above its absolute protocol limit.
	ErrProbeTooLarge = errors.New("probe too large")
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
	payload := make([]byte, timingPingSize)
	binary.BigEndian.PutUint64(payload[0:8], ping.ID)
	binary.BigEndian.PutUint64(payload[8:16], ping.SendMicros)
	return Frame{Type: FramePing, Payload: payload}, nil
}

// ParseTimingPing parses a timing ping frame.
func ParseTimingPing(frame Frame) (TimingPing, error) {
	if frame.Type != FramePing || len(frame.Payload) != timingPingSize {
		return TimingPing{}, ErrInvalidControlFrame
	}
	ping := TimingPing{
		ID:         binary.BigEndian.Uint64(frame.Payload[0:8]),
		SendMicros: binary.BigEndian.Uint64(frame.Payload[8:16]),
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
	payload := make([]byte, timingPongSize)
	binary.BigEndian.PutUint64(payload[0:8], pong.ID)
	binary.BigEndian.PutUint64(payload[8:16], pong.PingSendMicros)
	binary.BigEndian.PutUint64(payload[16:24], pong.ReceiveMicros)
	binary.BigEndian.PutUint64(payload[24:32], pong.SendMicros)
	return Frame{Type: FramePong, Payload: payload}, nil
}

// ParseTimingPong parses a timing pong frame.
func ParseTimingPong(frame Frame) (TimingPong, error) {
	if frame.Type != FramePong || len(frame.Payload) != timingPongSize {
		return TimingPong{}, ErrInvalidControlFrame
	}
	pong := TimingPong{
		ID:             binary.BigEndian.Uint64(frame.Payload[0:8]),
		PingSendMicros: binary.BigEndian.Uint64(frame.Payload[8:16]),
		ReceiveMicros:  binary.BigEndian.Uint64(frame.Payload[16:24]),
		SendMicros:     binary.BigEndian.Uint64(frame.Payload[24:32]),
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
	payload := make([]byte, clockSyncSize)
	binary.BigEndian.PutUint64(payload[0:8], sync.ClientSendMicros)
	binary.BigEndian.PutUint64(payload[8:16], sync.ServerReceiveMicros)
	binary.BigEndian.PutUint64(payload[16:24], sync.ServerSendMicros)
	binary.BigEndian.PutUint64(payload[24:32], sync.ClientReceiveMicros)
	return Frame{Type: FrameClockSync, Payload: payload}, nil
}

// ParseClockSync parses a clock synchronization frame.
func ParseClockSync(frame Frame) (ClockSync, error) {
	if frame.Type != FrameClockSync || len(frame.Payload) != clockSyncSize {
		return ClockSync{}, ErrInvalidControlFrame
	}
	sync := ClockSync{
		ClientSendMicros:    binary.BigEndian.Uint64(frame.Payload[0:8]),
		ServerReceiveMicros: binary.BigEndian.Uint64(frame.Payload[8:16]),
		ServerSendMicros:    binary.BigEndian.Uint64(frame.Payload[16:24]),
		ClientReceiveMicros: binary.BigEndian.Uint64(frame.Payload[24:32]),
	}
	if sync.ClientReceiveMicros < sync.ClientSendMicros || sync.ServerSendMicros < sync.ServerReceiveMicros {
		return ClockSync{}, ErrInvalidControlFrame
	}
	return sync, nil
}

// Probe carries bounded opaque bytes for lane delivery measurement.
type Probe struct {
	ID      uint64
	Payload []byte
}

// MarshalProbe returns a bounded probe frame.
func MarshalProbe(probe Probe) (Frame, error) {
	if probe.ID == 0 {
		return Frame{}, ErrInvalidControlFrame
	}
	if len(probe.Payload) > MaxProbePayloadSize {
		return Frame{}, ErrProbeTooLarge
	}
	payload := make([]byte, 8+len(probe.Payload))
	binary.BigEndian.PutUint64(payload[0:8], probe.ID)
	copy(payload[8:], probe.Payload)
	return Frame{Type: FrameProbe, Payload: payload}, nil
}

// ParseProbe parses a bounded probe frame.
func ParseProbe(frame Frame) (Probe, error) {
	if frame.Type != FrameProbe || len(frame.Payload) < 8 {
		return Probe{}, ErrInvalidControlFrame
	}
	if len(frame.Payload)-8 > MaxProbePayloadSize {
		return Probe{}, ErrProbeTooLarge
	}
	probe := Probe{ID: binary.BigEndian.Uint64(frame.Payload[0:8]), Payload: frame.Payload[8:]}
	if probe.ID == 0 {
		return Probe{}, ErrInvalidControlFrame
	}
	return probe, nil
}

// DeliveryReport reports cumulative parsing progress for one lane generation and direction.
type DeliveryReport struct {
	LaneID       LaneID
	Generation   uint64
	DataBytes    uint64
	DataPackets  uint64
	ProbeBytes   uint64
	ProbePackets uint64
}

// MarshalDeliveryReport returns a cumulative delivery report frame.
func MarshalDeliveryReport(report DeliveryReport) (Frame, error) {
	if report.LaneID.IsZero() || report.Generation == 0 {
		return Frame{}, ErrInvalidControlFrame
	}
	payload := make([]byte, deliveryReportSize)
	copy(payload[0:16], report.LaneID[:])
	binary.BigEndian.PutUint64(payload[16:24], report.Generation)
	binary.BigEndian.PutUint64(payload[24:32], report.DataBytes)
	binary.BigEndian.PutUint64(payload[32:40], report.DataPackets)
	binary.BigEndian.PutUint64(payload[40:48], report.ProbeBytes)
	binary.BigEndian.PutUint64(payload[48:56], report.ProbePackets)
	return Frame{Type: FrameDeliveryReport, Payload: payload}, nil
}

// ParseDeliveryReport parses a cumulative delivery report frame.
func ParseDeliveryReport(frame Frame) (DeliveryReport, error) {
	if frame.Type != FrameDeliveryReport || len(frame.Payload) != deliveryReportSize {
		return DeliveryReport{}, ErrInvalidControlFrame
	}
	var report DeliveryReport
	copy(report.LaneID[:], frame.Payload[0:16])
	report.Generation = binary.BigEndian.Uint64(frame.Payload[16:24])
	report.DataBytes = binary.BigEndian.Uint64(frame.Payload[24:32])
	report.DataPackets = binary.BigEndian.Uint64(frame.Payload[32:40])
	report.ProbeBytes = binary.BigEndian.Uint64(frame.Payload[40:48])
	report.ProbePackets = binary.BigEndian.Uint64(frame.Payload[48:56])
	if report.LaneID.IsZero() || report.Generation == 0 {
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
	payload := make([]byte, sessionCreatedSize)
	copy(payload[0:16], created.SessionID[:])
	copy(payload[16:48], created.SessionSecret[:])
	copy(payload[48:64], created.PathGroupID[:])
	binary.BigEndian.PutUint64(payload[64:72], created.ReceiveMicros)
	binary.BigEndian.PutUint64(payload[72:80], created.SendMicros)
	return Frame{Type: FrameSessionCreated, Payload: payload}, nil
}

// ParseSessionCreated parses a session-created control frame.
func ParseSessionCreated(frame Frame) (SessionCreated, error) {
	if frame.Type != FrameSessionCreated || len(frame.Payload) != sessionCreatedSize {
		return SessionCreated{}, ErrInvalidControlFrame
	}
	var created SessionCreated
	copy(created.SessionID[:], frame.Payload[0:16])
	copy(created.SessionSecret[:], frame.Payload[16:48])
	copy(created.PathGroupID[:], frame.Payload[48:64])
	created.ReceiveMicros = binary.BigEndian.Uint64(frame.Payload[64:72])
	created.SendMicros = binary.BigEndian.Uint64(frame.Payload[72:80])
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
	payload := make([]byte, laneAcceptedSize)
	copy(payload[0:16], accepted.SessionID[:])
	copy(payload[16:32], accepted.PathGroupID[:])
	binary.BigEndian.PutUint64(payload[32:40], accepted.ReceiveMicros)
	binary.BigEndian.PutUint64(payload[40:48], accepted.SendMicros)
	return Frame{Type: FrameLaneAccepted, Payload: payload}, nil
}

// ParseLaneAccepted parses a lane-accepted control frame.
func ParseLaneAccepted(frame Frame) (LaneAccepted, error) {
	if frame.Type != FrameLaneAccepted || len(frame.Payload) != laneAcceptedSize {
		return LaneAccepted{}, ErrInvalidControlFrame
	}
	var accepted LaneAccepted
	copy(accepted.SessionID[:], frame.Payload[0:16])
	copy(accepted.PathGroupID[:], frame.Payload[16:32])
	accepted.ReceiveMicros = binary.BigEndian.Uint64(frame.Payload[32:40])
	accepted.SendMicros = binary.BigEndian.Uint64(frame.Payload[40:48])
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
	payload := make([]byte, laneGenerationControlSize)
	copy(payload[0:16], lane.LaneID[:])
	binary.BigEndian.PutUint64(payload[16:24], lane.Generation)
	return Frame{Type: FrameLaneAbandon, Payload: payload}, nil
}

// ParseLaneAbandon parses a generation-specific lane-abandon frame.
func ParseLaneAbandon(frame Frame) (LaneGeneration, error) {
	if frame.Type != FrameLaneAbandon || len(frame.Payload) != laneGenerationControlSize {
		return LaneGeneration{}, ErrInvalidControlFrame
	}
	var lane LaneGeneration
	copy(lane.LaneID[:], frame.Payload[0:16])
	lane.Generation = binary.BigEndian.Uint64(frame.Payload[16:24])
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
	diagnostic := []byte(value.Diagnostic)
	payload := make([]byte, 30+len(diagnostic))
	binary.BigEndian.PutUint16(payload[0:2], uint16(value.Code))
	payload[2] = byte(value.Class)
	payload[3] = byte(value.Scope)
	copy(payload[4:20], value.LaneID[:])
	binary.BigEndian.PutUint64(payload[20:28], value.Generation)
	binary.BigEndian.PutUint16(payload[28:30], uint16(len(diagnostic)))
	copy(payload[30:], diagnostic)
	return Frame{Type: FrameError, Payload: payload}, nil
}

// ParseErrorFrame parses an in-session error frame.
func ParseErrorFrame(frame Frame) (ErrorFrame, error) {
	if frame.Type != FrameError || len(frame.Payload) < 30 {
		return ErrorFrame{}, ErrInvalidControlFrame
	}
	diagnosticLength := int(binary.BigEndian.Uint16(frame.Payload[28:30]))
	if diagnosticLength > MaxDiagnosticSize || len(frame.Payload) != 30+diagnosticLength {
		return ErrorFrame{}, ErrInvalidControlFrame
	}
	value := ErrorFrame{
		Code:       ErrorCode(binary.BigEndian.Uint16(frame.Payload[0:2])),
		Class:      ErrorClass(frame.Payload[2]),
		Scope:      ErrorScope(frame.Payload[3]),
		Generation: binary.BigEndian.Uint64(frame.Payload[20:28]),
		Diagnostic: string(frame.Payload[30:]),
	}
	copy(value.LaneID[:], frame.Payload[4:20])
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
