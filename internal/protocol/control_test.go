package protocol

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestControlFrameWireLayout(t *testing.T) {
	const (
		id1     = "01000000000000000000000000000000"
		id3     = "03000000000000000000000000000000"
		zeroID  = "00000000000000000000000000000000"
		secret2 = "0200000000000000000000000000000000000000000000000000000000000000"
	)
	for _, tt := range []struct {
		name    string
		value   any
		encoded string
	}{
		{name: "Ping", value: TimingPing{ID: 128, SendMicros: 16384}, encoded: "02058001808001"},
		{name: "Pong", value: TimingPong{ID: 1, PingSendMicros: 127, ReceiveMicros: 128, SendMicros: 16384},
			encoded: "0307017f8001808001"},
		{name: "ClockSync", value: ClockSync{
			ClientSendMicros: 127, ServerReceiveMicros: 128, ServerSendMicros: 16383, ClientReceiveMicros: 16384,
		}, encoded: "04087f8001ff7f808001"},
		{name: "Probe", value: Probe{Payload: []byte{1, 0x23, 0x45}}, encoded: "0503012345"},
		{name: "DeliveryReport", value: DeliveryReport{
			LaneID: testLaneID(1), Generation: 128, DataBytes: 16384, DataPackets: 127,
			ProbeBytes: 16383, ProbePackets: 1,
		}, encoded: "0619" + id1 + "80018080017fff7f01"},
		{name: "SessionCreated", value: SessionCreated{
			SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: testPathGroupID(3),
			ReceiveMicros: 128, SendMicros: 16384,
		}, encoded: "0745" + id1 + secret2 + id3 + "8001808001"},
		{name: "LaneAccepted", value: LaneAccepted{
			SessionID: testSessionID(1), PathGroupID: testPathGroupID(3), ReceiveMicros: 128, SendMicros: 16384,
		}, encoded: "0825" + id1 + id3 + "8001808001"},
		{name: "SessionClose", value: CloseClientShutdown, encoded: "090101"},
		{name: "LaneAbandon", value: LaneGeneration{LaneID: testLaneID(1), Generation: 128},
			encoded: "0a12" + id1 + "8001"},
		{name: "LaneError", value: ErrorFrame{
			Code: ErrorProtocolViolation, Class: ErrorLaneRejected, Scope: ErrorScopeLane,
			LaneID: testLaneID(1), Generation: 128, Diagnostic: "ok",
		}, encoded: "0b170a0201" + id1 + "80016f6b"},
		{name: "SessionError", value: ErrorFrame{
			Code: ErrorAuthentication, Class: ErrorSessionRejected, Scope: ErrorScopeSession, Diagnostic: "bad",
		}, encoded: "0b17030402" + zeroID + "00626164"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			want, err := hex.DecodeString(tt.encoded)
			if err != nil {
				t.Fatal(err)
			}
			frames, err := ParseFrames(want)
			if err != nil || len(frames) != 1 {
				t.Fatalf("ParseFrames() = %v, %v for literal wire layout", frames, err)
			}
			var encoded Frame
			var parsed any
			var parseErr error
			switch value := tt.value.(type) {
			case TimingPing:
				encoded, err = MarshalTimingPing(value)
				parsed, parseErr = ParseTimingPing(frames[0])
			case TimingPong:
				encoded, err = MarshalTimingPong(value)
				parsed, parseErr = ParseTimingPong(frames[0])
			case ClockSync:
				encoded, err = MarshalClockSync(value)
				parsed, parseErr = ParseClockSync(frames[0])
			case Probe:
				encoded, err = MarshalProbe(value)
				parsed, parseErr = ParseProbe(frames[0])
			case DeliveryReport:
				encoded, err = MarshalDeliveryReport(value)
				parsed, parseErr = ParseDeliveryReport(frames[0])
			case SessionCreated:
				encoded, err = MarshalSessionCreated(value)
				parsed, parseErr = ParseSessionCreated(frames[0])
			case LaneAccepted:
				encoded, err = MarshalLaneAccepted(value)
				parsed, parseErr = ParseLaneAccepted(frames[0])
			case CloseReason:
				encoded, err = MarshalSessionClose(value)
				parsed, parseErr = ParseSessionClose(frames[0])
			case LaneGeneration:
				encoded, err = MarshalLaneAbandon(value)
				parsed, parseErr = ParseLaneAbandon(frames[0])
			case ErrorFrame:
				encoded, err = MarshalErrorFrame(value)
				parsed, parseErr = ParseErrorFrame(frames[0])
			default:
				t.Fatalf("unsupported fixture type %T", value)
			}
			if err != nil {
				t.Fatal(err)
			}
			if parseErr != nil || !reflect.DeepEqual(parsed, tt.value) {
				t.Fatalf("parsed literal wire layout = %#v, %v, want %#v", parsed, parseErr, tt.value)
			}
			got, err := MarshalFrame(encoded)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("encoded wire layout = %x, %v, want %x", got, err, want)
			}
		})
	}
}

func TestTimingFrames(t *testing.T) {
	ping := TimingPing{ID: 1, SendMicros: 100}
	pingFrame, err := MarshalTimingPing(ping)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseTimingPing(pingFrame); err != nil || got != ping {
		t.Fatalf("ParseTimingPing() = %#v, %v", got, err)
	}

	pong := TimingPong{ID: 1, PingSendMicros: 100, ReceiveMicros: 150, SendMicros: 160}
	pongFrame, err := MarshalTimingPong(pong)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseTimingPong(pongFrame); err != nil || got != pong {
		t.Fatalf("ParseTimingPong() = %#v, %v", got, err)
	}

	sync := ClockSync{ClientSendMicros: 100, ServerReceiveMicros: 150, ServerSendMicros: 160, ClientReceiveMicros: 220}
	syncFrame, err := MarshalClockSync(sync)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseClockSync(syncFrame); err != nil || got != sync {
		t.Fatalf("ParseClockSync() = %#v, %v", got, err)
	}
}

func TestProbeAndDeliveryReport(t *testing.T) {
	probe := Probe{Payload: []byte{1, 2, 3}}
	probeFrame, err := MarshalProbe(probe)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseProbe(probeFrame); err != nil || !reflect.DeepEqual(got, probe) {
		t.Fatalf("ParseProbe() = %#v, %v", got, err)
	}
	encodedProbe, err := MarshalFrame(probeFrame)
	if err != nil {
		t.Fatal(err)
	}
	if len(encodedProbe) != FrameSize(len(probe.Payload)) {
		t.Fatalf("encoded probe length = %d, want %d", len(encodedProbe), FrameSize(len(probe.Payload)))
	}

	report := DeliveryReport{
		LaneID: testLaneID(1), Generation: 2, DataBytes: 4, DataPackets: 5, ProbeBytes: 6,
		ProbePackets: 7,
	}
	reportFrame, err := MarshalDeliveryReport(report)
	if err != nil {
		t.Fatal(err)
	}
	if len(reportFrame.Payload) != 21 {
		t.Fatalf("delivery report payload length = %d, want 21", len(reportFrame.Payload))
	}
	if got, err := ParseDeliveryReport(reportFrame); err != nil || got != report {
		t.Fatalf("ParseDeliveryReport() = %#v, %v", got, err)
	}
}

func TestSessionControlFrames(t *testing.T) {
	created := SessionCreated{
		SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: testPathGroupID(3),
		ReceiveMicros: 100, SendMicros: 110,
	}
	createdFrame, err := MarshalSessionCreated(created)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseSessionCreated(createdFrame); err != nil || got != created {
		t.Fatalf("ParseSessionCreated() = %#v, %v", got, err)
	}

	accepted := LaneAccepted{
		SessionID: testSessionID(1), PathGroupID: testPathGroupID(3), ReceiveMicros: 100, SendMicros: 110,
	}
	acceptedFrame, err := MarshalLaneAccepted(accepted)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseLaneAccepted(acceptedFrame); err != nil || got != accepted {
		t.Fatalf("ParseLaneAccepted() = %#v, %v", got, err)
	}

	closeFrame, err := MarshalSessionClose(CloseClientShutdown)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseSessionClose(closeFrame); err != nil || got != CloseClientShutdown {
		t.Fatalf("ParseSessionClose() = %d, %v", got, err)
	}

	lane := LaneGeneration{LaneID: testLaneID(1), Generation: 2}
	abandonFrame, err := MarshalLaneAbandon(lane)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ParseLaneAbandon(abandonFrame); err != nil || got != lane {
		t.Fatalf("ParseLaneAbandon() = %#v, %v", got, err)
	}
}

func TestErrorFrame(t *testing.T) {
	for _, want := range []ErrorFrame{
		{Code: ErrorStaleGeneration, Class: ErrorLaneRejected, Scope: ErrorScopeLane,
			LaneID: testLaneID(1), Generation: 2, Diagnostic: "stale generation"},
		{Code: ErrorAuthentication, Class: ErrorSessionRejected, Scope: ErrorScopeSession,
			Diagnostic: "authentication failed"},
	} {
		frame, err := MarshalErrorFrame(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ParseErrorFrame(frame)
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("ParseErrorFrame() = %#v, want %#v", got, want)
		}
	}
}

func TestControlFrameErrors(t *testing.T) {
	if _, err := MarshalTimingPing(TimingPing{}); err == nil {
		t.Fatal("MarshalTimingPing() succeeded with zero ID")
	}
	if _, err := MarshalTimingPong(TimingPong{ID: 1, ReceiveMicros: 2, SendMicros: 1}); err == nil {
		t.Fatal("MarshalTimingPong() succeeded with reversed timestamps")
	}
	if _, err := MarshalProbe(Probe{Payload: make([]byte, MaxProbePayloadSize+1)}); err == nil {
		t.Fatal("MarshalProbe() succeeded with oversized payload")
	}
	if _, err := MarshalDeliveryReport(DeliveryReport{}); err == nil {
		t.Fatal("MarshalDeliveryReport() succeeded without lane generation")
	}
	if _, err := MarshalSessionClose(0); err == nil {
		t.Fatal("MarshalSessionClose() succeeded with unknown reason")
	}
	if _, err := MarshalSessionClose(2); err == nil {
		t.Fatal("MarshalSessionClose() succeeded with an unsupported reason")
	}
	if _, err := ParseSessionClose(Frame{Type: FrameSessionClose, Payload: []byte{2}}); err == nil {
		t.Fatal("ParseSessionClose() succeeded with an unsupported reason")
	}
	if _, err := MarshalErrorFrame(ErrorFrame{
		Code: ErrorAuthentication, Class: ErrorSessionRejected, Scope: ErrorScopeSession, LaneID: testLaneID(1),
	}); err == nil {
		t.Fatal("MarshalErrorFrame() accepted lane identity in session scope")
	}
	if _, err := MarshalErrorFrame(ErrorFrame{
		Code: ErrorAuthentication, Class: ErrorSessionRejected, Scope: ErrorScopeLane,
		LaneID: testLaneID(1), Generation: 1,
	}); err == nil {
		t.Fatal("MarshalErrorFrame() accepted a session rejection with lane scope")
	}
	if _, err := MarshalErrorFrame(ErrorFrame{
		Code: ErrorAuthentication, Class: ErrorSessionRejected, Scope: ErrorScopeSession,
		Diagnostic: "line one\nline two",
	}); !errors.Is(err, ErrInvalidControlFrame) {
		t.Fatalf("MarshalErrorFrame() error = %v for control-byte diagnostic", err)
	}
	if _, err := MarshalErrorFrame(ErrorFrame{
		Code: ErrorClockSkew, Class: ErrorRetryable, Scope: ErrorScopeLane,
		LaneID: testLaneID(1), Generation: 1,
	}); !errors.Is(err, ErrInvalidControlFrame) {
		t.Fatalf("MarshalErrorFrame() error = %v for admission-only clock skew", err)
	}
	frame, err := MarshalErrorFrame(ErrorFrame{
		Code: ErrorAuthentication, Class: ErrorSessionRejected, Scope: ErrorScopeSession,
		Diagnostic: "authentication failed",
	})
	if err != nil {
		t.Fatal(err)
	}
	clockSkewFrame := frame
	clockSkewFrame.Payload = append([]byte(nil), frame.Payload...)
	clockSkewFrame.Payload[0] = byte(ErrorClockSkew)
	if _, err := ParseErrorFrame(clockSkewFrame); !errors.Is(err, ErrInvalidControlFrame) {
		t.Fatalf("ParseErrorFrame() error = %v for admission-only clock skew", err)
	}
	frame.Payload[20] = '\n'
	if _, err := ParseErrorFrame(frame); !errors.Is(err, ErrInvalidControlFrame) {
		t.Fatalf("ParseErrorFrame() error = %v for control-byte diagnostic", err)
	}
}

func FuzzParseControlFrame(f *testing.F) {
	add := func(frame Frame, err error) {
		if err != nil {
			f.Fatal(err)
		}
		f.Add(uint8(frame.Type), frame.Payload)
	}
	add(MarshalTimingPing(TimingPing{ID: 1, SendMicros: 2}))
	add(MarshalTimingPong(TimingPong{ID: 1, PingSendMicros: 2, ReceiveMicros: 3, SendMicros: 4}))
	add(MarshalClockSync(ClockSync{
		ClientSendMicros: 1, ServerReceiveMicros: 2, ServerSendMicros: 3, ClientReceiveMicros: 4,
	}))
	add(MarshalProbe(Probe{Payload: []byte{1, 2, 3}}))
	add(MarshalDeliveryReport(DeliveryReport{LaneID: testLaneID(1), Generation: 1}))
	add(MarshalSessionCreated(SessionCreated{
		SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: testPathGroupID(3),
		ReceiveMicros: 1, SendMicros: 2,
	}))
	add(MarshalLaneAccepted(LaneAccepted{
		SessionID: testSessionID(1), PathGroupID: testPathGroupID(3), ReceiveMicros: 1, SendMicros: 2,
	}))
	add(MarshalSessionClose(CloseClientShutdown))
	add(MarshalLaneAbandon(LaneGeneration{LaneID: testLaneID(1), Generation: 1}))
	add(MarshalErrorFrame(ErrorFrame{
		Code: ErrorAuthentication, Class: ErrorSessionRejected, Scope: ErrorScopeSession,
	}))
	f.Fuzz(func(t *testing.T, frameType uint8, payload []byte) {
		frame := Frame{Type: FrameType(frameType), Payload: payload}
		var err error
		valid := false
		var encoded Frame
		switch frame.Type {
		case FramePing:
			var value TimingPing
			if value, err = ParseTimingPing(frame); err == nil {
				valid = true
				encoded, err = MarshalTimingPing(value)
			}
		case FramePong:
			var value TimingPong
			if value, err = ParseTimingPong(frame); err == nil {
				valid = true
				encoded, err = MarshalTimingPong(value)
			}
		case FrameClockSync:
			var value ClockSync
			if value, err = ParseClockSync(frame); err == nil {
				valid = true
				encoded, err = MarshalClockSync(value)
			}
		case FrameProbe:
			var value Probe
			if value, err = ParseProbe(frame); err == nil {
				valid = true
				encoded, err = MarshalProbe(value)
			}
		case FrameDeliveryReport:
			var value DeliveryReport
			if value, err = ParseDeliveryReport(frame); err == nil {
				valid = true
				encoded, err = MarshalDeliveryReport(value)
			}
		case FrameSessionCreated:
			var value SessionCreated
			if value, err = ParseSessionCreated(frame); err == nil {
				valid = true
				encoded, err = MarshalSessionCreated(value)
			}
		case FrameLaneAccepted:
			var value LaneAccepted
			if value, err = ParseLaneAccepted(frame); err == nil {
				valid = true
				encoded, err = MarshalLaneAccepted(value)
			}
		case FrameSessionClose:
			var value CloseReason
			if value, err = ParseSessionClose(frame); err == nil {
				valid = true
				encoded, err = MarshalSessionClose(value)
			}
		case FrameLaneAbandon:
			var value LaneGeneration
			if value, err = ParseLaneAbandon(frame); err == nil {
				valid = true
				encoded, err = MarshalLaneAbandon(value)
			}
		case FrameError:
			var value ErrorFrame
			if value, err = ParseErrorFrame(frame); err == nil {
				valid = true
				encoded, err = MarshalErrorFrame(value)
			}
		}
		if valid && (err != nil || !bytes.Equal(encoded.Payload, payload)) {
			t.Fatalf("parsed control frame cannot be marshaled: %v", err)
		}
	})
}

func TestControlIntegerBoundaries(t *testing.T) {
	maximum := uint64(math.MaxUint64)
	for _, tt := range []struct {
		name    string
		marshal func() (Frame, error)
		parse   func(Frame) error
		size    int
		prefix  int
	}{
		{name: "Ping", marshal: func() (Frame, error) { return MarshalTimingPing(TimingPing{ID: maximum, SendMicros: maximum}) },
			parse: func(f Frame) error { _, err := ParseTimingPing(f); return err }, size: 20},
		{name: "Pong", marshal: func() (Frame, error) {
			return MarshalTimingPong(TimingPong{ID: maximum, PingSendMicros: maximum, ReceiveMicros: maximum, SendMicros: maximum})
		},
			parse: func(f Frame) error { _, err := ParseTimingPong(f); return err }, size: 40},
		{name: "ClockSync", marshal: func() (Frame, error) {
			return MarshalClockSync(ClockSync{ClientSendMicros: maximum, ServerReceiveMicros: maximum, ServerSendMicros: maximum, ClientReceiveMicros: maximum})
		},
			parse: func(f Frame) error { _, err := ParseClockSync(f); return err }, size: 40},
		{name: "DeliveryReport", marshal: func() (Frame, error) {
			return MarshalDeliveryReport(DeliveryReport{LaneID: testLaneID(1), Generation: maximum, DataBytes: maximum, DataPackets: maximum, ProbeBytes: maximum, ProbePackets: maximum})
		},
			parse: func(f Frame) error { _, err := ParseDeliveryReport(f); return err }, size: 66, prefix: 16},
		{name: "SessionCreated", marshal: func() (Frame, error) {
			return MarshalSessionCreated(SessionCreated{SessionID: testSessionID(1), SessionSecret: testSessionSecret(2), PathGroupID: testPathGroupID(3), ReceiveMicros: maximum, SendMicros: maximum})
		},
			parse: func(f Frame) error { _, err := ParseSessionCreated(f); return err }, size: 84, prefix: 64},
		{name: "LaneAccepted", marshal: func() (Frame, error) {
			return MarshalLaneAccepted(LaneAccepted{SessionID: testSessionID(1), PathGroupID: testPathGroupID(2), ReceiveMicros: maximum, SendMicros: maximum})
		},
			parse: func(f Frame) error { _, err := ParseLaneAccepted(f); return err }, size: 52, prefix: 32},
		{name: "LaneAbandon", marshal: func() (Frame, error) {
			return MarshalLaneAbandon(LaneGeneration{LaneID: testLaneID(1), Generation: maximum})
		},
			parse: func(f Frame) error { _, err := ParseLaneAbandon(f); return err }, size: 26, prefix: 16},
	} {
		t.Run(tt.name, func(t *testing.T) {
			frame, err := tt.marshal()
			if err != nil {
				t.Fatal(err)
			}
			before := bytes.Clone(frame.Payload)
			if len(frame.Payload) != tt.size || tt.parse(frame) != nil || !bytes.Equal(before, frame.Payload) {
				t.Fatal("full-width frame changed size, failed parsing, or mutated its input")
			}
			for cut := 0; cut < len(frame.Payload); cut++ {
				short := Frame{Type: frame.Type, Payload: frame.Payload[:cut]}
				if tt.parse(short) == nil {
					t.Fatalf("accepted truncated payload at %d", cut)
				}
			}
			trailing := Frame{Type: frame.Type, Payload: append(bytes.Clone(frame.Payload), 0)}
			if tt.parse(trailing) == nil {
				t.Fatal("accepted trailing content")
			}
			nonminimal := append(bytes.Clone(frame.Payload[:tt.prefix]), 0x81, 0)
			nonminimal = append(nonminimal, frame.Payload[tt.prefix+10:]...)
			if tt.parse(Frame{Type: frame.Type, Payload: nonminimal}) == nil {
				t.Fatal("accepted nonminimal integer")
			}
		})
	}
}

func TestProbePayloadBoundaries(t *testing.T) {
	for _, size := range []int{0, 127, 128, MaxProbePayloadSize} {
		source := bytes.Repeat([]byte{42}, size)
		frame, err := MarshalProbe(Probe{Payload: source})
		if err != nil {
			t.Fatal(err)
		}
		if size > 0 {
			source[0] = 9
		}
		parsed, err := ParseProbe(frame)
		if err != nil || len(parsed.Payload) != size || size > 0 && parsed.Payload[0] != 42 {
			t.Fatalf("probe ownership or size changed: %v", err)
		}
		if size > 0 && &parsed.Payload[0] != &frame.Payload[0] {
			t.Fatal("probe parser copied its borrowed payload")
		}
	}
	if _, err := ParseProbe(Frame{Type: FrameProbe, Payload: make([]byte, MaxProbePayloadSize+1)}); !errors.Is(err, ErrProbeTooLarge) {
		t.Fatalf("oversized probe error = %v", err)
	}
}

func TestErrorFrameCanonicalFields(t *testing.T) {
	frame, err := MarshalErrorFrame(ErrorFrame{Code: ErrorProtocolViolation, Class: ErrorLaneRejected, Scope: ErrorScopeLane,
		LaneID: testLaneID(1), Generation: math.MaxUint64, Diagnostic: string(bytes.Repeat([]byte{'a'}, MaxDiagnosticSize))})
	if err != nil {
		t.Fatal(err)
	}
	if len(frame.Payload) != 541 {
		t.Fatalf("maximum error size = %d", len(frame.Payload))
	}
	if _, err := ParseErrorFrame(frame); err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut < 29; cut++ {
		if _, err := ParseErrorFrame(Frame{Type: FrameError, Payload: frame.Payload[:cut]}); err == nil {
			t.Fatalf("accepted truncated error prefix at %d", cut)
		}
	}
	for _, payload := range [][]byte{
		append([]byte{0x8b, 0}, frame.Payload[1:]...),
		append([]byte{0x80, 0x80, 4}, frame.Payload[1:]...),
		append(bytes.Clone(frame.Payload[:19]), append([]byte{0x81, 0}, frame.Payload[29:]...)...),
		append(bytes.Clone(frame.Payload), 'a'),
	} {
		if _, err := ParseErrorFrame(Frame{Type: FrameError, Payload: payload}); err == nil {
			t.Fatal("accepted malformed error frame")
		}
	}
}
