package protocol

import (
	"encoding/binary"
	"errors"
	"reflect"
	"testing"
)

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
	if ProbeFrameOverhead != 13 {
		t.Fatalf("probe frame overhead = %d, want 13", ProbeFrameOverhead)
	}
	probe := Probe{ID: 7, Payload: []byte{1, 2, 3}}
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
	if len(encodedProbe) != ProbeFrameOverhead+len(probe.Payload) {
		t.Fatalf("encoded probe length = %d, want %d", len(encodedProbe), ProbeFrameOverhead+len(probe.Payload))
	}

	report := DeliveryReport{
		LaneID: testLaneID(1), Generation: 2, DataBytes: 4, DataPackets: 5, ProbeBytes: 6,
		ProbePackets: 7,
	}
	reportFrame, err := MarshalDeliveryReport(report)
	if err != nil {
		t.Fatal(err)
	}
	if len(reportFrame.Payload) != 56 {
		t.Fatalf("delivery report payload length = %d, want 56", len(reportFrame.Payload))
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
	if _, err := MarshalProbe(Probe{ID: 1, Payload: make([]byte, MaxProbePayloadSize+1)}); err == nil {
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
	binary.BigEndian.PutUint16(clockSkewFrame.Payload[:2], uint16(ErrorClockSkew))
	if _, err := ParseErrorFrame(clockSkewFrame); !errors.Is(err, ErrInvalidControlFrame) {
		t.Fatalf("ParseErrorFrame() error = %v for admission-only clock skew", err)
	}
	frame.Payload[30] = '\n'
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
	add(MarshalProbe(Probe{ID: 1, Payload: []byte{1, 2, 3}}))
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
		switch frame.Type {
		case FramePing:
			var value TimingPing
			if value, err = ParseTimingPing(frame); err == nil {
				valid = true
				_, err = MarshalTimingPing(value)
			}
		case FramePong:
			var value TimingPong
			if value, err = ParseTimingPong(frame); err == nil {
				valid = true
				_, err = MarshalTimingPong(value)
			}
		case FrameClockSync:
			var value ClockSync
			if value, err = ParseClockSync(frame); err == nil {
				valid = true
				_, err = MarshalClockSync(value)
			}
		case FrameProbe:
			var value Probe
			if value, err = ParseProbe(frame); err == nil {
				valid = true
				_, err = MarshalProbe(value)
			}
		case FrameDeliveryReport:
			var value DeliveryReport
			if value, err = ParseDeliveryReport(frame); err == nil {
				valid = true
				_, err = MarshalDeliveryReport(value)
			}
		case FrameSessionCreated:
			var value SessionCreated
			if value, err = ParseSessionCreated(frame); err == nil {
				valid = true
				_, err = MarshalSessionCreated(value)
			}
		case FrameLaneAccepted:
			var value LaneAccepted
			if value, err = ParseLaneAccepted(frame); err == nil {
				valid = true
				_, err = MarshalLaneAccepted(value)
			}
		case FrameSessionClose:
			var value CloseReason
			if value, err = ParseSessionClose(frame); err == nil {
				valid = true
				_, err = MarshalSessionClose(value)
			}
		case FrameLaneAbandon:
			var value LaneGeneration
			if value, err = ParseLaneAbandon(frame); err == nil {
				valid = true
				_, err = MarshalLaneAbandon(value)
			}
		case FrameError:
			var value ErrorFrame
			if value, err = ParseErrorFrame(frame); err == nil {
				valid = true
				_, err = MarshalErrorFrame(value)
			}
		}
		if valid && err != nil {
			t.Fatalf("parsed control frame cannot be marshaled: %v", err)
		}
	})
}
