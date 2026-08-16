package wgpacket

import (
	"encoding/binary"
	"testing"
)

func TestClassify(t *testing.T) {
	for _, tt := range []struct {
		name   string
		typeID byte
		length int
		want   Kind
	}{
		{name: "HandshakeInitiation", typeID: 1, length: 148, want: HandshakeInitiation},
		{name: "HandshakeResponse", typeID: 2, length: 92, want: HandshakeResponse},
		{name: "CookieReply", typeID: 3, length: 64, want: CookieReply},
		{name: "EmptyTransportData", typeID: 4, length: 32, want: TransportData},
		{name: "TransportData", typeID: 4, length: 1440, want: TransportData},
		{name: "MTUCappedTransportData", typeID: 4, length: 1452, want: TransportData},
		{name: "UnalignedTransportData", typeID: 4, length: 33, want: TransportData},
	} {
		t.Run(tt.name, func(t *testing.T) {
			packet := make([]byte, tt.length)
			packet[0] = tt.typeID
			if got := Classify(packet); got != tt.want {
				t.Fatalf("Classify() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClassifyRejects(t *testing.T) {
	for _, tt := range []struct {
		name   string
		typeID byte
		length int
		want   Kind
	}{
		{name: "Empty", length: 0, want: NonWireGuard},
		{name: "ShortType", length: 3, want: NonWireGuard},
		{name: "UnknownType", typeID: 5, length: 32, want: NonWireGuard},
		{name: "InitiationTooShort", typeID: 1, length: 147, want: Malformed},
		{name: "InitiationTooLong", typeID: 1, length: 149, want: Malformed},
		{name: "ResponseWrongLength", typeID: 2, length: 148, want: Malformed},
		{name: "CookieWrongLength", typeID: 3, length: 63, want: Malformed},
		{name: "TransportTooShort", typeID: 4, length: 16, want: Malformed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			packet := make([]byte, tt.length)
			if len(packet) >= 4 {
				packet[0] = tt.typeID
			}
			if got := Classify(packet); got != tt.want {
				t.Fatalf("Classify() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInspect(t *testing.T) {
	for _, tt := range []struct {
		name     string
		typeID   byte
		length   int
		sender   uint32
		receiver uint32
	}{
		{name: "Initiation", typeID: 1, length: handshakeInitiationLength, sender: 11},
		{name: "Response", typeID: 2, length: handshakeResponseLength, sender: 12, receiver: 11},
		{name: "Cookie", typeID: 3, length: cookieReplyLength, receiver: 11},
		{name: "Transport", typeID: 4, length: transportDataMinimumLength, receiver: 12},
	} {
		t.Run(tt.name, func(t *testing.T) {
			packet := make([]byte, tt.length)
			packet[0] = tt.typeID
			copy(packet[1:4], []byte{1, 2, 3})
			binary.LittleEndian.PutUint32(packet[4:8], max(tt.sender, tt.receiver))
			if tt.typeID == 2 {
				binary.LittleEndian.PutUint32(packet[8:12], tt.receiver)
			}
			got := Inspect(packet)
			if got.SenderIndex != tt.sender || got.ReceiverIndex != tt.receiver {
				t.Fatalf("Inspect() = %+v", got)
			}
		})
	}
}

func TestKind(t *testing.T) {
	for _, tt := range []struct {
		kind     Kind
		name     string
		accepted bool
		control  bool
	}{
		{kind: NonWireGuard, name: "non_wireguard"},
		{kind: Malformed, name: "malformed"},
		{kind: HandshakeInitiation, name: "handshake_initiation", accepted: true, control: true},
		{kind: HandshakeResponse, name: "handshake_response", accepted: true, control: true},
		{kind: CookieReply, name: "cookie_reply", accepted: true, control: true},
		{kind: TransportData, name: "transport_data", accepted: true},
		{kind: Kind(255), name: "unknown"},
	} {
		if got := tt.kind.String(); got != tt.name {
			t.Errorf("Kind(%d).String() = %q, want %q", tt.kind, got, tt.name)
		}
		if got := tt.kind.Accepted(); got != tt.accepted {
			t.Errorf("Kind(%d).Accepted() = %t, want %t", tt.kind, got, tt.accepted)
		}
		if got := tt.kind.Control(); got != tt.control {
			t.Errorf("Kind(%d).Control() = %t, want %t", tt.kind, got, tt.control)
		}
	}
}

func FuzzClassify(f *testing.F) {
	for _, packet := range [][]byte{
		nil,
		{1, 0, 0, 0},
		append([]byte{1, 1, 2, 3}, make([]byte, handshakeInitiationLength-4)...),
		make([]byte, handshakeInitiationLength),
		make([]byte, transportDataMinimumLength),
	} {
		f.Add(packet)
	}

	f.Fuzz(func(t *testing.T, packet []byte) {
		kind := Classify(packet)
		if kind > TransportData {
			t.Fatalf("Classify() returned invalid kind %d", kind)
		}
		if kind.Control() && !kind.Accepted() {
			t.Fatalf("control kind %d is not accepted", kind)
		}
	})
}
