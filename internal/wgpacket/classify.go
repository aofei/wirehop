// Package wgpacket describes and classifies public WireGuard packet structures.
package wgpacket

import "encoding/binary"

// Kind describes the structural classification of a UDP datagram.
type Kind uint8

const (
	// NonWireGuard identifies a datagram without a recognized WireGuard message type.
	NonWireGuard Kind = iota
	// Malformed identifies a recognized WireGuard message type with an invalid length.
	Malformed
	// HandshakeInitiation identifies a structurally valid handshake initiation.
	HandshakeInitiation
	// HandshakeResponse identifies a structurally valid handshake response.
	HandshakeResponse
	// CookieReply identifies a structurally valid cookie reply.
	CookieReply
	// TransportData identifies a structurally valid transport data packet.
	TransportData
)

// Header contains the public routing fields of one structurally valid WireGuard packet.
type Header struct {
	Kind          Kind
	SenderIndex   uint32
	ReceiverIndex uint32
}

const (
	// handshakeInitiationLength is the exact WireGuard handshake initiation length.
	handshakeInitiationLength = 148
	// handshakeResponseLength is the exact WireGuard handshake response length.
	handshakeResponseLength = 92
	// cookieReplyLength is the exact WireGuard cookie reply length.
	cookieReplyLength = 64
	// transportDataMinimumLength is the shortest valid WireGuard transport packet.
	transportDataMinimumLength = 32
)

// Accepted reports whether the kind represents a structurally valid WireGuard packet.
func (k Kind) Accepted() bool {
	return k >= HandshakeInitiation && k <= TransportData
}

// Control reports whether the kind represents a WireGuard handshake or cookie packet.
func (k Kind) Control() bool {
	return k >= HandshakeInitiation && k <= CookieReply
}

// String returns the stable metric and diagnostic name for the kind.
func (k Kind) String() string {
	switch k {
	case NonWireGuard:
		return "non_wireguard"
	case Malformed:
		return "malformed"
	case HandshakeInitiation:
		return "handshake_initiation"
	case HandshakeResponse:
		return "handshake_response"
	case CookieReply:
		return "cookie_reply"
	case TransportData:
		return "transport_data"
	default:
		return "unknown"
	}
}

// Classify returns the public structural classification of packet.
func Classify(packet []byte) Kind {
	return Inspect(packet).Kind
}

// Inspect classifies packet and extracts its public sender and receiver indexes.
func Inspect(packet []byte) Header {
	if len(packet) < 4 {
		return Header{Kind: NonWireGuard}
	}

	// Byte offsets 1 through 3 are opaque reserved data and may be nonzero.
	switch packet[0] {
	case 1:
		if len(packet) == handshakeInitiationLength {
			return Header{Kind: HandshakeInitiation, SenderIndex: binary.LittleEndian.Uint32(packet[4:8])}
		}
	case 2:
		if len(packet) == handshakeResponseLength {
			return Header{
				Kind: HandshakeResponse, SenderIndex: binary.LittleEndian.Uint32(packet[4:8]),
				ReceiverIndex: binary.LittleEndian.Uint32(packet[8:12]),
			}
		}
	case 3:
		if len(packet) == cookieReplyLength {
			return Header{Kind: CookieReply, ReceiverIndex: binary.LittleEndian.Uint32(packet[4:8])}
		}
	case 4:
		if len(packet) >= transportDataMinimumLength {
			return Header{Kind: TransportData, ReceiverIndex: binary.LittleEndian.Uint32(packet[4:8])}
		}
	default:
		return Header{Kind: NonWireGuard}
	}

	return Header{Kind: Malformed}
}
