package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

const (
	// SessionIDSize is the encoded size of a session identifier.
	SessionIDSize = 16
	// SessionSecretSize is the encoded size of an ephemeral session secret.
	SessionSecretSize = 32
	// LaneIDSize is the encoded size of a stable lane identifier.
	LaneIDSize = 16
	// PathGroupIDSize is the encoded size of a session-scoped path group identifier.
	PathGroupIDSize = 16
	// NonceSize is the encoded size of a handshake nonce.
	NonceSize = 12
)

// SessionID identifies one authenticated WireHop session.
type SessionID [SessionIDSize]byte

// SessionSecret authorizes lane joins to an established session.
type SessionSecret [SessionSecretSize]byte

// LaneID identifies one stable lane across connection generations.
type LaneID [LaneIDSize]byte

// PathGroupID identifies one session-scoped scheduling path group.
type PathGroupID [PathGroupIDSize]byte

// Nonce prevents replay of one authenticated handshake.
type Nonce [NonceSize]byte

// NewSessionID returns a cryptographically random session identifier.
func NewSessionID() SessionID {
	var id SessionID
	fillRandom(id[:])
	return id
}

// NewSessionSecret returns a cryptographically random ephemeral session secret.
func NewSessionSecret() SessionSecret {
	var secret SessionSecret
	fillRandom(secret[:])
	return secret
}

// NewLaneID returns a cryptographically random stable lane identifier.
func NewLaneID() LaneID {
	var id LaneID
	fillRandom(id[:])
	return id
}

// NewPathGroupID returns a cryptographically random path group identifier.
func NewPathGroupID() PathGroupID {
	var id PathGroupID
	fillRandom(id[:])
	return id
}

// NewNonce returns a cryptographically random handshake nonce.
func NewNonce() Nonce {
	var nonce Nonce
	fillRandom(nonce[:])
	return nonce
}

// String returns the lowercase hexadecimal session identifier.
func (id SessionID) String() string {
	return hex.EncodeToString(id[:])
}

// String returns the lowercase hexadecimal lane identifier.
func (id LaneID) String() string {
	return hex.EncodeToString(id[:])
}

// String returns the lowercase hexadecimal path group identifier.
func (id PathGroupID) String() string {
	return hex.EncodeToString(id[:])
}

// String returns the lowercase hexadecimal handshake nonce.
func (nonce Nonce) String() string {
	return hex.EncodeToString(nonce[:])
}

// IsZero reports whether the session identifier is unset.
func (id SessionID) IsZero() bool {
	return id == SessionID{}
}

// IsZero reports whether the lane identifier is unset.
func (id LaneID) IsZero() bool {
	return id == LaneID{}
}

// IsZero reports whether the path group identifier is unset.
func (id PathGroupID) IsZero() bool {
	return id == PathGroupID{}
}

// ParseSessionID parses a lowercase or uppercase hexadecimal session identifier.
func ParseSessionID(value string) (SessionID, error) {
	var id SessionID
	if err := decodeHex(value, id[:]); err != nil {
		return SessionID{}, fmt.Errorf("parse session ID: %w", err)
	}
	return id, nil
}

// ParseLaneID parses a lowercase or uppercase hexadecimal lane identifier.
func ParseLaneID(value string) (LaneID, error) {
	var id LaneID
	if err := decodeHex(value, id[:]); err != nil {
		return LaneID{}, fmt.Errorf("parse lane ID: %w", err)
	}
	return id, nil
}

// ParsePathGroupID parses a lowercase or uppercase hexadecimal path group identifier.
func ParsePathGroupID(value string) (PathGroupID, error) {
	var id PathGroupID
	if err := decodeHex(value, id[:]); err != nil {
		return PathGroupID{}, fmt.Errorf("parse path group ID: %w", err)
	}
	return id, nil
}

// ParseNonce parses a lowercase or uppercase hexadecimal handshake nonce.
func ParseNonce(value string) (Nonce, error) {
	var nonce Nonce
	if err := decodeHex(value, nonce[:]); err != nil {
		return Nonce{}, fmt.Errorf("parse nonce: %w", err)
	}
	return nonce, nil
}

// fillRandom fills value with a cryptographically secure, nonzero random value.
func fillRandom(value []byte) {
	for {
		rand.Read(value)
		for _, current := range value {
			if current != 0 {
				return
			}
		}
	}
}

// decodeHex decodes an exact-length hexadecimal value into dst.
func decodeHex(value string, dst []byte) error {
	if len(value) != hex.EncodedLen(len(dst)) {
		return fmt.Errorf("invalid encoded length %d", len(value))
	}
	if _, err := hex.Decode(dst, []byte(value)); err != nil {
		return fmt.Errorf("decode hexadecimal value: %w", err)
	}
	return nil
}
