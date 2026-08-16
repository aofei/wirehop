package protocol

import "testing"

func TestIdentifiers(t *testing.T) {
	sessionID := NewSessionID()
	if sessionID.IsZero() {
		t.Fatal("NewSessionID() returned zero")
	}
	parsedSessionID, err := ParseSessionID(sessionID.String())
	if err != nil || parsedSessionID != sessionID {
		t.Fatalf("ParseSessionID() = %v, %v", parsedSessionID, err)
	}

	laneID := NewLaneID()
	if laneID.IsZero() {
		t.Fatal("NewLaneID() returned zero")
	}
	parsedLaneID, err := ParseLaneID(laneID.String())
	if err != nil || parsedLaneID != laneID {
		t.Fatalf("ParseLaneID() = %v, %v", parsedLaneID, err)
	}

	pathGroupID := NewPathGroupID()
	if pathGroupID.IsZero() {
		t.Fatal("NewPathGroupID() returned zero")
	}
	parsedPathGroupID, err := ParsePathGroupID(pathGroupID.String())
	if err != nil || parsedPathGroupID != pathGroupID {
		t.Fatalf("ParsePathGroupID() = %v, %v", parsedPathGroupID, err)
	}

	secret := NewSessionSecret()
	if secret == (SessionSecret{}) {
		t.Fatal("NewSessionSecret() returned zero")
	}

	nonce := NewNonce()
	if nonce == (Nonce{}) {
		t.Fatal("NewNonce() returned zero")
	}
	parsedNonce, err := ParseNonce(nonce.String())
	if err != nil || parsedNonce != nonce {
		t.Fatalf("ParseNonce() = %v, %v", parsedNonce, err)
	}
}

func TestIdentifierParsingErrors(t *testing.T) {
	for _, value := range []string{"", "00", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"} {
		if _, err := ParseSessionID(value); err == nil {
			t.Fatalf("ParseSessionID(%q) succeeded", value)
		}
		if _, err := ParseLaneID(value); err == nil {
			t.Fatalf("ParseLaneID(%q) succeeded", value)
		}
		if _, err := ParsePathGroupID(value); err == nil {
			t.Fatalf("ParsePathGroupID(%q) succeeded", value)
		}
		if _, err := ParseNonce(value); err == nil {
			t.Fatalf("ParseNonce(%q) succeeded", value)
		}
	}
}
