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

	laneID := LaneID(128)
	if laneID.IsZero() {
		t.Fatal("lane ID is zero")
	}
	parsedLaneID, err := ParseLaneID(laneID.String())
	if err != nil || parsedLaneID != laneID {
		t.Fatalf("ParseLaneID() = %v, %v", parsedLaneID, err)
	}

	pathGroupID := PathGroupID(16384)
	if pathGroupID.IsZero() {
		t.Fatal("path group ID is zero")
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

func TestCompactIdentifierCanonicalForm(t *testing.T) {
	for _, value := range []string{"0", "01", "+1", "-1", " 1", "1 ", "18446744073709551616"} {
		if _, err := ParseLaneID(value); err == nil {
			t.Fatalf("lane ID accepted %q", value)
		}
		if _, err := ParsePathGroupID(value); err == nil {
			t.Fatalf("path group ID accepted %q", value)
		}
	}
	const maximum = "18446744073709551615"
	lane, laneErr := ParseLaneID(maximum)
	group, groupErr := ParsePathGroupID(maximum)
	if laneErr != nil || groupErr != nil || lane.String() != maximum || group.String() != maximum {
		t.Fatalf("maximum selectors: lane %v, group %v, errors %v and %v", lane, group, laneErr, groupErr)
	}
}
