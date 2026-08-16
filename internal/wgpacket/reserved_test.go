package wgpacket

import "testing"

func TestReservedText(t *testing.T) {
	const value = "AQID"
	want := Reserved{1, 2, 3}
	var reserved Reserved
	if err := reserved.UnmarshalText([]byte(value)); err != nil {
		t.Fatal(err)
	}
	if reserved != want {
		t.Fatalf("UnmarshalText() value = %v, want %v", reserved, want)
	}
	if !reserved.Enabled() {
		t.Fatal("Enabled() = false, want true")
	}
	encoded, err := reserved.MarshalText()
	if err != nil || string(encoded) != value {
		t.Fatalf("MarshalText() = %q, %v, want %q", encoded, err, value)
	}
}

func TestReservedUnmarshalTextRejects(t *testing.T) {
	for _, value := range []string{"", "AQI", "AQI=", "AQID=", "AQI!", "AQID\n", "AAAA"} {
		var reserved Reserved
		if err := reserved.UnmarshalText([]byte(value)); err == nil {
			t.Fatalf("UnmarshalText(%q) succeeded", value)
		}
	}
}

func TestReservedZero(t *testing.T) {
	if (Reserved{}).Enabled() {
		t.Fatal("zero Reserved.Enabled() = true, want false")
	}
	encoded, err := (Reserved{}).MarshalText()
	if err != nil || len(encoded) != 0 {
		t.Fatalf("zero Reserved.MarshalText() = %q, %v", encoded, err)
	}
}

func FuzzReservedText(f *testing.F) {
	for _, value := range []string{"AQID", "AAAA", "", "AQI=", "AQID=", "AQID\n"} {
		f.Add(value)
	}

	f.Fuzz(func(t *testing.T, value string) {
		var reserved Reserved
		if err := reserved.UnmarshalText([]byte(value)); err != nil {
			return
		}
		if !reserved.Enabled() {
			t.Fatalf("UnmarshalText(%q) returned a disabled value", value)
		}
		encoded, err := reserved.MarshalText()
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != value {
			t.Fatalf("MarshalText() = %q, want %q", encoded, value)
		}
	})
}
