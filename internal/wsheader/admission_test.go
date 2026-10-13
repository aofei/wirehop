package wsheader

import (
	"encoding/hex"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
)

func admissionHello(mode protocol.HelloMode) protocol.ClientHello {
	hello := protocol.ClientHello{
		Mode: mode, LaneID: 1, Generation: 2, PathGroupID: 3,
		Nonce: protocol.Nonce{4}, UnixSeconds: 5, MonotonicMicros: 6,
	}
	if mode == protocol.HelloCreate {
		hello.Target = target.MustParse("wg.example.com:51820")
	} else {
		hello.SessionID = protocol.SessionID{7}
	}
	return hello
}

func TestHeaders(t *testing.T) {
	for _, tt := range []struct {
		name string
		mode protocol.HelloMode
	}{
		{name: "Create", mode: protocol.HelloCreate},
		{name: "Join", mode: protocol.HelloJoin},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hello := admissionHello(tt.mode)
			key := []byte("secret token\x00\xff")
			for _, path := range []string{"/", "/_wirehop", "/a%2Fb", "/" + strings.Repeat("a", MaxPathSize-1)} {
				headers, err := Headers(path, hello, key)
				if err != nil {
					t.Fatal(err)
				}
				parsedURL, err := url.Parse("https://relay.example" + path)
				if err != nil {
					t.Fatal(err)
				}
				request := &http.Request{Method: http.MethodGet, URL: parsedURL, Header: headers}
				parsed, err := Parse(request)
				if err != nil {
					t.Fatal(err)
				}
				if err := Verify(request, parsed, key); err != nil {
					t.Fatal(err)
				}
				parsed.AuthTag = protocol.AuthTag{}
				if parsed != hello {
					t.Fatalf("Parse() = %+v, want %+v", parsed, hello)
				}
			}
			if _, err := Headers("/", hello, nil); !errors.Is(err, protocol.ErrMissingAuthKey) {
				t.Fatalf("missing key error = %v", err)
			}
			for _, path := range []string{"", "/" + strings.Repeat("a", MaxPathSize)} {
				if _, err := Headers(path, hello, key); !errors.Is(err, ErrInvalid) {
					t.Fatalf("invalid path error = %v", err)
				}
			}
			for _, mutate := range []func(*protocol.ClientHello){
				func(h *protocol.ClientHello) { h.Mode = 0 },
				func(h *protocol.ClientHello) { h.LaneID = 0 },
				func(h *protocol.ClientHello) { h.Generation = 0 },
				func(h *protocol.ClientHello) { h.PathGroupID = 0 },
				func(h *protocol.ClientHello) { h.Nonce = protocol.Nonce{} },
				func(h *protocol.ClientHello) { h.UnixSeconds = 0 },
				func(h *protocol.ClientHello) {
					h.Target = target.Endpoint{}
					h.SessionID = protocol.SessionID{}
				},
				func(h *protocol.ClientHello) {
					h.Target = target.MustParse("wg.example.com:51820")
					h.SessionID = protocol.SessionID{1}
				},
			} {
				invalid := hello
				mutate(&invalid)
				if _, err := Headers("/", invalid, key); !errors.Is(err, ErrInvalid) {
					t.Fatalf("invalid hello %+v error = %v", invalid, err)
				}
			}
		})
	}
}

func TestVerify(t *testing.T) {
	for _, mode := range []protocol.HelloMode{protocol.HelloCreate, protocol.HelloJoin} {
		hello := admissionHello(mode)
		key := []byte("test-token")
		headers, err := Headers("/_wirehop", hello, key)
		if err != nil {
			t.Fatal(err)
		}
		request := &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers}
		parsed, err := Parse(request)
		if err != nil {
			t.Fatal(err)
		}
		for _, tt := range []struct {
			name   string
			mutate func(*protocol.ClientHello)
		}{
			{name: "LaneID", mutate: func(h *protocol.ClientHello) { h.LaneID++ }},
			{name: "Generation", mutate: func(h *protocol.ClientHello) { h.Generation++ }},
			{name: "PathGroupID", mutate: func(h *protocol.ClientHello) { h.PathGroupID++ }},
			{name: "Nonce", mutate: func(h *protocol.ClientHello) { h.Nonce[0]++ }},
			{name: "Timestamp", mutate: func(h *protocol.ClientHello) { h.UnixSeconds++ }},
			{name: "MonotonicSend", mutate: func(h *protocol.ClientHello) { h.MonotonicMicros++ }},
			{name: "AuthTag", mutate: func(h *protocol.ClientHello) { h.AuthTag[0]++ }},
			{name: "Destination", mutate: func(h *protocol.ClientHello) {
				if h.Mode == protocol.HelloCreate {
					h.Target = target.MustParse("other.example.com:51820")
				} else {
					h.SessionID[0]++
				}
			}},
			{name: "Operation", mutate: func(h *protocol.ClientHello) {
				if h.Mode == protocol.HelloCreate {
					h.Mode, h.Target, h.SessionID = protocol.HelloJoin, target.Endpoint{}, protocol.SessionID{7}
				} else {
					h.Mode, h.Target, h.SessionID = protocol.HelloCreate, target.MustParse("wg.example.com:51820"), protocol.SessionID{}
				}
			}},
		} {
			t.Run(tt.name+string(rune('0'+mode)), func(t *testing.T) {
				mutated := parsed
				tt.mutate(&mutated)
				if err := Verify(request, mutated, key); !errors.Is(err, protocol.ErrAuthenticationFailed) {
					t.Fatalf("Verify() error = %v", err)
				}
			})
		}
		if err := Verify(request, parsed, []byte("wrong-key")); !errors.Is(err, protocol.ErrAuthenticationFailed) {
			t.Fatalf("wrong key error = %v", err)
		}
		request.URL.Path = "/other"
		if err := Verify(request, parsed, key); !errors.Is(err, protocol.ErrAuthenticationFailed) {
			t.Fatalf("changed path error = %v", err)
		}
		request.URL.Path = "/_wirehop"
		if err := protocol.VerifyClientHello(parsed, key); !errors.Is(err, protocol.ErrAuthenticationFailed) {
			t.Fatalf("WebSocket tag accepted on raw stream: %v", err)
		}
		if err := protocol.SignClientHello(&parsed, key); err != nil {
			t.Fatal(err)
		}
		if err := Verify(request, parsed, key); !errors.Is(err, protocol.ErrAuthenticationFailed) {
			t.Fatalf("raw stream tag accepted on WebSocket: %v", err)
		}
	}
}

func TestParse(t *testing.T) {
	if _, err := Parse(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil request error = %v", err)
	}
	for _, mode := range []protocol.HelloMode{protocol.HelloCreate, protocol.HelloJoin} {
		headers, err := Headers("/_wirehop", admissionHello(mode), []byte("token"))
		if err != nil {
			t.Fatal(err)
		}
		for name := range headers {
			for _, duplicate := range []bool{false, true} {
				invalid := headers.Clone()
				if duplicate {
					invalid.Add(name, invalid.Get(name))
				} else {
					invalid.Del(name)
				}
				if _, err := Parse(&http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: invalid}); !errors.Is(err, ErrInvalid) {
					t.Fatalf("header %s duplicate %t error = %v", name, duplicate, err)
				}
			}
		}
		for _, mutate := range []func(*http.Request){
			func(r *http.Request) { r.URL = nil },
			func(r *http.Request) { r.Method = http.MethodPost },
			func(r *http.Request) { r.URL.Path = "" },
			func(r *http.Request) { r.URL.Path = "/" + strings.Repeat("a", MaxPathSize) },
			func(r *http.Request) { r.URL.ForceQuery = true },
			func(r *http.Request) { r.URL.RawQuery = "a=1" },
			func(r *http.Request) { r.Header.Set("Authorization", "Bearer token") },
			func(r *http.Request) { r.Header.Set("Authorization", "WireHop-HMAC "+strings.Repeat("0", 63)) },
			func(r *http.Request) { r.Header.Set("Authorization", "WireHop-HMAC "+strings.Repeat("g", 64)) },
			func(r *http.Request) { r.Header.Set(headerNonce, strings.Repeat("0", 24)) },
			func(r *http.Request) { r.Header.Set(headerNonce, "not-hex") },
			func(r *http.Request) { r.Header.Set(headerTarget, "WG.EXAMPLE.COM:51820") },
			func(r *http.Request) {
				r.Header.Set(headerSessionID, protocol.SessionID{7}.String())
				r.Header.Set(headerTarget, "wg.example.com:51820")
			},
		} {
			request := &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers.Clone()}
			mutate(request)
			if _, err := Parse(request); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid request error = %v", err)
			}
		}
		testCanonicalAdmissionNumbers(t, headers, func(headers http.Header) error {
			_, err := Parse(&http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers})
			return err
		})
		for _, name := range []string{"Authorization", headerNonce, headerSessionID} {
			if value := headers.Get(name); value != "" {
				if name == "Authorization" {
					headers.Set(name, "WireHop-HMAC "+strings.ToUpper(strings.TrimPrefix(value, "WireHop-HMAC ")))
				} else {
					headers.Set(name, strings.ToUpper(value))
				}
			}
		}
		request := &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers}
		parsed, err := Parse(request)
		if err != nil {
			t.Fatal(err)
		}
		if err := Verify(request, parsed, []byte("token")); err != nil {
			t.Fatalf("uppercase hexadecimal error = %v", err)
		}
	}
}

func TestAuthenticate(t *testing.T) {
	// This vector is calculated independently from the specified byte layout.
	hello := admissionHello(protocol.HelloCreate)
	tag, err := authenticate(http.MethodGet, "/_wirehop", hello, []byte("test-token"))
	if err != nil {
		t.Fatal(err)
	}
	const expected = "72ddecd98889dc9db9a0322e4dd6167377081cb30c97a06a4a4bdc642880ba27"
	if hex.EncodeToString(tag[:]) != expected {
		t.Fatalf("authentication tag = %x, want %s", tag, expected)
	}
}

func testCanonicalAdmissionNumbers(t *testing.T, headers http.Header, parse func(http.Header) error) {
	t.Helper()
	for _, field := range []struct {
		name    string
		header  string
		maximum string
		zero    bool
	}{
		{name: "LaneID", header: headerLaneID, maximum: "18446744073709551615"},
		{name: "Generation", header: headerLaneGeneration, maximum: "18446744073709551615"},
		{name: "PathGroupID", header: headerPathGroupID, maximum: "18446744073709551615"},
		{name: "Timestamp", header: headerTimestamp, maximum: "9223372036854775807"},
		{name: "MonotonicSend", header: headerMonotonicSend, maximum: "18446744073709551615", zero: true},
	} {
		t.Run(field.name, func(t *testing.T) {
			for _, tt := range []struct {
				name  string
				value string
				valid bool
			}{
				{name: "One", value: "1", valid: true},
				{name: "Maximum", value: field.maximum, valid: true},
				{name: "Zero", value: "0", valid: field.zero},
				{name: "LeadingZero", value: "01"},
				{name: "LeadingZeroes", value: "0001"},
				{name: "Zeroes", value: "00"},
				{name: "PositiveSign", value: "+1"},
				{name: "NegativeSign", value: "-1"},
				{name: "NegativeZero", value: "-0"},
				{name: "LeadingSpace", value: " 1"},
				{name: "TrailingSpace", value: "1 "},
				{name: "LineBreak", value: "1\r\n"},
				{name: "Overflow", value: "18446744073709551616"},
				{name: "Empty"},
			} {
				t.Run(tt.name, func(t *testing.T) {
					mutated := headers.Clone()
					mutated.Set(field.header, tt.value)
					err := parse(mutated)
					if tt.valid {
						if err != nil {
							t.Fatalf("canonical %s = %q: %v", field.header, tt.value, err)
						}
					} else if !errors.Is(err, ErrInvalid) {
						t.Fatalf("noncanonical %s = %q error = %v, want %v", field.header, tt.value, err, ErrInvalid)
					}
				})
			}
		})
	}
}

func FuzzParseAdmissionNumbers(f *testing.F) {
	creation, err := Headers("/_wirehop", protocol.ClientHello{
		Mode: protocol.HelloCreate, Target: target.MustParse("wg.example.com:51820"), LaneID: 1,
		Generation: 1, PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: 1, MonotonicMicros: 1,
	}, []byte("token"))
	if err != nil {
		f.Fatal(err)
	}
	join := protocol.ClientHello{
		Mode: protocol.HelloJoin, SessionID: protocol.SessionID{1}, LaneID: 1,
		Generation: 1, PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: 1, MonotonicMicros: 1,
	}
	joining, err := Headers("/_wirehop", join, []byte{1})
	if err != nil {
		f.Fatal(err)
	}
	for selector := range uint8(5) {
		for _, value := range []string{
			"", "0", "00", "1", "01", "+1", "-1", " 1", "1\r\n",
			"9223372036854775807", "9223372036854775808", "18446744073709551615", "18446744073709551616",
		} {
			f.Add(selector, value)
		}
	}
	f.Fuzz(func(t *testing.T, selector uint8, value string) {
		field := []string{headerLaneID, headerLaneGeneration, headerPathGroupID, headerTimestamp,
			headerMonotonicSend}[selector%5]
		maximum := "18446744073709551615"
		if field == headerTimestamp {
			maximum = "9223372036854775807"
		}
		// Check decimal grammar and magnitude independently of strconv parsing and formatting.
		valid := value != "" && len(value) <= len(maximum) && (len(value) == 1 || value[0] != '0') &&
			(len(value) < len(maximum) || value <= maximum) && (value != "0" || field == headerMonotonicSend)
		for index := range len(value) {
			if value[index] < '0' || value[index] > '9' {
				valid = false
				break
			}
		}
		request := &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: creation.Clone()}
		request.Header.Set(field, value)
		_, createErr := Parse(request)
		request.Header = joining.Clone()
		request.Header.Set(field, value)
		_, joinErr := Parse(request)
		if (createErr == nil) != valid || (joinErr == nil) != valid {
			t.Fatalf("%s = %q: create error %v, join error %v, want valid %t", field, value, createErr, joinErr, valid)
		}
		if !valid && (!errors.Is(createErr, ErrInvalid) || !errors.Is(joinErr, ErrInvalid)) {
			t.Fatalf("%s = %q: create error %v, join error %v, want %v", field, value, createErr, joinErr, ErrInvalid)
		}
	})
}
