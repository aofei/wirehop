package wsheader

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/target"
)

func TestCreateRoundTrip(t *testing.T) {
	request := Create{
		Token: "secret-token", Target: target.MustParse("wg.example.com:51820"), LaneID: protocol.LaneID(1),
		Generation: 2, PathGroupID: protocol.PathGroupID(3), Nonce: protocol.Nonce{4}, UnixSeconds: 5,
		MonotonicMicros: 6,
	}
	headers, err := Headers(request)
	if err != nil {
		t.Fatal(err)
	}
	httpRequest := &http.Request{Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers}
	parsed, err := ParseCreate(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	if parsed != request {
		t.Fatalf("ParseCreate() = %+v, want %+v", parsed, request)
	}
}

func TestCreateErrors(t *testing.T) {
	valid := Create{
		Token: "token", Target: target.MustParse("wg.example.com:51820"), LaneID: protocol.LaneID(1),
		Generation: 1, PathGroupID: protocol.PathGroupID(1), Nonce: protocol.Nonce{1}, UnixSeconds: 1,
	}
	for _, mutate := range []func(*Create){
		func(value *Create) { value.Token = "" },
		func(value *Create) { value.Token = "token\nvalue" },
		func(value *Create) { value.Token = "token value" },
		func(value *Create) { value.Token = "token=bad" },
		func(value *Create) { value.Target = target.Endpoint{} },
		func(value *Create) { value.LaneID = protocol.LaneID(0) },
		func(value *Create) { value.Generation = 0 },
		func(value *Create) { value.PathGroupID = protocol.PathGroupID(0) },
		func(value *Create) { value.Nonce = protocol.Nonce{} },
		func(value *Create) { value.UnixSeconds = 0 },
	} {
		request := valid
		mutate(&request)
		if _, err := Headers(request); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Headers(%+v) error = %v, want %v", request, err, ErrInvalid)
		}
	}
	headers, err := Headers(valid)
	if err != nil {
		t.Fatal(err)
	}
	headers.Add(headerTarget, valid.Target.String())
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() duplicate error = %v, want %v", err, ErrInvalid)
	}
	headers.Del(headerTarget)
	headers.Set(headerTarget, "WG.EXAMPLE.COM:51820")
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() noncanonical target error = %v, want %v", err, ErrInvalid)
	}
	headers.Set(headerTarget, valid.Target.String())
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop", ForceQuery: true}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() empty query error = %v, want %v", err, ErrInvalid)
	}
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodPost, URL: &url.URL{Path: "/_wirehop"}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() method error = %v, want %v", err, ErrInvalid)
	}
	if _, err := ParseCreate(&http.Request{
		Method: http.MethodGet, URL: &url.URL{Path: "/" + strings.Repeat("a", MaxPathSize)}, Header: headers,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ParseCreate() oversized path error = %v, want %v", err, ErrInvalid)
	}
}

func TestValidateBearerToken(t *testing.T) {
	for _, token := range []string{"a", "abc-._~+/", "YWJjZA=="} {
		if err := ValidateBearerToken(token); err != nil {
			t.Fatalf("ValidateBearerToken(%q) error = %v", token, err)
		}
	}
	for _, token := range []string{"", "=", "abc def", "abc=def", "abc,", strings.Repeat("a", 4097)} {
		if err := ValidateBearerToken(token); !errors.Is(err, ErrInvalid) {
			t.Fatalf("ValidateBearerToken(%q) error = %v, want %v", token, err, ErrInvalid)
		}
	}
}

func TestParseCreateCanonicalNumbers(t *testing.T) {
	headers, err := Headers(Create{
		Token: "token", Target: target.MustParse("wg.example.com:51820"), LaneID: 1,
		Generation: 1, PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: 1, MonotonicMicros: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	testCanonicalAdmissionNumbers(t, headers, func(headers http.Header) error {
		_, err := ParseCreate(&http.Request{
			Method: http.MethodGet, URL: &url.URL{Path: "/_wirehop"}, Header: headers,
		})
		return err
	})
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
	creation, err := Headers(Create{
		Token: "token", Target: target.MustParse("wg.example.com:51820"), LaneID: 1,
		Generation: 1, PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: 1, MonotonicMicros: 1,
	})
	if err != nil {
		f.Fatal(err)
	}
	join := Join{
		Method: http.MethodGet, Path: "/_wirehop", SessionID: protocol.SessionID{1}, LaneID: 1,
		Generation: 1, PathGroupID: 1, Nonce: protocol.Nonce{1}, UnixSeconds: 1, MonotonicMicros: 1,
	}
	if err := SignJoin(&join, protocol.SessionSecret{1}); err != nil {
		f.Fatal(err)
	}
	joining, err := JoinHeaders(join)
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
		_, createErr := ParseCreate(request)
		request.Header = joining.Clone()
		request.Header.Set(field, value)
		_, joinErr := ParseJoin(request)
		if (createErr == nil) != valid || (joinErr == nil) != valid {
			t.Fatalf("%s = %q: create error %v, join error %v, want valid %t", field, value, createErr, joinErr, valid)
		}
		if !valid && (!errors.Is(createErr, ErrInvalid) || !errors.Is(joinErr, ErrInvalid)) {
			t.Fatalf("%s = %q: create error %v, join error %v, want %v", field, value, createErr, joinErr, ErrInvalid)
		}
	})
}
