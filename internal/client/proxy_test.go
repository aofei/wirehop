package client

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	neturl "net/url"
	"strings"
	"testing"
	"time"
)

func TestDialWebSocketProxyRejection(t *testing.T) {
	for _, secure := range []bool{false, true} {
		name := "HTTP"
		if secure {
			name = "HTTPS"
		}
		t.Run(name, func(t *testing.T) {
			for _, tt := range []struct {
				name   string
				status int
				want   failureDisposition
			}{
				{name: "AuthenticationRequired", status: http.StatusProxyAuthRequired, want: failureCloseLane},
				{name: "Forbidden", status: http.StatusForbidden, want: failureCloseLane},
				{name: "Unavailable", status: http.StatusServiceUnavailable, want: failureRetry},
				{name: "RateLimited", status: http.StatusTooManyRequests, want: failureRetry},
				{name: "RequestTimeout", status: http.StatusRequestTimeout, want: failureRetry},
			} {
				t.Run(tt.name, func(t *testing.T) {
					proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
						if request.Method != http.MethodConnect {
							t.Errorf("proxy request method = %s, want CONNECT", request.Method)
						}
						writer.Header().Set("X-Proxy-Diagnostic", "private diagnostic")
						writer.WriteHeader(tt.status)
					}))
					if secure {
						proxy.StartTLS()
					} else {
						proxy.Start()
					}
					t.Cleanup(proxy.Close)
					proxyURL, err := neturl.Parse(proxy.URL)
					if err != nil {
						t.Fatal(err)
					}
					roots := x509.NewCertPool()
					if secure {
						roots.AddCert(proxy.Certificate())
					}
					instance := &Client{config: Config{
						Dialer: &net.Dialer{}, Proxy: http.ProxyURL(proxyURL), HandshakeTimeout: time.Second,
						TLSConfig: &tls.Config{RootCAs: roots, ServerName: "relay.example"},
					}}
					spec := testLaneSpec(t, "wss://relay.example/_wirehop")
					prepared, err := instance.prepareLane(t.Context(), spec)
					if err != nil {
						t.Fatal(err)
					}
					defer prepared.Close()
					connection, _, _, cancel, err := instance.dialWebSocket(
						context.Background(), spec.URL(), make(http.Header), prepared, time.Second,
					)
					cancel()
					if connection != nil {
						connection.Close()
						t.Fatal("rejected proxy CONNECT returned a carrier")
					}
					if err == nil {
						t.Fatal("rejected proxy CONNECT returned no error")
					}
					if got := classifyLaneFailure(err); got != tt.want {
						t.Errorf("proxy status %d disposition = %v, want %v", tt.status, got, tt.want)
					}
					if text := err.Error(); !strings.Contains(text, fmt.Sprintf("HTTP %d", tt.status)) ||
						strings.Contains(text, "private diagnostic") {
						t.Errorf("proxy error lost its status or exposed peer text: %s", text)
					}
				})
			}
		})
	}
}
