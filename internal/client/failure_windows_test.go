package client

import (
	"net"
	"os"
	"testing"

	"golang.org/x/sys/windows"
)

func TestClassifyLaneFailure(t *testing.T) {
	for _, tt := range []struct {
		name  string
		errno windows.Errno
		want  failureDisposition
	}{
		{name: "WinsockAccessDenied", errno: windows.WSAEACCES, want: failureCloseLane},
		{name: "NetworkAccessDenied", errno: windows.ERROR_NETWORK_ACCESS_DENIED, want: failureCloseLane},
		{name: "ConnectionRefused", errno: windows.WSAECONNREFUSED, want: failureRetry},
		{name: "ConnectionReset", errno: windows.WSAECONNRESET, want: failureRetry},
		{name: "ConnectionTimeout", errno: windows.WSAETIMEDOUT, want: failureRetry},
	} {
		t.Run(tt.name, func(t *testing.T) {
			socketError := &net.OpError{Op: "dial", Net: "tcp", Err: os.NewSyscallError("connect", tt.errno)}
			want := tt.want
			for _, tt := range []struct {
				name string
				err  error
			}{
				{name: "Raw", err: tt.errno},
				{name: "Socket", err: socketError},
				{name: "TLSHandshake", err: &handshakeError{carrier: "TLS", cause: socketError}},
				{name: "WebSocketHandshake", err: &handshakeError{carrier: "WebSocket", cause: socketError}},
			} {
				t.Run(tt.name, func(t *testing.T) {
					if disposition := classifyLaneFailure(tt.err); disposition != want {
						t.Fatalf("socket failure disposition = %d, want %d", disposition, want)
					}
					if want == failureCloseLane && laneFailureEndsSession(tt.err, 1) {
						t.Fatal("a permission-denied lane ended a session with another available lane")
					}
					if want == failureCloseLane && !laneFailureEndsSession(tt.err, 0) {
						t.Fatal("the last permission-denied lane did not end the session")
					}
				})
			}
		})
	}
}
