//go:build linux

package carrier

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestConfigureTCP(t *testing.T) {
	t.Run("Options", testConfigureTCPOptions)

	t.Run("Backpressure", testConfigureTCPBackpressure)

	t.Run("Multipath", testConfigureTCPMultipath)

	t.Run("WriteQueueErrors", testConfigureTCPWriteQueueErrors)
}

func testConfigureTCPOptions(t *testing.T) {
	for _, tt := range []struct {
		name     string
		network  string
		address  string
		accepted bool
	}{
		{name: "DialedIPv4", network: "tcp4", address: "127.0.0.1"},
		{name: "AcceptedIPv4BeforeHandshake", network: "tcp4", address: "127.0.0.1", accepted: true},
		{name: "DialedIPv6", network: "tcp6", address: "::1"},
		{name: "AcceptedIPv6BeforeHandshake", network: "tcp6", address: "::1", accepted: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dialed, accepted := newTCPOptionsPair(t, tt.network, tt.address, tt.accepted)
			connection := dialed
			if tt.accepted {
				connection = accepted
				if actual := readTCPSocketOptions(t, connection); actual.lowat != 16*1024 {
					t.Fatal("accepted socket was not configured before the carrier handshake")
				}
			}
			if err := connection.SetNoDelay(false); err != nil {
				t.Fatal(err)
			}
			if err := connection.SetKeepAlive(true); err != nil {
				t.Fatal(err)
			}
			if err := connection.SetWriteBuffer(64 * 1024); err != nil {
				t.Fatal(err)
			}
			setTCPNotSentLowat(t, connection, 64*1024)
			before := readTCPSocketOptions(t, connection)
			if before.lowat != 64*1024 {
				t.Fatal("fixture did not replace the socket's unsent threshold")
			}
			for range 2 {
				if err := ConfigureTCP(connection); err != nil {
					t.Fatal(err)
				}
				actual := readTCPSocketOptions(t, connection)
				if actual.noDelay != 1 || actual.keepAlive != 0 || actual.lowat != 16*1024 {
					t.Fatalf("carrier TCP options = %+v", actual)
				}
				if actual.congestion != before.congestion || actual.sendBuffer != before.sendBuffer {
					t.Fatal("carrier setup changed the selected controller or send-buffer size")
				}
			}
			connection.Close()
			if err := configureTCPWriteQueue(connection); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("configure closed TCP socket = %v, want net.ErrClosed", err)
			}
		})
	}
}

func testConfigureTCPBackpressure(t *testing.T) {
	for _, tt := range []struct {
		name, network, address string
	}{
		{name: "IPv4", network: "tcp4", address: "127.0.0.1"},
		{name: "IPv6", network: "tcp6", address: "::1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			connection, peer := newTCPOptionsPair(t, tt.network, tt.address, false)
			if err := peer.SetReadBuffer(4096); err != nil {
				t.Fatal(err)
			}
			if err := connection.SetWriteBuffer(64 * 1024); err != nil {
				t.Fatal(err)
			}
			if err := ConfigureTCP(connection); err != nil {
				t.Fatal(err)
			}
			payload := make([]byte, 4*1024*1024)
			if err := connection.SetWriteDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			written, err := connection.Write(payload)
			if !errors.Is(err, os.ErrDeadlineExceeded) || written <= 0 || written >= len(payload) {
				t.Fatalf("backpressured Write() = %d, %v", written, err)
			}
			if err := connection.SetWriteDeadline(time.Time{}); err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			finished := make(chan error, 1)
			go func() {
				close(started)
				_, err := connection.Write(payload)
				finished <- err
			}()
			<-started
			select {
			case err := <-finished:
				t.Fatalf("backpressured write returned before Close(): %v", err)
			case <-time.After(10 * time.Millisecond):
			}
			if err := connection.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-finished:
				if !errors.Is(err, net.ErrClosed) {
					t.Fatalf("Write() after Close() = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Close() did not release the backpressured writer")
			}
		})
	}
}

func testConfigureTCPMultipath(t *testing.T) {
	for _, tt := range []struct {
		name, network, address string
		listen, dial, active   bool
	}{
		{name: "NegotiatedIPv4", network: "tcp4", address: "127.0.0.1", listen: true, dial: true, active: true},
		{name: "NegotiatedIPv6", network: "tcp6", address: "::1", listen: true, dial: true, active: true},
		{name: "DialerFallbackIPv4", network: "tcp4", address: "127.0.0.1", dial: true},
		{name: "DialerFallbackIPv6", network: "tcp6", address: "::1", dial: true},
		{name: "ListenerFallbackIPv4", network: "tcp4", address: "127.0.0.1", listen: true},
		{name: "ListenerFallbackIPv6", network: "tcp6", address: "::1", listen: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var config net.ListenConfig
			config.SetMultipathTCP(tt.listen)
			listener, err := config.Listen(t.Context(), tt.network, net.JoinHostPort(tt.address, "0"))
			if err != nil {
				if tt.network == "tcp6" && (errors.Is(err, unix.EAFNOSUPPORT) ||
					errors.Is(err, unix.EPROTONOSUPPORT) || errors.Is(err, unix.EADDRNOTAVAIL)) {
					t.Skipf("IPv6 loopback is unavailable: %v", err)
				}
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			dialer := net.Dialer{Timeout: 5 * time.Second}
			dialer.SetMultipathTCP(tt.dial)
			dialed, err := dialer.DialContext(t.Context(), tt.network, listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { dialed.Close() })
			if err := dialed.(*net.TCPConn).SetNoDelay(true); errors.Is(err, unix.ENOPROTOOPT) || errors.Is(err, unix.EOPNOTSUPP) {
				t.Skip("kernel MPTCP sockets do not support TCP_NODELAY")
			} else if err != nil {
				t.Fatal(err)
			}
			accepted, err := NewTCPOptionsListener(listener).Accept()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { accepted.Close() })
			if actual := readTCPSocketOptions(t, accepted.(*net.TCPConn)); actual.lowatAvailable && actual.lowat != 16*1024 {
				t.Fatal("accepted socket was not configured before the carrier handshake")
			}
			for _, connection := range []*net.TCPConn{dialed.(*net.TCPConn), accepted.(*net.TCPConn)} {
				// A fallback case has one explicitly plain TCP endpoint. Older kernels cannot report fallback reliably.
				if tt.active {
					active, err := connection.MultipathTCP()
					if err != nil {
						t.Fatal(err)
					}
					if !active {
						t.Skip("MPTCP is unavailable")
					}
				}
				before := readTCPSocketOptions(t, connection)
				expectedLowat := before.lowat
				if before.lowatAvailable {
					setTCPNotSentLowat(t, connection, 64*1024)
					before = readTCPSocketOptions(t, connection)
					expectedLowat = 16 * 1024
				}
				if err := ConfigureTCP(connection); err != nil {
					t.Fatal(err)
				}
				after := readTCPSocketOptions(t, connection)
				if after.lowatAvailable != before.lowatAvailable || after.lowat != expectedLowat ||
					after.noDelay != 1 || after.keepAlive != 0 ||
					after.congestion != before.congestion || after.sendBuffer != before.sendBuffer {
					t.Fatalf("configured socket = %+v, before %+v", after, before)
				}
			}
		})
	}
}

func testConfigureTCPWriteQueueErrors(t *testing.T) {
	if runtime.GOARCH != "amd64" && runtime.GOARCH != "arm64" {
		t.Skip("socket-option fault injection requires the amd64 or arm64 syscall ABI")
	}
	for _, tt := range []struct {
		name     string
		errno    unix.Errno
		optional bool
	}{
		{name: "UnsupportedProtocolOption", errno: unix.ENOPROTOOPT, optional: true},
		{name: "UnsupportedOperation", errno: unix.EOPNOTSUPP, optional: true},
		{name: "PermissionDenied", errno: unix.EPERM},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if os.Getenv("WIREHOP_TEST_TCP_OPTION_ERROR") != tt.name {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				command := exec.CommandContext(ctx, os.Args[0],
					"-test.v", "-test.run=^TestConfigureTCP$/^WriteQueueErrors$/^"+tt.name+"$")
				command.Env = append(os.Environ(), "WIREHOP_TEST_TCP_OPTION_ERROR="+tt.name)
				output, err := command.CombinedOutput()
				if err != nil {
					t.Fatalf("socket-option subprocess: %v\n%s", err, output)
				}
				if bytes.Contains(output, []byte("--- SKIP:")) {
					t.Skipf("socket-option subprocess skipped: %s", output)
				}
				if !bytes.Contains(output, []byte("--- PASS: "+t.Name()+" (")) {
					t.Fatalf("socket-option subprocess did not complete %s:\n%s", t.Name(), output)
				}
				t.Logf("socket-option subprocess:\n%s", output)
				return
			}
			connection, _ := newTCPOptionsPair(t, "tcp4", "127.0.0.1", false)
			setTCPNotSentLowat(t, connection, 64*1024)
			before := readTCPSocketOptions(t, connection)
			rejectTCPWriteQueueOption(t, tt.errno)
			err := ConfigureTCP(connection)
			if tt.optional && err != nil || !tt.optional && !errors.Is(err, tt.errno) {
				t.Fatalf("ConfigureTCP() with %v = %v", tt.errno, err)
			}
			after := readTCPSocketOptions(t, connection)
			if after.lowat != before.lowat || after.noDelay != 1 || after.keepAlive != 0 ||
				after.congestion != before.congestion || after.sendBuffer != before.sendBuffer {
				t.Fatalf("rejected socket option changed unrelated state: after %+v, before %+v", after, before)
			}
		})
	}
}

func rejectTCPWriteQueueOption(t *testing.T, errno unix.Errno) {
	t.Helper()
	// Confine the filter to this subprocess. The locked thread is discarded when its goroutine exits.
	runtime.LockOSThread()
	// Match the syscall number, protocol level, and option in seccomp_data's 64-bit argument layout.
	filter := []unix.SockFilter{
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 0},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.SYS_SETSOCKOPT, Jf: 5},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 24},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.IPPROTO_TCP, Jf: 3},
		{Code: unix.BPF_LD | unix.BPF_W | unix.BPF_ABS, K: 32},
		{Code: unix.BPF_JMP | unix.BPF_JEQ | unix.BPF_K, K: unix.TCP_NOTSENT_LOWAT, Jf: 1},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ERRNO | uint32(errno)},
		{Code: unix.BPF_RET | unix.BPF_K, K: unix.SECCOMP_RET_ALLOW},
	}
	program := unix.SockFprog{Len: uint16(len(filter)), Filter: &filter[0]}
	if err := unix.Prctl(unix.PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	_, _, filterError := unix.Syscall6(unix.SYS_PRCTL, unix.PR_SET_SECCOMP, unix.SECCOMP_MODE_FILTER,
		uintptr(unsafe.Pointer(&program)), 0, 0, 0)
	runtime.KeepAlive(filter)
	if filterError == unix.EPERM || filterError == unix.EACCES {
		t.Skipf("seccomp filters are unavailable: %v", filterError)
	}
	if filterError != 0 {
		t.Fatal(filterError)
	}
}

type tcpSocketOptions struct {
	lowatAvailable                        bool
	noDelay, keepAlive, lowat, sendBuffer int
	congestion                            string
}

func newTCPOptionsPair(t *testing.T, network, address string, configureAccepted bool) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	var config net.ListenConfig
	config.SetMultipathTCP(false)
	base, err := config.Listen(t.Context(), network, net.JoinHostPort(address, "0"))
	if err != nil {
		if network == "tcp6" && (errors.Is(err, unix.EAFNOSUPPORT) ||
			errors.Is(err, unix.EPROTONOSUPPORT) || errors.Is(err, unix.EADDRNOTAVAIL)) {
			t.Skipf("IPv6 loopback is unavailable: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { base.Close() })
	if err := base.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	listener := base
	if configureAccepted {
		listener = NewTCPOptionsListener(base)
	}
	dialer := net.Dialer{Timeout: 5 * time.Second}
	dialer.SetMultipathTCP(false)
	dialed, err := dialer.DialContext(t.Context(), network, base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dialed.Close() })
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { accepted.Close() })
	return dialed.(*net.TCPConn), accepted.(*net.TCPConn)
}

func setTCPNotSentLowat(t *testing.T, connection *net.TCPConn, value int) {
	t.Helper()
	raw, err := connection.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var operationError error
	err = raw.Control(func(descriptor uintptr) {
		operationError = unix.SetsockoptInt(int(descriptor), unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, value)
	})
	if err != nil || operationError != nil {
		t.Fatalf("set TCP unsent threshold: %v, %v", err, operationError)
	}
}

func readTCPSocketOptions(t *testing.T, connection *net.TCPConn) tcpSocketOptions {
	t.Helper()
	raw, err := connection.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var result tcpSocketOptions
	var operationError error
	err = raw.Control(func(descriptor uintptr) {
		for _, option := range []struct {
			level, name int
			value       *int
		}{
			{unix.IPPROTO_TCP, unix.TCP_NODELAY, &result.noDelay},
			{unix.SOL_SOCKET, unix.SO_KEEPALIVE, &result.keepAlive},
			{unix.IPPROTO_TCP, unix.TCP_NOTSENT_LOWAT, &result.lowat},
			{unix.SOL_SOCKET, unix.SO_SNDBUF, &result.sendBuffer},
		} {
			*option.value, operationError = unix.GetsockoptInt(int(descriptor), option.level, option.name)
			if option.name == unix.TCP_NOTSENT_LOWAT {
				if operationError == unix.ENOPROTOOPT || operationError == unix.EOPNOTSUPP {
					operationError = nil
					continue
				}
				result.lowatAvailable = operationError == nil
			}
			if operationError != nil {
				return
			}
		}
		result.congestion, operationError = unix.GetsockoptString(int(descriptor), unix.IPPROTO_TCP, unix.TCP_CONGESTION)
	})
	if err != nil || operationError != nil {
		t.Fatalf("read TCP socket options: %v, %v", err, operationError)
	}
	return result
}
