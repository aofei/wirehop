package command

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/carrier"
	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/lanespec"
	"github.com/aofei/wirehop/internal/laneurl"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/policy"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/server"
	"github.com/aofei/wirehop/internal/socketopts"
	targetpkg "github.com/aofei/wirehop/internal/target"
	"github.com/aofei/wirehop/internal/wsheader"
)

var errTestWrite = errors.New("test write failure")

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) {
	return 0, errTestWrite
}

type channelWriter struct {
	values chan string
}

type tlsConnectionObservation struct {
	connection net.Conn
	resumed    bool
}

type observingTLSListener struct {
	net.Listener
	config       *tls.Config
	observations chan tlsConnectionObservation
}

func (l *observingTLSListener) Accept() (net.Conn, error) {
	connection, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	secure := tls.Server(connection, l.config)
	if err := secure.Handshake(); err != nil {
		connection.Close()
		return nil, err
	}
	l.observations <- tlsConnectionObservation{
		connection: secure,
		resumed:    secure.ConnectionState().DidResume,
	}
	return secure, nil
}

func (w channelWriter) Write(value []byte) (int, error) {
	w.values <- string(bytes.TrimSpace(value))
	return len(value), nil
}

func TestRunUsage(t *testing.T) {
	for _, test := range []struct {
		name  string
		args  []string
		token string
	}{
		{name: "MissingSubcommand"},
		{name: "UnknownSubcommand", args: []string{"unknown"}},
		{name: "ClientMissingToken", args: []string{
			"client", "--listen", "127.0.0.1:51820", "--target", "127.0.0.1:51820",
			"--lane", "tls://example.com:443",
		}},
		{name: "ClientInsecureDenied", token: "token", args: []string{
			"client", "--listen", "127.0.0.1:51820", "--target", "127.0.0.1:51820",
			"--lane", "tcp://example.com:443",
		}},
		{name: "ClientMulticastListen", token: "token", args: []string{
			"client", "--listen", "224.0.0.1:51820", "--target", "127.0.0.1:51820",
			"--lane", "tls://example.com:443",
		}},
		{name: "ClientUnusedTLSServerName", token: "token", args: []string{
			"client", "--listen", "127.0.0.1:51820", "--target", "127.0.0.1:51820",
			"--lane", "tcp://example.com:443", "--allow-insecure", "--tls-server-name", "example.com",
		}},
		{name: "ServerInsecureDenied", token: "token", args: []string{
			"server", "--listen", "ws://:8080/_wirehop", "--allow-target", "127.0.0.1:51820",
		}},
		{name: "ServerTLSFilesRequired", token: "token", args: []string{
			"server", "--listen", "wss://:8443/_wirehop", "--allow-target", "127.0.0.1:51820",
		}},
		{name: "ServerUnusedTLSFiles", token: "token", args: []string{
			"server", "--listen", "tcp://:8080", "--allow-insecure", "--allow-target", "127.0.0.1:51820",
			"--tls-cert", "server.crt", "--tls-key", "server.key",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			getenv := func(string) string { return test.token }
			if err := Run(context.Background(), test.args, getenv, &output, &output); !errors.Is(err, ErrUsage) {
				t.Fatalf("Run() error = %v, want %v", err, ErrUsage)
			}
		})
	}
}

func TestExecuteUsageError(t *testing.T) {
	for _, test := range []struct {
		name       string
		args       []string
		token      string
		wantStderr string
	}{
		{
			name: "MissingSubcommand",
			wantStderr: "wirehop: expected client, server, forward, or version subcommand\n" +
				"Try 'wirehop --help' for more information.\n",
		},
		{
			name: "UnknownFlag",
			args: []string{"client", "--unknown"},
			wantStderr: "wirehop: unknown option \"--unknown\"\n" +
				"Try 'wirehop client --help' for more information.\n",
		},
		{
			name: "MissingFlagValue",
			args: []string{"client", "--listen"},
			wantStderr: "wirehop: option \"--listen\" requires a value\n" +
				"Try 'wirehop client --help' for more information.\n",
		},
		{
			name: "InvalidFlagValue",
			args: []string{"client", "--fwmark", "invalid"},
			wantStderr: "wirehop: invalid value \"invalid\" for --fwmark: parse error\n" +
				"Try 'wirehop client --help' for more information.\n",
		},
		{
			name: "InvalidReservedValue",
			args: []string{"client", "--reserved", "invalid"},
			wantStderr: "wirehop: invalid value \"invalid\" for --reserved: expected canonical Base64 encoding of exactly three bytes\n" +
				"Try 'wirehop client --help' for more information.\n",
		},
		{
			name: "ZeroReservedValue",
			args: []string{"client", "--reserved", "AAAA"},
			wantStderr: "wirehop: invalid value \"AAAA\" for --reserved: value must not be all zero\n" +
				"Try 'wirehop client --help' for more information.\n",
		},
		{
			name: "InvalidBooleanFlagValue",
			args: []string{"client", "--allow-insecure=invalid"},
			wantStderr: "wirehop: invalid boolean value \"invalid\" for --allow-insecure: parse error\n" +
				"Try 'wirehop client --help' for more information.\n",
		},
		{
			name: "MissingRequiredOptions",
			args: []string{"client"},
			wantStderr: "wirehop: required options are missing: --listen, --target, --lane\n" +
				"Try 'wirehop client --help' for more information.\n",
		},
		{
			name: "ServerMissingRequiredOptions",
			args: []string{"server"},
			wantStderr: "wirehop: required options are missing: --listen, --allow-target\n" +
				"Try 'wirehop server --help' for more information.\n",
		},
		{
			name: "UnexpectedArgument",
			args: []string{"server", "help"},
			wantStderr: "wirehop: unexpected argument \"help\"\n" +
				"Try 'wirehop server --help' for more information.\n",
		},
		{
			name: "ForwardInvalidReservedValue",
			args: []string{"forward", "--reserved", "invalid"},
			wantStderr: "wirehop: invalid value \"invalid\" for --reserved: expected canonical Base64 encoding of exactly three bytes\n" +
				"Try 'wirehop forward --help' for more information.\n",
		},
		{
			name: "ForwardMissingRequiredOptions",
			args: []string{"forward"},
			wantStderr: "wirehop: required options are missing: --listen, --target\n" +
				"Try 'wirehop forward --help' for more information.\n",
		},
		{
			name: "ForwardFirewallMarkOverflow",
			args: []string{
				"forward", "--listen", "127.0.0.1:51820", "--target", "127.0.0.1:51820",
				"--fwmark", "4294967296",
			},
			wantStderr: "wirehop: --fwmark exceeds 32 bits\n" +
				"Try 'wirehop forward --help' for more information.\n",
		},
		{
			name: "VersionUnexpectedArgument",
			args: []string{"version", "extra"},
			wantStderr: "wirehop: unexpected argument \"extra\"\n" +
				"Try 'wirehop version --help' for more information.\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := Execute(context.Background(), test.args, func(string) string { return test.token },
				&stdout, &stderr)
			if code != 2 || stdout.Len() != 0 || stderr.String() != test.wantStderr {
				t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
			}
			if bytes.Contains(stderr.Bytes(), []byte("Usage:")) {
				t.Fatalf("Execute() printed full usage: %q", stderr.String())
			}
		})
	}
}

func TestExecuteRuntimeError(t *testing.T) {
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{"--help"}, func(string) string { return "" },
		errorWriter{}, &stderr)
	if code != 1 || stderr.String() != "wirehop: test write failure\n" {
		t.Fatalf("Execute() = %d, stderr %q", code, stderr.String())
	}
}

func TestExecuteNilStandardError(t *testing.T) {
	var stdout bytes.Buffer
	if code := Execute(context.Background(), nil, func(string) string { return "" }, &stdout, nil); code != 2 {
		t.Fatalf("Execute() = %d, want 2", code)
	}
}

func TestExecuteDoesNotExposeInvalidToken(t *testing.T) {
	const token = "private token"
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"client", "--listen", "127.0.0.1:51820", "--target", "127.0.0.1:51820",
		"--lane", "tls://example.com:443",
	}, func(string) string { return token }, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "WIREHOP_TOKEN is invalid") ||
		strings.Contains(stderr.String(), token) {
		t.Fatalf("Execute() = %d, stderr %q", code, stderr.String())
	}
}

func TestRunValidatesArgumentsBeforeToken(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "ClientLane",
			args: []string{
				"client", "--listen", "127.0.0.1:51820", "--target", "127.0.0.1:51820",
				"--lane", "tls://example.com",
			},
			want: "--lane 1: tls URLs require an explicit port",
		},
		{
			name: "ServerTarget",
			args: []string{
				"server", "--listen", "wss://:443/_wirehop", "--allow-target", "0.0.0.0:51820",
				"--tls-cert", "server.crt", "--tls-key", "server.key",
			},
			want: `invalid allowed target "0.0.0.0:51820": invalid UDP target: unspecified addresses are not allowed`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := Run(context.Background(), test.args, func(string) string { return "" }, &output, &output)
			if !errors.Is(err, ErrUsage) || err.Error() != test.want {
				t.Fatalf("Run() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestInvalidLocalListenReason(t *testing.T) {
	for _, test := range []struct {
		address string
		want    string
	}{
		{address: "127.0.0.1:0"},
		{address: "[fe80::1%en0]:51820"},
		{address: "[::ffff:127.0.0.1]:51820", want: "IPv4-mapped IPv6 addresses are not allowed"},
		{address: "[ff02::1]:51820", want: "multicast addresses are not allowed"},
	} {
		address := netip.MustParseAddrPort(test.address)
		if got := invalidLocalListenReason(address); got != test.want {
			t.Errorf("invalidLocalListenReason(%s) = %q, want %q", address, got, test.want)
		}
	}
}

func TestRunRejectsExcessLanes(t *testing.T) {
	args := []string{
		"client", "--listen", "127.0.0.1:51820", "--target", "127.0.0.1:51820",
	}
	for range defaultMaxLanesPerSession + 1 {
		args = append(args, "--lane", "tls://example.com:443")
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), args, func(string) string { return "test-token" }, &stdout, &stderr)
	want := "wirehop: at most 16 --lane options are allowed\n" +
		"Try 'wirehop client --help' for more information.\n"
	if code != 2 || stdout.Len() != 0 || stderr.String() != want {
		t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestRunForwardRejectsUnsupportedFirewallMark(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("Linux supports socket firewall marks")
	}
	var output bytes.Buffer
	err := Run(context.Background(), []string{
		"forward", "--listen", "127.0.0.1:51820", "--target", "127.0.0.1:51820", "--fwmark", "1",
	}, func(string) string { return "" }, &output, &output)
	if !errors.Is(err, ErrUsage) || !errors.Is(err, socketopts.ErrUnsupportedMark) ||
		err.Error() != "--fwmark is supported only on Linux" {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestRunRejectsRepeatedSingleValueOptions(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		want    string
		command string
	}{
		{name: "ClientListen", command: "client", args: []string{
			"client", "--listen", "127.0.0.1:1", "--listen", "127.0.0.1:2",
		}, want: "--listen may be specified only once"},
		{name: "ClientTarget", command: "client", args: []string{
			"client", "--target=127.0.0.1:1", "--target=127.0.0.1:2",
		}, want: "--target may be specified only once"},
		{name: "ClientReserved", command: "client", args: []string{
			"client", "--reserved", "AQID", "--reserved", "BAUG",
		}, want: "--reserved may be specified only once"},
		{name: "ClientTLSServerName", command: "client", args: []string{
			"client", "--tls-server-name", "one.example", "--tls-server-name", "two.example",
		}, want: "--tls-server-name may be specified only once"},
		{name: "ClientFirewallMark", command: "client", args: []string{
			"client", "--fwmark", "1", "--fwmark", "2",
		}, want: "--fwmark may be specified only once"},
		{name: "ClientAllowInsecure", command: "client", args: []string{
			"client", "--allow-insecure", "--allow-insecure",
		}, want: "--allow-insecure may be specified only once"},
		{name: "ServerTLSCertificate", command: "server", args: []string{
			"server", "--tls-cert", "one.pem", "--tls-cert", "two.pem",
		}, want: "--tls-cert may be specified only once"},
		{name: "ServerTLSKey", command: "server", args: []string{
			"server", "--tls-key=one.pem", "--tls-key=two.pem",
		}, want: "--tls-key may be specified only once"},
		{name: "ServerAllowInsecure", command: "server", args: []string{
			"server", "--allow-insecure", "--allow-insecure",
		}, want: "--allow-insecure may be specified only once"},
		{name: "ForwardListen", command: "forward", args: []string{
			"forward", "--listen", "127.0.0.1:1", "--listen", "127.0.0.1:2",
		}, want: "--listen may be specified only once"},
		{name: "ForwardTarget", command: "forward", args: []string{
			"forward", "--target=127.0.0.1:1", "--target=127.0.0.1:2",
		}, want: "--target may be specified only once"},
		{name: "ForwardReserved", command: "forward", args: []string{
			"forward", "--reserved", "AQID", "--reserved", "BAUG",
		}, want: "--reserved may be specified only once"},
		{name: "ForwardFirewallMark", command: "forward", args: []string{
			"forward", "--fwmark", "1", "--fwmark", "2",
		}, want: "--fwmark may be specified only once"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := Run(context.Background(), test.args, func(string) string { return "test-token" }, &output, &output)
			usage, ok := errors.AsType[*usageError](err)
			if !ok || usage.command != test.command || err.Error() != test.want {
				t.Fatalf("Run() error = %v, want %q for %s", err, test.want, test.command)
			}
		})
	}
}

func TestRunRejectsConflictingServerListeners(t *testing.T) {
	var output bytes.Buffer
	err := Run(context.Background(), []string{
		"server", "--listen", "tcp://127.0.0.1:8443", "--listen", "ws://127.0.0.1:8443/_wirehop",
		"--allow-target", "127.0.0.1:51820", "--allow-insecure",
	}, func(string) string { return "test-token" }, &output, &output)
	want := "--listen 2 conflicts with --listen 1: both bind 127.0.0.1:8443"
	if !errors.Is(err, ErrUsage) || err.Error() != want {
		t.Fatalf("Run() error = %v, want %q", err, want)
	}
}

func TestRunLaneDeclarationErrorContext(t *testing.T) {
	for _, test := range []struct {
		name      string
		args      []string
		want      string
		wantCause error
	}{
		{
			name: "ClientLane",
			args: []string{
				"client", "--listen", "127.0.0.1:0", "--target", "127.0.0.1:51820",
				"--lane", "wss://example.com", "--lane", "tcp://example.com",
			},
			want:      "--lane 2: tcp URLs require an explicit port",
			wantCause: lanespec.ErrInvalid,
		},
		{
			name: "ClientResolve",
			args: []string{
				"client", "--listen", "127.0.0.1:0", "--target", "127.0.0.1:51820",
				"--lane", "wss://example.com", "--lane",
				"url=wss://example.com/_wirehop,resolve=other.example",
			},
			want:      "--lane 2: resolve must be an unbracketed IP address without a port",
			wantCause: lanespec.ErrInvalid,
		},
		{
			name: "ServerListener",
			args: []string{
				"server", "--listen", "wss://:443/_wirehop", "--listen", "tcp://localhost",
				"--allow-target", "127.0.0.1:51820",
			},
			want:      "--listen 2: tcp URLs require an explicit port",
			wantCause: laneurl.ErrInvalid,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			err := Run(context.Background(), test.args, func(string) string { return "test-token" }, &output, &output)
			if !errors.Is(err, ErrUsage) || !errors.Is(err, test.wantCause) || err.Error() != test.want {
				t.Fatalf("Run() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRepeatFlagPreservesDuplicates(t *testing.T) {
	var values repeatFlag
	if err := values.Set("wss://example.com:443/_wirehop"); err != nil {
		t.Fatal(err)
	}
	if err := values.Set("wss://example.com:443/_wirehop"); err != nil {
		t.Fatal(err)
	}
	if len(values) != 2 || values[0] != values[1] {
		t.Fatalf("repeat values = %v", values)
	}
}

func TestClientTLSConfigEnablesSessionResumption(t *testing.T) {
	if config := newClientTLSConfig(""); config.ServerName != "" || config.ClientSessionCache == nil {
		t.Fatalf("proxy-capable client TLS config = %+v", config)
	}

	temporary := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	certificate := temporary.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(temporary.Certificate())
	temporary.Close()

	for _, version := range []uint16{tls.VersionTLS12, tls.VersionTLS13} {
		t.Run(tls.VersionName(version), func(t *testing.T) {
			config := newClientTLSConfig("example.com")
			if config.ServerName != "example.com" || config.ClientSessionCache == nil {
				t.Fatalf("secure client TLS config = %+v", config)
			}
			config.RootCAs = roots
			config.MinVersion = version
			config.MaxVersion = version
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			serverConfig := &tls.Config{
				Certificates: []tls.Certificate{certificate}, MinVersion: version, MaxVersion: version,
			}
			serverResults := make(chan error, 2)
			go func() {
				for range 2 {
					connection, err := listener.Accept()
					if err == nil {
						secure := tls.Server(connection, serverConfig)
						err = secure.Handshake()
						if err == nil {
							_, err = secure.Write([]byte{1})
						}
						secure.Close()
					}
					serverResults <- err
				}
			}()

			for attempt := range 2 {
				connection, err := tls.Dial("tcp", listener.Addr().String(), config.Clone())
				if err != nil {
					t.Fatal(err)
				}
				var marker [1]byte
				if _, err := io.ReadFull(connection, marker[:]); err != nil {
					t.Fatal(err)
				}
				resumed := connection.ConnectionState().DidResume
				if err := connection.Close(); err != nil {
					t.Fatal(err)
				}
				if resumed != (attempt == 1) {
					t.Fatalf("TLS attempt %d resumed = %t", attempt+1, resumed)
				}
				if err := <-serverResults; err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestClientTLSLaneReconnectResumesSession(t *testing.T) {
	temporary := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := temporary.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(temporary.Certificate())
	temporary.Close()
	target, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	targetAddress := target.LocalAddr().(*net.UDPAddr).AddrPort()
	instance := newCommandTestServerForTarget(t, targetAddress, 10*time.Second)
	base, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observations := make(chan tlsConnectionObservation, 2)
	listener := &observingTLSListener{
		Listener: carrier.NewTCPOptionsListener(base),
		config: &tls.Config{
			Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12, MaxVersion: tls.VersionTLS12,
		},
		observations: observations,
	}
	serverContext, stopServer := context.WithCancel(context.Background())
	serverResult := make(chan error, 1)
	go func() { serverResult <- instance.Serve(serverContext, listener) }()
	defer func() {
		stopServer()
		if err := <-serverResult; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	}()
	_, port, err := net.SplitHostPort(base.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	spec, err := lanespec.Parse("url=tls://example.com:" + port + ",resolve=127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	clientTLS := newClientTLSConfig("example.com")
	clientTLS.RootCAs = roots
	clientTLS.MinVersion = tls.VersionTLS12
	clientTLS.MaxVersion = tls.VersionTLS12
	relayClient, err := client.Start(context.Background(), client.Config{
		Lanes: []lanespec.Spec{spec}, Listen: netip.MustParseAddrPort("127.0.0.1:0"),
		Target: commandTestTarget(t, targetAddress),
		Token:  []byte("test-token"), TLSConfig: clientTLS, HandshakeTimeout: time.Second,
		StartupTimeout: 3 * time.Second, MaxLanes: 1,
		IngressLimits:       packetqueue.Limits{Packets: 4, Bytes: 8192},
		LaneLimits:          packetqueue.Limits{Packets: 4, Bytes: 8192},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relayClient.Close()
	var first tlsConnectionObservation
	select {
	case first = <-observations:
	case <-time.After(3 * time.Second):
		t.Fatal("initial TLS lane did not connect")
	}
	if first.resumed {
		t.Fatal("initial TLS lane unexpectedly resumed a session")
	}
	waitForCommandLanes(t, instance, 1)
	if err := first.connection.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case second := <-observations:
		if !second.resumed {
			t.Fatal("reconnected TLS lane did not resume its session")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TLS lane did not reconnect")
	}
}

func TestSubcommandHelp(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{name: "ClientFlag", args: []string{"client", "--help"}, want: clientUsage},
		{name: "ClientShortFlag", args: []string{"client", "-h"}, want: clientUsage},
		{name: "ClientTopic", args: []string{"help", "client"}, want: clientUsage},
		{name: "ServerFlag", args: []string{"server", "--help"}, want: serverUsage},
		{name: "ServerShortFlag", args: []string{"server", "-h"}, want: serverUsage},
		{name: "ServerTopic", args: []string{"help", "server"}, want: serverUsage},
		{name: "ForwardFlag", args: []string{"forward", "--help"}, want: forwardUsage},
		{name: "ForwardShortFlag", args: []string{"forward", "-h"}, want: forwardUsage},
		{name: "ForwardTopic", args: []string{"help", "forward"}, want: forwardUsage},
		{name: "VersionFlag", args: []string{"version", "--help"}, want: versionUsage},
		{name: "VersionShortFlag", args: []string{"version", "-h"}, want: versionUsage},
		{name: "VersionTopic", args: []string{"help", "version"}, want: versionUsage},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			code := Execute(context.Background(), test.args, func(string) string { return "" }, &stdout, &stderr)
			if code != 0 || stdout.String() != test.want || stderr.Len() != 0 {
				t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
			}
		})
	}
}

func TestVersion(t *testing.T) {
	for _, args := range [][]string{{"version"}, {"--version"}} {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := Execute(context.Background(), args, func(string) string { return "" }, &stdout, &stderr)
		if code != 0 || !strings.HasPrefix(stdout.String(), "wirehop ") || !strings.HasSuffix(stdout.String(), "\n") ||
			stderr.Len() != 0 {
			t.Fatalf("Execute(%v) = %d, stdout %q, stderr %q", args, code, stdout.String(), stderr.String())
		}
	}
}

func TestVersionOverride(t *testing.T) {
	original := versionOverride
	versionOverride = "v1.2.3"
	t.Cleanup(func() { versionOverride = original })
	var output bytes.Buffer
	if err := writeVersion(&output); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "wirehop v1.2.3\n" {
		t.Fatalf("writeVersion() = %q", got)
	}
}

func TestRootHelp(t *testing.T) {
	for _, argument := range []string{"help", "-h", "--help"} {
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := Execute(context.Background(), []string{argument}, func(string) string { return "" }, &stdout, &stderr)
		if code != 0 || stdout.String() != rootUsage || stderr.Len() != 0 {
			t.Fatalf("Execute(%s) = %d, stdout %q, stderr %q",
				argument, code, stdout.String(), stderr.String())
		}
	}
}

func TestExactPathHandler(t *testing.T) {
	handler := exactPathHandler("/_wirehop", http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	}))
	for _, test := range []struct {
		path string
		want int
	}{
		{path: "/_wirehop", want: http.StatusNoContent},
		{path: "/_wirehop?", want: http.StatusNotFound},
		{path: "/_wirehop?unexpected=1", want: http.StatusNotFound},
		{path: "/", want: http.StatusNotFound},
		{path: "/_wirehop/", want: http.StatusNotFound},
	} {
		request := httptest.NewRequest(http.MethodGet, "http://example.com"+test.path, nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.want || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("path %q response = %d, %q, want %d, no-store", test.path, response.Code,
				response.Header().Get("Cache-Control"), test.want)
		}
	}
}

func TestClientListenAddressWriteFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	err = Run(context.Background(), []string{
		"client", "--listen", "127.0.0.1:0", "--target", "127.0.0.1:51820",
		"--lane", "tcp://" + address, "--allow-insecure",
	}, func(string) string { return "test-token" }, errorWriter{}, &stderr)
	if !errors.Is(err, errTestWrite) {
		t.Fatalf("Run() error = %v, want %v", err, errTestWrite)
	}
}

func TestClientFixedListenAddressIsSilent(t *testing.T) {
	listen := reserveUDPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stderr bytes.Buffer
	result := make(chan int, 1)
	go func() {
		result <- Execute(ctx, []string{
			"client", "--listen", listen, "--target", "127.0.0.1:51820",
			"--lane", "tcp://127.0.0.1:1", "--allow-insecure",
		}, func(string) string { return "test-token" }, errorWriter{}, &stderr)
	}()
	waitForUDPBind(t, listen)
	cancel()
	code := waitForCommandCode(t, result)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("Execute() = %d, stderr %q", code, stderr.String())
	}
}

func TestClientDynamicListenAddressOutput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout := channelWriter{values: make(chan string, 2)}
	var stderr bytes.Buffer
	result := make(chan int, 1)
	go func() {
		result <- Execute(ctx, []string{
			"client", "--listen", "127.0.0.1:0", "--target", "127.0.0.1:51820",
			"--lane", "tcp://127.0.0.1:1", "--allow-insecure",
		}, func(string) string { return "test-token" }, stdout, &stderr)
	}()
	var output string
	select {
	case output = <-stdout.values:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the dynamic listen address")
	}
	address, err := netip.ParseAddrPort(output)
	if err != nil || address.Port() == 0 {
		t.Fatalf("client listen address = %q", output)
	}
	cancel()
	code := waitForCommandCode(t, result)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("Execute() = %d, stderr %q", code, stderr.String())
	}
	select {
	case extra := <-stdout.values:
		t.Fatalf("Execute() printed extra output %q", extra)
	default:
	}
}

func TestServerStartupAndCancellationAreSilent(t *testing.T) {
	address := reserveTCPAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	result := make(chan int, 1)
	go func() {
		result <- Execute(ctx, []string{
			"server", "--listen", "tcp://" + address, "--allow-insecure",
			"--allow-target", "127.0.0.1:51820",
		}, func(string) string { return "test-token" }, &stdout, &stderr)
	}()
	connection := dialTCPEventually(t, address)
	if err := connection.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	code := waitForCommandCode(t, result)
	if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestForwardListenAddressWriteFailure(t *testing.T) {
	targetConnection, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.MustParseAddrPort("127.0.0.1:0"),
	))
	if err != nil {
		t.Fatal(err)
	}
	defer targetConnection.Close()
	var stderr bytes.Buffer
	err = Run(context.Background(), []string{
		"forward", "--listen", "127.0.0.1:0", "--target", targetConnection.LocalAddr().String(),
	}, func(string) string { return "" }, errorWriter{}, &stderr)
	if !errors.Is(err, errTestWrite) {
		t.Fatalf("Run() error = %v, want %v", err, errTestWrite)
	}
}

func TestForwardOutput(t *testing.T) {
	targetConnection, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.MustParseAddrPort("127.0.0.1:0"),
	))
	if err != nil {
		t.Fatal(err)
	}
	defer targetConnection.Close()
	targetAddress := targetConnection.LocalAddr().String()

	t.Run("FixedListenAddressIsSilent", func(t *testing.T) {
		listen := reserveUDPAddress(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		result := make(chan int, 1)
		go func() {
			result <- Execute(ctx, []string{
				"forward", "--listen", listen, "--target", targetAddress,
			}, func(string) string { return "" }, &stdout, &stderr)
		}()
		waitForUDPBind(t, listen)
		cancel()
		code := waitForCommandCode(t, result)
		if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
			t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
		}
	})

	t.Run("DynamicListenAddress", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stdout := channelWriter{values: make(chan string, 2)}
		var stderr bytes.Buffer
		result := make(chan int, 1)
		go func() {
			result <- Execute(ctx, []string{
				"forward", "--listen", "127.0.0.1:0", "--target", targetAddress,
			}, func(string) string { return "" }, stdout, &stderr)
		}()
		var output string
		select {
		case output = <-stdout.values:
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for the dynamic listen address")
		}
		address, err := netip.ParseAddrPort(output)
		if err != nil || address.Port() == 0 {
			t.Fatalf("forward listen address = %q", output)
		}
		cancel()
		code := waitForCommandCode(t, result)
		if code != 0 || stderr.Len() != 0 {
			t.Fatalf("Execute() = %d, stderr %q", code, stderr.String())
		}
		select {
		case extra := <-stdout.values:
			t.Fatalf("Execute() printed extra output %q", extra)
		default:
		}
	})
}

func TestRunForwardDoesNotReadEnvironment(t *testing.T) {
	targetConnection, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(
		netip.MustParseAddrPort("127.0.0.1:0"),
	))
	if err != nil {
		t.Fatal(err)
	}
	defer targetConnection.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout := channelWriter{values: make(chan string, 1)}
	var stderr bytes.Buffer
	environmentReads := 0
	result := make(chan error, 1)
	go func() {
		result <- Run(ctx, []string{
			"forward", "--listen", "127.0.0.1:0", "--target", targetConnection.LocalAddr().String(),
		}, func(string) string {
			environmentReads++
			return ""
		}, stdout, &stderr)
	}()
	select {
	case <-stdout.values:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for forward startup")
	}
	cancel()
	var runErr error
	select {
	case runErr = <-result:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for forward shutdown")
	}
	if runErr != nil || environmentReads != 0 || stderr.Len() != 0 {
		t.Fatalf("Run() error = %v, environment reads %d, stderr %q", runErr, environmentReads, stderr.String())
	}
}

func TestCanceledCommandsSkipRuntimeStartup(t *testing.T) {
	t.Run("Client", func(t *testing.T) {
		listener, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := Execute(ctx, []string{
			"client", "--listen", listener.LocalAddr().String(), "--target", "wg.example.com:51820",
			"--lane", "tcp://127.0.0.1:1", "--allow-insecure",
		}, func(string) string { return "test-token" }, &stdout, &stderr)
		if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
			t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
		}
	})
	t.Run("Server", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := Execute(ctx, []string{
			"server", "--listen", "wss://127.0.0.1:8443/_wirehop",
			"--allow-target", "wg.example.com:51820",
			"--tls-cert", "/definitely/missing/server.crt", "--tls-key", "/definitely/missing/server.key",
		}, func(string) string { return "test-token" }, &stdout, &stderr)
		if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
			t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
		}
	})
	t.Run("Forward", func(t *testing.T) {
		listener, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var stdout bytes.Buffer
		var stderr bytes.Buffer
		code := Execute(ctx, []string{
			"forward", "--listen", listener.LocalAddr().String(), "--target", "wg.example.com:51820",
		}, func(string) string { return "" }, &stdout, &stderr)
		if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
			t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
		}
	})
}

func TestServerValidatesMalformedTargetsBeforeLoadingCertificate(t *testing.T) {
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Execute(context.Background(), []string{
		"server", "--listen", "wss://127.0.0.1:8443/_wirehop",
		"--allow-target", "bad_.example:51820",
		"--tls-cert", "/definitely/missing/server.crt", "--tls-key", "/definitely/missing/server.key",
	}, func(string) string { return "test-token" }, &stdout, &stderr)
	want := "wirehop: invalid allowed target \"bad_.example:51820\": invalid UDP target: malformed hostname\n" +
		"Try 'wirehop server --help' for more information.\n"
	if code != 2 || stdout.Len() != 0 || stderr.String() != want {
		t.Fatalf("Execute() = %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
}

func TestHTTPErrorWriter(t *testing.T) {
	var output bytes.Buffer
	writer := httpErrorWriter{logger: slog.New(slog.NewTextHandler(&output, nil))}
	if _, err := writer.Write([]byte("http: TLS handshake error from 192.0.2.1:1234: EOF\n")); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("TLS scan output = %q", output.String())
	}
	if _, err := writer.Write([]byte("http: panic serving 192.0.2.1:1234: test panic\n")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `level=WARN msg="HTTP server error"`) ||
		!strings.Contains(output.String(), `error="panic serving 192.0.2.1:1234: test panic"`) {
		t.Fatalf("HTTP server output = %q", output.String())
	}
}

func TestHTTPHandlerTracker(t *testing.T) {
	tracker := newHTTPHandlerTracker()
	entered := make(chan struct{})
	release := make(chan struct{})
	invocations := make(chan struct{}, 1)
	handler := tracker.handler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		invocations <- struct{}{}
		close(entered)
		<-release
	}))
	request := httptest.NewRequest(http.MethodGet, "http://example.com/", nil)
	result := make(chan struct{})
	go func() {
		handler.ServeHTTP(httptest.NewRecorder(), request)
		close(result)
	}()
	<-entered
	done := tracker.stop()
	select {
	case <-done:
		t.Fatal("handler tracker stopped before its active handler")
	default:
	}
	rejected := httptest.NewRecorder()
	handler.ServeHTTP(rejected, request)
	if rejected.Code != http.StatusServiceUnavailable || rejected.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("stopping handler response = %d, %q", rejected.Code, rejected.Header().Get("Cache-Control"))
	}
	select {
	case <-invocations:
	default:
		t.Fatal("active handler invocation was not observed")
	}
	select {
	case <-invocations:
		t.Fatal("handler tracker admitted a new handler while stopping")
	default:
	}
	close(release)
	<-result
	<-done
}

func TestRunClientCancellationClosesSession(t *testing.T) {
	target, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	targetAddress := target.LocalAddr().(*net.UDPAddr).AddrPort()
	instance := newCommandTestServerForTarget(t, targetAddress, 10*time.Second)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serverContext, stopServer := context.WithCancel(context.Background())
	serverResult := make(chan error, 1)
	go func() { serverResult <- instance.Serve(serverContext, listener) }()
	defer func() {
		stopServer()
		if err := <-serverResult; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commandResult := make(chan error, 1)
	output := channelWriter{values: make(chan string, 1)}
	go func() {
		_, port, err := net.SplitHostPort(listener.Addr().String())
		if err != nil {
			commandResult <- err
			return
		}
		commandResult <- Run(ctx, []string{
			"client", "--listen", "127.0.0.1:0", "--target", targetAddress.String(),
			"--lane", "url=tcp://relay.invalid:" + port + ",resolve=127.0.0.1", "--allow-insecure",
		}, func(string) string { return "test-token" }, output, io.Discard)
	}()
	waitForCommandSessions(t, instance, 1)
	waitForCommandLanes(t, instance, 1)
	localAddress, err := netip.ParseAddrPort(<-output.values)
	if err != nil {
		t.Fatal(err)
	}
	source, err := net.DialUDP("udp", nil, net.UDPAddrFromAddrPort(localAddress))
	if err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, 148)
	binary.LittleEndian.PutUint32(packet, 1)
	if _, err := source.Write(packet); err != nil {
		t.Fatal(err)
	}
	source.Close()
	if err := target.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := target.ReadFromUDP(make([]byte, 148)); err != nil {
		t.Fatalf("read relayed packet: %v", err)
	}
	cancel()
	if err := <-commandResult; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	waitForCommandSessions(t, instance, 0)
}

func TestServeListenersSharesPreHeaderAdmission(t *testing.T) {
	addresses := []string{reserveTCPAddress(t), reserveTCPAddress(t)}
	urls := make([]laneurl.URL, 0, len(addresses))
	for _, address := range addresses {
		url, err := laneurl.ParseListen("ws://" + address + "/_wirehop")
		if err != nil {
			t.Fatal(err)
		}
		urls = append(urls, url)
	}
	instance := newCommandTestServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() { result <- serveListeners(ctx, instance, urls, tls.Certificate{}, logger) }()
	first := dialTCPEventually(t, addresses[0])
	defer first.Close()
	deadline := time.Now().Add(time.Second)
	for instance.Snapshot().PendingAdmissions != 1 {
		if time.Now().After(deadline) {
			t.Fatal("first listener did not reserve admission capacity")
		}
		time.Sleep(time.Millisecond)
	}
	second := dialTCPEventually(t, addresses[1])
	if err := second.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Read(make([]byte, 1)); err == nil {
		t.Fatal("second listener accepted work beyond the shared admission limit")
	} else {
		if networkError, ok := errors.AsType[net.Error](err); ok && networkError.Timeout() {
			t.Fatal("second listener bypassed pre-header admission control")
		}
	}
	second.Close()
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("serveListeners() error = %v", err)
	}
}

func TestServeListenersWaitsForWebSocketHandlers(t *testing.T) {
	target, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	targetAddress := target.LocalAddr().(*net.UDPAddr).AddrPort()
	instance := newCommandTestServerForTarget(t, targetAddress, 10*time.Second)
	address := reserveTCPAddress(t)
	laneURL, err := laneurl.ParseListen("ws://" + address + "/_wirehop")
	if err != nil {
		t.Fatal(err)
	}
	serverContext, stopServer := context.WithCancel(context.Background())
	serverResult := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		serverResult <- serveListeners(
			serverContext, instance, []laneurl.URL{laneURL}, tls.Certificate{}, logger,
		)
	}()

	clientContext, stopClient := context.WithCancel(context.Background())
	clientResult := make(chan error, 1)
	go func() {
		clientResult <- Run(clientContext, []string{
			"client", "--listen", "127.0.0.1:0", "--target", targetAddress.String(),
			"--lane", laneURL.String(), "--allow-insecure",
		}, func(string) string { return "test-token" }, io.Discard, io.Discard)
	}()
	waitForCommandSessions(t, instance, 1)
	waitForCommandLanes(t, instance, 1)

	stopServer()
	if err := <-serverResult; err != nil {
		t.Fatalf("serveListeners() error = %v", err)
	}
	if lanes := instance.Snapshot().AttachedLanes; lanes != 0 {
		t.Fatalf("server attached lanes after serveListeners() = %d, want 0", lanes)
	}
	stopClient()
	if err := <-clientResult; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

func TestServeListenersWSS(t *testing.T) {
	target, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	targetAddress := target.LocalAddr().(*net.UDPAddr).AddrPort()
	instance := newCommandTestServerForTarget(t, targetAddress, time.Second)
	address := reserveTCPAddress(t)
	laneURL, err := laneurl.ParseListen("wss://" + address + "/_wirehop")
	if err != nil {
		t.Fatal(err)
	}
	temporary := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := temporary.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(temporary.Certificate())
	temporary.Close()
	serverContext, stopServer := context.WithCancel(context.Background())
	serverResult := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		serverResult <- serveListeners(
			serverContext, instance, []laneurl.URL{laneURL}, certificate, logger,
		)
	}()
	defer func() {
		stopServer()
		if err := <-serverResult; err != nil {
			t.Errorf("serveListeners() error = %v", err)
		}
	}()
	probe := tls.Client(dialTCPEventually(t, address), &tls.Config{
		RootCAs: roots, ServerName: "127.0.0.1", NextProtos: []string{"http/1.1"},
	})
	if err := probe.Handshake(); err != nil {
		t.Fatal(err)
	}
	if negotiated := probe.ConnectionState().NegotiatedProtocol; negotiated != "http/1.1" {
		t.Fatalf("WSS negotiated protocol = %q, want http/1.1", negotiated)
	}
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	spec, err := lanespec.Parse(laneURL.String())
	if err != nil {
		t.Fatal(err)
	}
	relayClient, err := client.Start(context.Background(), client.Config{
		Lanes: []lanespec.Spec{spec}, Listen: netip.MustParseAddrPort("127.0.0.1:0"),
		Target: commandTestTarget(t, targetAddress),
		Token:  []byte("test-token"), TLSConfig: &tls.Config{RootCAs: roots}, HandshakeTimeout: time.Second,
		StartupTimeout: 3 * time.Second, MaxLanes: 1,
		IngressLimits:       packetqueue.Limits{Packets: 4, Bytes: 8192},
		LaneLimits:          packetqueue.Limits{Packets: 4, Bytes: 8192},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relayClient.Close()
	waitForCommandLanes(t, instance, 1)
	if err := relayClient.Close(); err != nil {
		t.Fatal(err)
	}
	waitForCommandSessions(t, instance, 0)
}

func TestServeListenersWebSocketAdmissionBoundary(t *testing.T) {
	token := strings.Repeat("a", 4096)
	target := netip.MustParseAddrPort("127.0.0.1:51820")
	instance := newCommandTestServerForTokenAndTarget(t, token, target, time.Second)
	address := reserveTCPAddress(t)
	path := "/" + strings.Repeat("a", wsheader.MaxPathSize-1)
	laneURL, err := laneurl.ParseListen("ws://" + address + path)
	if err != nil {
		t.Fatal(err)
	}
	serverContext, stopServer := context.WithCancel(context.Background())
	serverResult := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		serverResult <- serveListeners(
			serverContext, instance, []laneurl.URL{laneURL}, tls.Certificate{}, logger,
		)
	}()
	defer func() {
		stopServer()
		if err := <-serverResult; err != nil {
			t.Errorf("serveListeners() error = %v", err)
		}
	}()
	spec, err := lanespec.Parse(laneURL.String())
	if err != nil {
		t.Fatal(err)
	}
	relayClient, err := client.Start(context.Background(), client.Config{
		Lanes: []lanespec.Spec{spec}, Listen: netip.MustParseAddrPort("127.0.0.1:0"),
		Target: commandTestTarget(t, target),
		Token:  []byte(token), HandshakeTimeout: time.Second, StartupTimeout: 3 * time.Second, MaxLanes: 1,
		IngressLimits:       packetqueue.Limits{Packets: 4, Bytes: 8192},
		LaneLimits:          packetqueue.Limits{Packets: 4, Bytes: 8192},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer relayClient.Close()
	waitForCommandLanes(t, instance, 1)
}

func TestServeListenersClosesPartialPreparation(t *testing.T) {
	firstAddress := reserveTCPAddress(t)
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	values := []string{"tcp://" + firstAddress, "tcp://" + occupied.Addr().String()}
	urls := make([]laneurl.URL, 0, len(values))
	for _, value := range values {
		url, err := laneurl.ParseListen(value)
		if err != nil {
			t.Fatal(err)
		}
		urls = append(urls, url)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := serveListeners(context.Background(), newCommandTestServer(t), urls, tls.Certificate{}, logger); err == nil {
		t.Fatal("serveListeners() succeeded with an occupied address")
	}
	probe, err := net.Listen("tcp", firstAddress)
	if err != nil {
		t.Fatalf("first prepared listener remained open: %v", err)
	}
	probe.Close()
}

func newCommandTestServer(t *testing.T) *server.Server {
	t.Helper()
	return newCommandTestServerForTarget(t, netip.MustParseAddrPort("127.0.0.1:51820"), time.Second)
}

func newCommandTestServerForTarget(t *testing.T, target netip.AddrPort,
	reconnectGrace time.Duration) *server.Server {
	return newCommandTestServerForTokenAndTarget(t, "test-token", target, reconnectGrace)
}

func newCommandTestServerForTokenAndTarget(t *testing.T, token string, target netip.AddrPort,
	reconnectGrace time.Duration) *server.Server {
	t.Helper()
	targets, err := policy.NewTargetSet([]targetpkg.Endpoint{commandTestTarget(t, target)})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := server.New(server.Config{
		Token: []byte(token), Targets: targets, AuthenticationSkew: time.Minute,
		HandshakeTimeout: time.Second, ReplayEntries: 16, JoinNonceEntries: 16, MaxSessions: 4,
		MaxLanesPerSession: 4, MaxPendingAdmissions: 1, ReconnectGrace: reconnectGrace,
		IngressLimits:       packetqueue.Limits{Packets: 4, Bytes: 8192},
		LaneLimits:          packetqueue.Limits{Packets: 4, Bytes: 8192},
		RetentionLimits:     retention.Limits{Packets: 16, Bytes: 32 * 1024},
		Deadlines:           relay.DeadlinePolicy{Control: time.Second, Transport: time.Second},
		DeduplicationWindow: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return instance
}

func commandTestTarget(t *testing.T, address netip.AddrPort) targetpkg.Endpoint {
	t.Helper()
	target, err := targetpkg.FromAddrPort(address)
	if err != nil {
		t.Fatal(err)
	}
	return target
}

func waitForCommandSessions(t *testing.T, instance *server.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for instance.Snapshot().Sessions != want {
		if time.Now().After(deadline) {
			t.Fatalf("server sessions = %d, want %d", instance.Snapshot().Sessions, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCommandLanes(t *testing.T, instance *server.Server, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for instance.Snapshot().AttachedLanes != want {
		if time.Now().After(deadline) {
			t.Fatalf("server attached lanes = %d, want %d", instance.Snapshot().AttachedLanes, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func reserveUDPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(netip.MustParseAddrPort("127.0.0.1:0")))
	if err != nil {
		t.Fatal(err)
	}
	address := listener.LocalAddr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForUDPBind(t *testing.T, address string) {
	t.Helper()
	parsed := netip.MustParseAddrPort(address)
	deadline := time.Now().Add(3 * time.Second)
	for {
		listener, err := net.ListenUDP("udp", net.UDPAddrFromAddrPort(parsed))
		if err != nil {
			return
		}
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("UDP address %s was not bound", address)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForCommandCode(t *testing.T, result <-chan int) int {
	t.Helper()
	select {
	case code := <-result:
		return code
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the command")
		return -1
	}
}

func dialTCPEventually(t *testing.T, address string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		connection, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			return connection
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial %s: %v", address, err)
		}
		time.Sleep(time.Millisecond)
	}
}
