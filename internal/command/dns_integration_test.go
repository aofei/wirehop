package command

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aofei/wirehop/internal/client"
	"github.com/aofei/wirehop/internal/forward"
	"github.com/aofei/wirehop/internal/lanespec"
	"github.com/aofei/wirehop/internal/laneurl"
	"github.com/aofei/wirehop/internal/packetqueue"
	"github.com/aofei/wirehop/internal/policy"
	"github.com/aofei/wirehop/internal/relay"
	"github.com/aofei/wirehop/internal/retention"
	"github.com/aofei/wirehop/internal/server"
	"github.com/aofei/wirehop/internal/target"
	"golang.org/x/net/dns/dnsmessage"
)

func TestDNSFallback(t *testing.T) {
	if os.Getenv("WIREHOP_TEST_DNS_FALLBACK") != "1" {
		t.Skip("requires an isolated container with primary 127.0.0.2, secondary 127.0.0.3, timeout:5, and attempts:1")
	}
	configuration, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{"nameserver 127.0.0.2", "nameserver 127.0.0.3", "timeout:5", "attempts:1"} {
		if !strings.Contains(string(configuration), required) {
			t.Fatalf("missing %q in isolated resolver configuration: %s", required, configuration)
		}
	}
	previousResolver := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true}
	t.Cleanup(func() { net.DefaultResolver = previousResolver })
	var primary, secondary atomic.Int32
	serveTestDNS(t, "127.0.0.2:53", false, &primary)
	serveTestDNS(t, "127.0.0.3:53", true, &secondary)

	t.Run("Listener", func(t *testing.T) {
		started := time.Now()
		config := net.ListenConfig{}
		listener, err := listenWithRetry(t.Context(), "listener.example.com:0", started.Add(listenerStartupTimeout), config.Listen)
		if err != nil {
			t.Fatal(err)
		}
		listener.Close()
		t.Logf("listener prepared after %v", time.Since(started).Round(time.Millisecond))
	})

	for _, scheme := range []string{"tcp", "tls", "ws", "wss", "forward"} {
		t.Run(strings.ToUpper(scheme), func(t *testing.T) {
			first, second := primary.Load(), secondary.Load()
			started := time.Now()
			targetSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			defer targetSocket.Close()
			_, port, err := net.SplitHostPort(targetSocket.LocalAddr().String())
			if err != nil {
				t.Fatal(err)
			}
			endpoint := target.MustParse("target.example.com:" + port)
			ctx, cancel := context.WithTimeout(t.Context(), 25*time.Second)
			defer cancel()
			var local netip.AddrPort
			if scheme == "forward" {
				instance, err := forward.Start(ctx, forward.Config{Listen: netip.MustParseAddrPort("127.0.0.1:0"), Target: endpoint})
				if err != nil {
					t.Fatal(err)
				}
				defer instance.Close()
				if err := instance.WaitReady(ctx); err != nil {
					t.Fatal(err)
				}
				local = instance.LocalAddr()
			} else {
				local = startDNSFallbackRelay(t, ctx, scheme, endpoint)
			}
			if elapsed := time.Since(started); elapsed < 5*time.Second {
				t.Fatalf("preparation took %v, did not exercise primary DNS timeout", elapsed)
			}
			if primary.Load() == first || secondary.Load() == second {
				t.Fatal("preparation did not reach both nameservers")
			}
			peer, err := net.DialUDP("udp4", nil, net.UDPAddrFromAddrPort(local))
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			initiation := make([]byte, 148)
			initiation[0] = 1
			binary.LittleEndian.PutUint32(initiation[4:8], 11)
			if _, err := peer.Write(initiation); err != nil {
				t.Fatal(err)
			}
			targetSocket.SetReadDeadline(time.Now().Add(3 * time.Second))
			var buffer [2048]byte
			n, source, err := targetSocket.ReadFromUDPAddrPort(buffer[:])
			if err != nil || !bytes.Equal(buffer[:n], initiation) {
				t.Fatalf("target read = %d, %v", n, err)
			}
			response := make([]byte, 92)
			response[0] = 2
			binary.LittleEndian.PutUint32(response[4:8], 21)
			binary.LittleEndian.PutUint32(response[8:12], 11)
			if _, err := targetSocket.WriteToUDPAddrPort(response, source); err != nil {
				t.Fatal(err)
			}
			peer.SetReadDeadline(time.Now().Add(3 * time.Second))
			n, err = peer.Read(buffer[:])
			if err != nil || !bytes.Equal(buffer[:n], response) {
				t.Fatalf("local read = %d, %v", n, err)
			}
			t.Logf("bidirectional forwarding after %v, primary queries=%d, secondary queries=%d",
				time.Since(started).Round(time.Millisecond), primary.Load()-first, secondary.Load()-second)
		})
	}
}

func startDNSFallbackRelay(t *testing.T, ctx context.Context, scheme string, endpoint target.Endpoint) netip.AddrPort {
	t.Helper()
	targets, err := policy.NewTargetSet([]target.Endpoint{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := server.New(server.Config{
		Token: []byte("test-token"), Targets: targets, AuthenticationSkew: time.Minute,
		HandshakeTimeout: defaultHandshakeTimeout, ReplayEntries: 16, JoinNonceEntries: 16, MaxSessions: 4,
		MaxLanesPerSession: 4, MaxPendingAdmissions: 4, ReconnectGrace: time.Second,
		IngressLimits: packetqueue.Limits{Packets: 4, Bytes: 8192}, LaneLimits: packetqueue.Limits{Packets: 4, Bytes: 8192},
		RetentionLimits: retention.Limits{Packets: 16, Bytes: 32 * 1024},
		Deadlines:       relay.DeadlinePolicy{Control: time.Second, Transport: time.Second}, DeduplicationWindow: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	address := reserveTCPAddress(t)
	path := ""
	if scheme == "ws" || scheme == "wss" {
		path = "/_wirehop"
	}
	url, err := laneurl.ParseListen(scheme + "://" + address + path)
	if err != nil {
		t.Fatal(err)
	}
	temporary := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := temporary.TLS.Certificates[0]
	roots := x509.NewCertPool()
	roots.AddCert(temporary.Certificate())
	temporary.Close()
	serverContext, stopServer := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		result <- serveListeners(serverContext, instance, []laneurl.URL{url}, certificate, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	t.Cleanup(func() {
		stopServer()
		if err := <-result; err != nil {
			t.Errorf("serveListeners() = %v", err)
		}
	})
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	spec, err := lanespec.Parse(scheme + "://carrier.example.com:" + port + path)
	if err != nil {
		t.Fatal(err)
	}
	relayClient, err := client.Start(ctx, client.Config{
		Lanes: []lanespec.Spec{spec}, Listen: netip.MustParseAddrPort("127.0.0.1:0"), Target: endpoint,
		Token: []byte("test-token"), TLSConfig: &tls.Config{RootCAs: roots, ServerName: "127.0.0.1"},
		HandshakeTimeout: defaultHandshakeTimeout, MaxLanes: 1,
		IngressLimits: packetqueue.Limits{Packets: 4, Bytes: 8192}, LaneLimits: packetqueue.Limits{Packets: 4, Bytes: 8192},
		Deadlines: relay.DeadlinePolicy{Control: time.Second, Transport: time.Second}, DeduplicationWindow: 16,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { relayClient.Close() })
	for relayClient.SessionID().IsZero() || instance.Snapshot().AttachedLanes == 0 {
		select {
		case <-ctx.Done():
			t.Fatalf("relay did not become ready: %v", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	return relayClient.LocalAddr()
}

func serveTestDNS(t *testing.T, address string, respond bool, queries *atomic.Int32) {
	t.Helper()
	connection, err := net.ListenUDP("udp4", net.UDPAddrFromAddrPort(netip.MustParseAddrPort(address)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	t.Cleanup(func() {
		connection.Close()
		<-done
	})
	go func() {
		defer close(done)
		var buffer [4096]byte
		for {
			n, peer, err := connection.ReadFromUDP(buffer[:])
			if err != nil {
				return
			}
			queries.Add(1)
			if !respond {
				continue
			}
			var request dnsmessage.Message
			if err := request.Unpack(buffer[:n]); err != nil {
				t.Error(err)
				return
			}
			response := dnsmessage.Message{
				Header: dnsmessage.Header{ID: request.ID, Response: true, Authoritative: true,
					RecursionDesired: request.RecursionDesired, RecursionAvailable: true},
				Questions: request.Questions,
			}
			for _, question := range request.Questions {
				if question.Type == dnsmessage.TypeA {
					response.Answers = append(response.Answers, dnsmessage.Resource{
						Header: dnsmessage.ResourceHeader{Name: question.Name, Type: question.Type, Class: dnsmessage.ClassINET, TTL: 1},
						Body:   &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}},
					})
				}
			}
			encoded, err := response.Pack()
			if err != nil {
				t.Error(err)
				return
			}
			if _, err := connection.WriteToUDP(encoded, peer); err != nil {
				t.Error(err)
				return
			}
		}
	}()
}
