package client

import (
	"errors"
	"io"
	"net"
	neturl "net/url"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aofei/wirehop/internal/protocol"
	"github.com/aofei/wirehop/internal/relay"
)

func TestCompleteCreation(t *testing.T) {
	for _, tt := range []struct {
		name    string
		receive uint64
		wantErr bool
	}{
		{name: "MaximumUncertainty", receive: 10_000_001},
		{name: "ExcessUncertainty", receive: 10_000_003, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, frame, err := completeCreation(1, tt.receive, creationResult{receiveMicros: 1, sendMicros: 1})
			if tt.wantErr {
				if !errors.Is(err, relay.ErrStaleClockSample) || classifyLaneFailure(err) != failureRetry {
					t.Fatalf("uncertain admission = %v, want recoverable timing failure", err)
				}
			} else if err != nil || frame.Type != protocol.FrameClockSync {
				t.Fatalf("boundary admission = %v, frame %v", err, frame.Type)
			}
		})
	}
}

func TestClientPrepareSOCKSTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		left, right := net.Pipe()
		defer left.Close()
		defer right.Close()
		greeting := make(chan error, 1)
		go func() {
			var buffer [3]byte
			_, err := io.ReadFull(right, buffer[:])
			greeting <- err
		}()
		instance := &Client{config: Config{HandshakeTimeout: 100 * time.Millisecond}}
		proxyURL, err := neturl.Parse("socks5://127.0.0.1:1080")
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		connection, err := instance.prepareSOCKS(t.Context(), left, proxyURL, "relay.example:443")
		if connection != nil || err == nil || time.Since(started) != instance.config.HandshakeTimeout {
			t.Fatalf("stalled SOCKS preparation = %v, %v after %v", connection, err, time.Since(started))
		}
		if err := <-greeting; err != nil {
			t.Fatal(err)
		}
	})
}
