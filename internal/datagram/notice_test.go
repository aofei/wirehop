package datagram

import (
	"bytes"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

func TestUDPNoticeReport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var output bytes.Buffer
		notice := udpNotice{logger: slog.New(slog.NewTextHandler(&output, nil))}
		err := &net.OpError{Op: "write", Net: "udp", Addr: &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 51820},
			Err: fmt.Errorf("private diagnostic: %w", syscall.EMSGSIZE)}
		notice.report("write target", err)
		var workers sync.WaitGroup
		for range 100 {
			workers.Go(func() { notice.report("write target", err) })
		}
		workers.Wait()
		if strings.Count(output.String(), "level=WARN") != 1 {
			t.Fatalf("burst warnings were not bounded: %s", &output)
		}
		time.Sleep(time.Minute)
		notice.report("write target", err)
		logged := output.String()
		if strings.Count(logged, "level=WARN") != 2 || !strings.Contains(logged, "suppressed=100") ||
			!strings.Contains(logged, "check WireGuard MTU") {
			t.Fatalf("missing rate-limited MTU diagnostic: %s", logged)
		}
		if strings.Contains(logged, "192.0.2.1") || strings.Contains(logged, "private diagnostic") {
			t.Fatalf("socket warning exposed private context: %s", logged)
		}
	})
}
